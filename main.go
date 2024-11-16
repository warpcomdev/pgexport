package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"log/slog"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/docker/go-units"
	"github.com/jackc/pgx/v5"
	"github.com/urfave/cli/v2"
	"github.com/warpcomdev/pgexport/scanner"
)

type config struct {
	Address    string        `json:"address"`
	Timeout    time.Duration `json:"timeout"`
	Host       string        `json:"host"`
	Port       int           `json:"port"`
	Username   string        `json:"username"`
	Password   string        `json:"-"`
	InitialDB  string        `json:"initialdb"`
	Exceptions []string      `json:"exceptions"`
	Threshold  int64         `json:"threshold"`
	Interval   time.Duration `json:"interval"`
	Pause      time.Duration `json:"pause"`
	Prefix     string        `json:"prefix"`
}

func defaults() config {
	scanDefaults := scanner.Defaults()
	return config{
		Address:    ":8080",
		Timeout:    5 * time.Second,
		Host:       "localhost",
		Port:       5432,
		Username:   "postgres",
		InitialDB:  scanDefaults.InitialDB,
		Threshold:  max(scanDefaults.Threshold, units.GB),
		Exceptions: []string{},
		Interval:   30 * time.Minute,
	}
}

func (c *config) Flags() []cli.Flag {
	return []cli.Flag{
		&cli.StringFlag{
			Name:        "address",
			Aliases:     []string{"a"},
			Value:       c.Address,
			Destination: &c.Address,
			Usage:       "address to listen on",
			Required:    false,
		},
		&cli.DurationFlag{
			Name:        "timeout",
			Aliases:     []string{"t"},
			Usage:       "HTTP timeout",
			Value:       c.Timeout,
			Destination: &c.Timeout,
			Required:    false,
		},
		&cli.StringFlag{
			Name:        "host",
			Aliases:     []string{"H"},
			Usage:       "database host",
			Value:       c.Host,
			Destination: &c.Host,
			Required:    false,
		},
		&cli.IntFlag{
			Name:        "port",
			Aliases:     []string{"p"},
			Usage:       "database port",
			Value:       c.Port,
			Destination: &c.Port,
			Required:    false,
		},
		&cli.StringFlag{
			Name:        "username",
			Aliases:     []string{"U"},
			Usage:       "database user",
			Value:       c.Username,
			Destination: &c.Username,
			Required:    false,
		},
		&cli.StringFlag{
			Name:        "initialdb",
			Aliases:     []string{"d"},
			Usage:       "initial database",
			Value:       c.InitialDB,
			Destination: &c.InitialDB,
			Required:    false,
		},
		&cli.StringSliceFlag{
			Name:    "exceptions",
			Aliases: []string{"e"},
			Usage:   fmt.Sprintf("databases to omit - besides '%s'", strings.Join(scanner.Defaults().Exceptions, "', '")),
			Value:   cli.NewStringSlice(c.Exceptions...),
			Action: func(_ *cli.Context, exc []string) error {
				c.Exceptions = exc
				return nil
			},
			Required: false,
		},
		&cli.StringFlag{
			Name:    "threshold",
			Aliases: []string{"T"},
			Usage:   "drop metrics for tables below this size",
			Value:   units.HumanSize(float64(c.Threshold)),
			Action: func(_ *cli.Context, threshold string) error {
				th, err := units.FromHumanSize(threshold)
				if err != nil {
					return err
				}
				c.Threshold = th
				return nil
			},
			Required: false,
		},
		&cli.DurationFlag{
			Name:        "interval",
			Aliases:     []string{"i"},
			Value:       c.Interval,
			Destination: &c.Interval,
			Usage:       "polling interval",
			Required:    false,
		},
		&cli.StringFlag{
			Name:        "prefix",
			Aliases:     []string{"P"},
			Usage:       "prefijo para las métricas",
			Value:       c.Prefix,
			Destination: &c.Prefix,
			Required:    false,
		},
	}
}

func (c *config) Validate() error {
	if c.Timeout < 0 {
		return errors.New("timeout must be greater than 0")
	}
	if c.Port <= 1024 {
		return errors.New("port must be greater than 1024")
	}
	if c.Interval < 5*time.Minute {
		return errors.New("period must be greater than 5 minutes")
	}
	passwd := os.Getenv("PGPASSWORD")
	if passwd == "" {
		return errors.New("PGPASSWORD environment must be set")
	}
	c.Password = passwd
	return nil
}

func (c config) Connect(ctx context.Context, logger *slog.Logger, database string) (*pgx.Conn, error) {
	connString := fmt.Sprintf("postgres://%s:%s@%s:%d/%s", c.Username, c.Password, c.Host, c.Port, database)
	return pgx.Connect(ctx, connString)
}

// Dispose of the connection to the database
func (c config) Dispose(ctx context.Context, logger *slog.Logger, conn *pgx.Conn, database string) error {
	return conn.Close(ctx)
}

func (c config) Start(ctx context.Context, logger *slog.Logger) http.Handler {
	scannerConfig := scanner.Defaults()
	scannerConfig.InitialDB = c.InitialDB
	scannerConfig.Threshold = c.Threshold
	scannerConfig.Exceptions = append(scannerConfig.Exceptions, c.Exceptions...)
	var sharedBuffer *bytes.Buffer
	var bufferLock sync.Mutex
	go func() {
		timer := time.NewTimer(0)
		for {
			select {
			case <-ctx.Done():
				return
			case <-timer.C:
				buffer := bytes.NewBuffer(nil)
				if err := scanner.Scan(ctx, logger, scannerConfig, c, scanner.TextWriter(buffer, c.Prefix)); err != nil {
					logger.Error("failed to scan", "error", err)
				}
				func() {
					bufferLock.Lock()
					defer bufferLock.Unlock()
					sharedBuffer = buffer
				}()
				timer.Reset(time.Duration(c.Interval) * time.Minute)
			}
		}
	}()
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Body != nil {
			defer func(body io.ReadCloser) {
				io.Copy(io.Discard, body)
				body.Close()
			}(r.Body)
		}
		if r.Method != http.MethodGet {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		if r.URL.Path == "/" {
			http.Redirect(w, r, "/metrics", http.StatusFound)
			return
		}
		if r.URL.Path != "/metrics" {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		var buffer *bytes.Buffer
		// Closure for defer
		func() {
			bufferLock.Lock()
			defer bufferLock.Unlock()
			buffer = sharedBuffer
		}()
		w.Header().Add("Content-Type", "text/plain; version=0.0.4")
		w.WriteHeader(http.StatusOK)
		w.Write(buffer.Bytes())
	})
}

func main() {
	cfg := defaults()
	app := &cli.App{
		Name:  "pgexport",
		Usage: "Expose database and table sizes as Prometheus metrics",
		Flags: cfg.Flags(),
		Action: func(cCtx *cli.Context) error {
			logger := slog.Default()
			if err := cfg.Validate(); err != nil {
				logger.Error("invalid configuration", "error", err.Error(), "config", cfg)
				return err
			}
			ctx, cancelFunc := context.WithCancel(context.Background())
			defer cancelFunc()
			server := http.Server{
				Addr:         cfg.Address,
				ReadTimeout:  time.Duration(cfg.Timeout) * time.Second,
				WriteTimeout: time.Duration(cfg.Timeout) * time.Second,
				Handler:      cfg.Start(ctx, logger),
			}
			logger.Info("Waiting for requests", "config", cfg)
			return server.ListenAndServe()
		},
	}

	if err := app.Run(os.Args); err != nil {
		log.Fatal(err)
	}
}

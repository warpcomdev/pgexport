# Postgres DB size exporter

Esta aplicación exporta información sobre el tamaño de las bases de datos y tablas de una base de datos postgres, en el formato `text-exposition` de prometheus.

Modo de uso:

```bash
$ ./pgexport  --help
NAME:
   pgexport - Expose database and table sizes as Prometheus metrics

USAGE:
   pgexport [global options] command [command options]

COMMANDS:
   help, h  Shows a list of commands or help for one command

GLOBAL OPTIONS:
   --address value, -a value                                      address to listen on (default: ":8080")
   --timeout value, -t value                                      HTTP timeout (default: 5s)
   --host value, -H value                                         database host (default: "localhost")
   --port value, -p value                                         database port (default: 5432)
   --username value, -U value                                     database user (default: "postgres")
   --initialdb value, -d value                                    initial database (default: "postgres")
   --exceptions value, -e value [ --exceptions value, -e value ]  databases to omit - besides 'template0', 'template1', 'postgres'
   --threshold value, -T value                                    drop metrics for tables below this size (default: "1GB")
   --interval value, -i value                                     polling interval (default: 30m0s)
   --prefix value, -P value                                       prefijo para las métricas
   --verbose, -v                                                  muestra logs verbosos (default: false)
   --help, -h                                                     show help
```

Métricas exportadas:

- `database_size_bytes`
- `table_is_hypertable`
- `table_size_bytes`
- `table_relation_size_bytes`
- `table_index_size_bytes`

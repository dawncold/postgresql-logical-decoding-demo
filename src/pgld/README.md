## install wal2json
```bash
apt install postgresql-11-wal2json
```

## postgresql.conf
You need to set up at least two parameters at postgresql.conf:

```
wal_level = logical
max_replication_slots = 1
```

## pg_hba.conf
You need a replication trusted entry, e.g.
```
local   replication     all     trust
```

## restart postgresql
```
/usr/lib/postgresql/11/bin/pg_ctl -D /var/lib/postgresql/data/ restart
```

## create database and table

## start RabbitMQ, PurchaseService and CreditCardService

```
~/Downloads/rabbitmq_server-3.7.12/sbin/rabbitmq-server

source vent/bin/activate
python src/pgld/main.py
python src/pgld/credit_card.py
```
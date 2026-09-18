#!/bin/bash

set -e

psql \
	-v NO_ERROR_STOP=1 \
	--username "$POSTGRES_USER" \
	--dbname "$POSTGRES_DB" \
	-v debezium_user="$DEBEZIUM_USER" \
	-v debezium_pass="$DEBEZIUM_PASS" <<'SQL'

CREATE USER :"debezium_user"
	WITH PASSWORD :'debezium_pass';

ALTER USER :"debezium_user" WITH REPLICATION;

GRANT CONNECT ON DATABASE ecommerce TO :"debezium_user";

GRANT USAGE ON SCHEMA public TO :"debezium_user";

GRANT SELECT ON ALL TABLES IN SCHEMA public TO :"debezium_user";

ALTER DEFAULT PRIVILEGES IN SCHEMA public
	GRANT SELECT ON TABLES TO :"debezium_user";

SQL

DB=$1
if [ x${DB} = x ]; then echo "database name required" 1>&2; exit 1; fi

cat <<EOF | mysql
CREATE DATABASE ${DB};
GRANT ALL ON ${DB}.* TO 'memento_rw'@'localhost';
GRANT SELECT on ${DB}.* to 'memento_ro'@'%';
EOF

mysql ${DB} <sql/memento_dbtables.sql


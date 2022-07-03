cat <<EOF | mysql
CREATE USER 'memento_rw'@'localhost' IDENTIFIED BY 'LKpoiinjdscudfc';
CREATE USER 'memento_ro'@'%' IDENTIFIED BY 'memento_ro';
EOF

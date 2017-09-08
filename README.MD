# index-mysql
Pho adapter for Index service based on MySQL database

# Configuration
Servise use default values as
Name             | Value                             
-----------------|----------------------
Host:            | `localhost`
Port:            | `3306`
User:            | `root`
Password:        | `(empty)`
Database:        | `phoindex`
Table:           | `indexes`

You can change this in two way:
A) In config: 
`"mysql:mysql://username:password@host:port/?database=phoindex&table=indexes"`
where you change it with own connection data. 

B) .env file: 
Host: INDEX_HOST
Port: INDEX_PORT
User: INDEX_USER
Password: INDEX_PWD
Database: INDEX_DB
Table: INDEX_TABLE
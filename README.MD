# Overall
Pho adapter for Index service based on MySQL database. Its store all attributes values from nodes and edges to table. 
Last updated code you can found [here](https://github.com/pho-adapters/index-mysql/).

### Install
This apapter already inclided in [pho-microcernel](https://github.com/phonetworks/pho-microkernel). 
We rccommend composer to install it:
```
composer require pho-adapters/index-mysql
```

# Setup 

## Setup database
Database must include table with this structure:
```
CREATE TABLE IF NOT EXISTS `index` ( 
    `uuid` VARCHAR(32) NOT NULL, 
    `class` VARCHAR(255) NOT NULL, 
    `key` VARCHAR(255) NOT NULL, 
    `value` TEXT NOT NULL, 
    INDEX (`uuid`, `class`) 
) ENGINE=INNODB.
```
If you do not have that one 

# Configuration
Service use default values: 

Name            | Value
----------------|--------------
Host            | localhost
Port            | 3306
User            | root
Password        | (empty)
Database        | phoindex
Table           | indexes

You can change this parameters in two way:
##### A) In config: 
`mysql://username:password@host:port/?database=(you_database)&table=(table)`
where you change it with own connection data. 

##### B) .env file: 
Name             | Value                
-----------------|-----------------
Host:            | INDEX_HOST
Port:            | INDEX_PORT
User:            | INDEX_USER
Password:        | INDEX_PWD
Database:        | INDEX_DB
Table:           | INDEX_TABLE
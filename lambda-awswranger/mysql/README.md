# TODO:
* create a table manually
* add partition if necessary

```bash
DESTINATION_PATH=s3://xxxxxxxxxxxxx-databases/mysql
LIMIT=10000
LOG_LEVEL=10
MYSQL_DATABASE=bank
MYSQL_HOST=mysql-1.xxxxxxxxxxx.us-east-1.rds.amazonaws.com
MYSQL_PASS=Jhxxxxxxxxxxxxsfs
MYSQL_PORT=3306
MYSQL_USER=admin
OFFSET_PARAMETER_NAME=/data-ingestion/lambda/offset
SAVE_MODE=append
TABLE_COLUMNS=*
TABLE_NAME=transactions
```
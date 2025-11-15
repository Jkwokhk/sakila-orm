Setup:
```
cd ./sakila-orm  

python3 -m venv venv

source venv/bin/activate

pip3 install Django mysqlclient dot-env
```

Environment variables
```
cd ./sakila-orm
touch .env
DB_USER = "YOUR_USERNAME"
DB_PASSWORD = "YOUR_PASSWORD"
```
To run
```
cd sakila-orm

python3 manage.py init

python3 manage.py full-load

python3 manage.py incremental

python3 manage.py validate

```
To test run
```
python3 test_commands.py

```






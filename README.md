Setup:
cd ./sakila-orm
python3 -m venv venv
source venv/bin/activate
pip3 install Django mysqlclient dot-env

Naviage to settings.py -> In DATABASES, replace USER and PASSWORD to your own

Instructions to run program:
In sakila-orm dir, 
cd sakila-orm
python3 manage.py init
python3 manage.py full-load
python3 manage.py incremental
python3 manage.py validate

To test run
python3 test_commands.py

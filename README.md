# airflow-sqlsensor
A repository to show how to use Apache Airflow SqlSensor for checking Database changes

This is a code to check your database every second and notice you, or write them every time a new reord added to your database.

**VERY IMPORTANT NOTE:** 

If you are runnig Apache Airflow on docker and your database is on your local machine, do not use `localhost` as your host address when you want to make a connection to your databse (either in the code or when you want to define a connection in airflow ui connection tab) use `host.docker.internal` instead. 

Stack overflow: [add an airflow connection to a localhost database](https://stackoverflow.com/questions/68308437/add-an-airflow-connection-to-a-localhost-database-postgres-running-on-docker)

**Before using the code change the following parameters with your :**

`db_host`: your database address

`db_port`: your database port

`db_user`: database user

`db_password`: your datbase password

`db_name`: your database name

`TABLE`: table you want to check it's changes

`DATE_COL`: date column or every column your changes based on

`CONN_ID`: the connection id which you defined in airflow for your databse

This repo will be updated ...

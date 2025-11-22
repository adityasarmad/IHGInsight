
pip install -r /requirements.txt


airflow db init

airflow users create --username admin --firstname Aditya --lastname Desai --role Admin --email admin@example.com --password admin

airflow scheduler  2>&1 &

airflow webserver
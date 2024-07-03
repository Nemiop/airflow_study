# airflow_study
Study airflow

sudo pip3 install virtualenv

virtualenv airflow_env

source airflow_env/bin/activate

pip3 install --upgrade pip
pip3 install apache-airflow[gcp,sentry,statsd]
airflow db init

airflow standalone
Or use two terminals:
1:
airflow scheduler 

2:
airflow webserver

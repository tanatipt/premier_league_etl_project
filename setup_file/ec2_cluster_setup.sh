sudo apt update
sudo apt install -y python3-pip
sudo apt install -y sqlite3
sudo apt-get install -y libpq-dev
pip3 install --upgrade awscli
pip3 install boto3
sudo pip3 install virtualenv 
virtualenv venv 
source venv/bin/activate
pip install "apache-airflow[postgres]==2.5.0" --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.5.0/constraints-3.10.txt"
pip install pandas apache-airflow-providers-snowflake==2.1.0 snowflake-connector-python==2.5.1 snowflake-sqlalchemy==1.2.5
airflow db init
sudo apt-get install postgresql postgresql-contrib
sudo -i -u postgres
psql
CREATE DATABASE airflow;
CREATE USER airflow WITH PASSWORD 'airflow';
GRANT ALL PRIVILEGES ON DATABASE airflow TO airflow;
exit
exit
ls
cd airflow
sed -i 's#sqlite:////home/ubuntu/airflow/airflow.db#postgresql+psycopg2://airflow:airflow@localhost/airflow#g' airflow.cfg
sed -i 's#SequentialExecutor#LocalExecutor#g' airflow.cfg
airflow db init
airflow users create -u airflow -f airflow -l airflow -r Admin -e airflow1@gmail.com
User id --airflow
password--admin@123! 
mkdir /home/ubuntu/dags
cd airflow
vi airflow.cfg
change the below properties --
dags_folder = /home/ubuntu/dags
load_examples = False

airflow db init
airflow webserver

source venv/bin/activate
airflow scheduler
















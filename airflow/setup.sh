# install docker

sudo apt update
sudo apt install apt-transport-https ca-certificates curl software-properties-common -y
curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo apt-key add -
sudo add-apt-repository "deb [arch=amd64] https://download.docker.com/linux/ubuntu focal stable" -y
apt-cache policy docker-ce

sudo apt install docker-ce -y
# sudo usermod -aG docker ${USER}
# su - ${USER}


# install airflow

curl -LfO 'https://airflow.apache.org/docs/apache-airflow/2.8.1/docker-compose.yaml'
mkdir -p ./dags ./logs ./plugins ./config
echo -e "AIRFLOW_UID=$(id -u)" > .env
sudo docker compose up airflow-init

# заменить в docker-compose.yaml 
# image: ${AIRFLOW_IMAGE_NAME:-apache/airflow:2.6.1}
# # build: .
# на 
# # image: ${AIRFLOW_IMAGE_NAME:-apache/airflow:2.6.1}
# build: .

touch Dockerfile
# содержимое:
# FROM apache/airflow:2.8.1
# ADD requirements.txt .
# RUN pip install apache-airflow==${AIRFLOW_VERSION} -r requirements.txt

touch requirements.txt

# fill foloder /dags

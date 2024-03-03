git clone https://github.com/uncle-alfer/otus-mlops-homework-project.git

# fill ~/otus-mlops-homework-project/mlflow/environment
# fill ~/otus-mlops-homework-project/mlflow/credentials

cp ~/otus-mlops-homework-project/mlflow/environment /etc/environment
mkdir ~/.aws
cp ~/otus-mlops-homework-project/mlflow/credentials ~/.aws/credentials

curl -O https://repo.anaconda.com/archive/Anaconda3-2023.07-1-Linux-x86_64.sh

bash Anaconda3-2023.07-1-Linux-x86_64.sh

conda create -n mlflow

conda activate mlflow

conda install -c conda-forge mlflow
conda install -c anaconda boto3
pip install psycopg2-binary
pip install pandas




TERRAFORM

sudo apt-get install unzip
wget https://hashicorp-releases.yandexcloud.net/terraform/1.8.0-alpha20240228/terraform_1.8.0-alpha20240228_linux_amd64.zip

unzip terraform_1.8.0-alpha20240228_linux_amd64.zip

export PATH=$PATH:/home/terraform

curl -sSL https://storage.yandexcloud.net/yandexcloud-yc/install.sh | bash
yc init
<token>
..

yc iam key create --service-account-id ajeudhbo9vdspmps1hip --output key.json

yc config profile create project-profile

yc config set service-account-key key.json
yc config set cloud-id b1g96ctfvketflpqes0u
yc config set folder-id b1gob40s11qs9fivife8

export YC_TOKEN=$(yc iam create-token)
export YC_CLOUD_ID=$(yc config get cloud-id)
export YC_FOLDER_ID=$(yc config get folder-id)

touch ~/.terraformrc
```
provider_installation {
  network_mirror {
    url = "https://terraform-mirror.yandexcloud.net/"
    include = ["registry.terraform.io/*/*"]
  }
  direct {
    exclude = ["registry.terraform.io/*/*"]
  }
}
```

terraform init

# ansible
sudo apt update
sudo apt install pip -y
python3 -m pip install --user pipx
python3 -m pipx ensurepath
pipx install --include-deps ansible
#/bin/bash

repo_dir=`git rev-parse --show-toplevel`
echo -e "\033[1;34mStep 1: Activating venv ... \033[0m"
cd $repo_dir && source ./venv/Scripts/activate
echo -e "\033[1;34mStep 2: Installin pip-tools ...\033[0m"
pip install pip-tools
echo -e "\033[1;34mStep 3: Removing old deps ...\033[0m"
if [ -f $repo_dir/project/requirements/requirements*.txt ]; then
	rm $repo_dir/project/requirements/requirements*.txt
fi
echo -e "\033[1;34mStep 4: Creating requirements.txt ...\033[0m"
pip-compile ./project/requirements/requirements.in --output-file ./project/requirements/requirements.txt --no-emit-index-url --no-emit-find-links --no-emit-trusted-host
echo -e "\033[1;34mStep 5: Creating requirements-dev.txt ...\033[0m"
pip-compile ./project/requirements/requirements-dev.in --output-file ./project/requirements/requirements-dev.txt --no-emit-index-url --no-emit-find-links --no-emit-trusted-host
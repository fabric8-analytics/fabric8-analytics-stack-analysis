#!/bin/bash -xe

# --------------------------------------------------------------------------------------------------
# this script will be executed by aws spark emr during bootstrap process of each node
# we install python dependencies of our training job here
# --------------------------------------------------------------------------------------------------

sudo pip install cython==0.25.2 --disable-pip-version-check
sudo pip install uuid==1.30 --disable-pip-version-check
sudo pip install boto3==1.4.4 --disable-pip-version-check
sudo pip install boto==2.46.1 --disable-pip-version-check
sudo pip install pandas==0.19.2 --disable-pip-version-check
sudo pip install sklearn --disable-pip-version-check
sudo pip install numpy==1.13.0 --disable-pip-version-check
sudo pip install scipy==0.19.0 --disable-pip-version-check
sudo pip install psutil==4.3.0 --disable-pip-version-check
sudo pip install nltk==3.2.5 --disable-pip-version-check
sudo pip install pomegranate==0.7.3 --disable-pip-version-check
sudo pip install daiquiri==1.3.0 --disable-pip-version-check
#wget -P /tmp https://github.com/pgmpy/pgmpy/archive/dev.zip
#cd /tmp
#unzip -u dev.zip
#cd pgmpy-dev
#sudo pip install -r requirements.txt
#sudo python setup.py install
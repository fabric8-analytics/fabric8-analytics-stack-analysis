#!/bin/bash -xe

# --------------------------------------------------------------------------------------------------
# this script will be executed by aws spark emr during bootstrap process of each node
# we install python dependencies of our training job here
# --------------------------------------------------------------------------------------------------

sudo pip install cython==0.25.2
sudo pip install uuid
sudo pip install boto3==1.4.4
sudo pip install boto==2.46.1
sudo pip install pandas
sudo pip install sklearn
sudo pip install numpy==1.13.0
sudo pip install scipy==0.19.0
sudo pip install psutil
sudo pip install nltk==3.2.5
sudo pip install pomegranate==0.7.3
sudo pip install daiquiri==1.3.0
#wget -P /tmp https://github.com/pgmpy/pgmpy/archive/dev.zip
#cd /tmp
#unzip -u dev.zip
#cd pgmpy-dev
#sudo pip install -r requirements.txt
#sudo python setup.py install
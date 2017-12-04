FROM centos:7
MAINTAINER Avishkar Gupta <avgupta@redhat.com>

ENV LANG=en_US.UTF-8

RUN yum install -y epel-release && \
    yum install -y zip && \
    yum install -y gcc && \
    yum -y install tkinter && \
    yum-config-manager --disable testing-devtools-2-centos-7 && \
    yum -y install gcc-c++.x86_64 && \
    yum clean all

RUN yum -y install https://centos7.iuscommunity.org/ius-release.rpm
RUN yum -y install python36u && \
    yum -y install python36u-pip && \
    yum -y install python36u-devel

# --------------------------------------------------------------------------------------------------
# install python packages
# --------------------------------------------------------------------------------------------------
COPY ./analytics_platform/kronos/requirements.txt /
RUN python3.6 -m pip install -r /requirements.txt && rm /requirements.txt
RUN python3.6 -m pip install pomegranate==0.7.3

# --------------------------------------------------------------------------------------------------
# copy src code and scripts into root dir /
# the rest_api.py code assumes this dir structure
# --------------------------------------------------------------------------------------------------
COPY ./analytics_platform/kronos/deployment/rest_api.py /rest_api.py
COPY ./analytics_platform/kronos/scripts/bootstrap_action.sh /
COPY ./tagging_platform/helles/scripts/bootstrap_action.sh /helles_bootstrap_action.sh
COPY ./analytics_platform /analytics_platform
COPY ./tagging_platform /tagging_platform
COPY ./util /util
COPY ./analytics_platform/kronos/src/config.py.template /analytics_platform/kronos/src/config.py

# --------------------------------------------------------------------------------------------------
# add entrypoint for the container
# --------------------------------------------------------------------------------------------------
ADD ./analytics_platform/kronos/scripts/entrypoint.sh /bin/entrypoint.sh

ENTRYPOINT ["/bin/entrypoint.sh"]

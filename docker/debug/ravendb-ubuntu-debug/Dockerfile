FROM ravendb/ravendb:5.2-ubuntu-latest

EXPOSE 22 8080 38888

# install SSH server and set root password to 'debug'
RUN apt-get update && \
    apt-get install -y openssh-server unzip curl && \
    sh -c "echo 'root:debug' | chpasswd" && \
    sed -i '/PermitRootLogin/c\PermitRootLogin yes' /etc/ssh/sshd_config

CMD service ssh start && \
    /opt/RavenDB/run-raven.sh

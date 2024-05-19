FROM centos:7
ENV TERM=xterm;\
    DOWNLOAD=https://github.com/etcd-io/etcd/releases/download/v3.4.32/ \
    ETCDVERSION=etcd-v3.4.32-linux-amd64 \
    USER=admin 
RUN yum install mkdir curl wget tar chown unzip -y;\
    useradd ${USER} ;\
    mkdir -p /export/{servers,Logs,packages,Apps,Shell};\
    wget ${DOWNLOAD}${ETCDVERSION}.tar.gz && tar -zxf ${ETCDVERSION}.tar.gz -C /export/servers/ && \
    /bin/rm -rf ${ETCDVERSION}.tar.gz;\
    chown -R ${USER}.${USER} /export ;\
    ln -s /export/servers/${ETCDVERSION}/etcd* /usr/bin/;
EXPOSE 2379 2380


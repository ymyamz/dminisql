FROM centos:7

ENV GOROOT /usr/local/go  
ENV GOPATH /data/gopath  
ENV PATH $GOROOT/bin:$PATH  
ENV CGO_ENABLED=1  
ENV TERM=xterm;\
    DOWNLOAD=https://github.com/etcd-io/etcd/releases/download/v3.4.32/ \
    ETCDVERSION=etcd-v3.4.32-linux-amd64 \
    USER=admin 

RUN yum install mkdir curl wget tar curl chown unzip git gcc gcc-c++ -y;\
    useradd ${USER} ;\
    mkdir -p /export/{servers,Logs,packages,Apps,Shell};\
    wget ${DOWNLOAD}${ETCDVERSION}.tar.gz && tar -zxf ${ETCDVERSION}.tar.gz -C /export/servers/ && \
    /bin/rm -rf ${ETCDVERSION}.tar.gz;\
    chown -R ${USER}.${USER} /export ;\
    ln -s /export/servers/${ETCDVERSION}/etcd* /usr/bin/;

RUN curl -s -o go.tar.gz https://storage.googleapis.com/golang/go1.22.0.linux-amd64.tar.gz
RUN tar --remove-files -C /usr/local/ -zxf go.tar.gz
RUN mkdir -p /data/go
RUN ln -sv /usr/local/go/bin/go /bin

WORKDIR /data/gopath
RUN git clone https://github.com/ymyamz/dminisql.git dminisql  
EXPOSE 2379 2380


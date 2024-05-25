docker network create --driver bridge --subnet 172.20.0.0/16 --gateway 172.20.0.1 etcd-cluster

配置的docker文件在env文件夹中。
先运行 docker build --no-cache -t term-node .
在运行docker-compose up -d

开终端docker exec -it etcd-node1 /bin/bash进入节点测试
git clone https://github.com/ymyamz/dminisql.git

master默认为etcd-node1,172.20.0.10

TODO
6个终端（不同的ip地址）
在本地一个client,一个master,一个region
分布式etcd（远程）

create table suer(name TEXT,address TEXT); 
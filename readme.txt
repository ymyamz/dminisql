docker network create --driver bridge --subnet 172.20.0.0/16 --gateway 172.20.0.1 etcd-cluster

配置的docker文件在env文件夹中。
先运行 docker build -t etcd-shell .
在运行docker-compose up -d

开终端docker exec -it etcd-node1 /bin/bash进入节点测试

master默认为etcd-node1,172.20.0.10
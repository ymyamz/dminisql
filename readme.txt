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

 

使用步骤：
1，打开三个终端，分别在三个终端分别输入以下命令：
go run main.go region 
go run main.go master
go run main.go client

2，在client终端输入以下命令：
create table user (name TEXT,address TEXT);
create table user_2 (name TEXT,address TEXT);
show tables;

INSERT INTO user (name, address) VALUES ('John Doe', '123 Main Street');  
可以看到client输出向master询问table所在ip的结果
SELECT * FROM user;  
可以看到执行结果
docker配置步骤：

配置的docker文件在env文件夹中。
运行：
docker build --no-cache -t term-node .
docker network create --driver bridge --subnet 172.20.0.0/16 --gateway 172.20.0.1 etcd-cluster
docker-compose up -d

打开三个终端，依次类推能进入三个节点etcd-node1，etcd-node2，etcd-node3：
开终端docker exec -it etcd-node1 /bin/bash进入节点测试
git clone https://github.com/ymyamz/dminisql.git
cd dminisql
注意docker运行代码要加上一个命令行参数d
go run main.go master d

master默认为etcd-node1,172.20.0.10！！！client可以随便在哪里节点都行
然后etcd-node2,3运行go run main.go region d

可测试指令如下：
create table user(name TEXT,address TEXT);
show tables;

INSERT INTO user (name, address) VALUES ('John Doe', '123 Main Street');
SELECT * FROM user;  


CREATE INDEX index_name ON table_name(name);
show indexes;
DROP INDEX index_name;
（旧的init可以运行，新的好像有问题）

go run main.go region l 8002
select * from user cross join user1;

 select * from user cross join user2;



create table user(name TEXT,address TEXT);

create table user2(name TEXT,address TEXT);
INSERT INTO user (name, address) VALUES ('John Doe', '123 Main Street');

INSERT INTO user2 (name, address) VALUES ('John Doe2', '1234 Main Street');

 select * from user cross join user2;

create table user3(name TEXT,address TEXT);
INSERT INTO user3 (name, address) VALUES ('John Doe1', '123 Main Street');

INSERT INTO user3 (name, address) VALUES ('John Doe2', '123 Main Street');

 select * from user cross join user3;

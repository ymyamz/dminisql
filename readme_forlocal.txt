初始化逻辑可以考虑重写（master.gob）
把ip地址改为port，所以新建终端输入go run main.go region l 8001代表新建了一个region。
请选择8001,8002,8003，...类推，不可以使用8000（这是master的port）

打开终端输入go run main.go region l 8001，go run main.go region l 8002，go run main.go region l 8003
打开终端输入go run main.go master l
go run main.go client l
（必须加l意思是local）

其中region的data数据都保存在./data/[port_name].db文件中
可使用文件测试，指令如下 例如".read ./sql/test.txt;"（严格要求是.read加一个空格开头）test.txt文件内容中每行一个命令加分号

打开etcd-v3.4.31-windows-amd64目录，已配置好config，打开一个终端输入.\etcd.exe --config-file .\config.yml
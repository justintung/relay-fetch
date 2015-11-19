# relay-fetch
备库预热工具 relayfetch及性能测试
### 如何使用
    下载：
    git clone https://github.com/justintung/relay-fetch.git
    cd relay-fetch
    make
###执行程序
    例如： ./relayfetch –uroot -S /u01/mysql/run/mysql.sock –D
###选项 描述
- -a [NUM]当落后NUM秒时，唤醒read relay log 线程
- -d Debug log
- -D 后台运行
- -u 用户名
- -p 密码
- -s [NUM]m，限制防止过快（默认为1M）预热读relay log和SQL线程执行位置的距离
- -S Socket路径
- -n Worker线程数
- -t 当类似tb_1、tb_2、tb_3这样的表命名方式，但具有不同的表模式时。请加上该选项

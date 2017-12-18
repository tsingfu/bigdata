# 大数据平台

### 安装步骤
```
    1 准备数据库和java环境
    2 参考执行 https://raw.githubusercontent.com/xiaomatech/ambari/master/ambari_server_install.sh
```

### 此系统已实现的功能(能干什么)
```
实现了常用的十几个功能
主要有:
    1 57个大数据组件的安装部署升级
    2 组件的存活监控 和url监控
    3 日志管理(有跟elk很像的logsearch组件)
    4 操作审计(hdfs,hive,hbase,kafka,storm等) 能知道谁在什么时候操作了什么资源
    5 权限控制(支持hdfs,hive,hbase,kafka,storm等) 支持行列级的权限管理
    6 监控告警(不止是系统的监控 还包括各种组件的jvm metrics的监控)
    7 自动扩容
    8 组件的配置都是经过生产实践考验的配置(50PB+数据 , 1k+ server, 10w+job/天)
```

### hdfs nameservice划分
```
```
### 需要开发什么系统和与什么内部系统对接
```
```
### 主要组件介绍
```
```

### 大概都需要怎么样的服务器
```
    大数据组件部署的原则是混部 计算尽量靠近存储 大部分都是写入后不修改的append操作
    主要需要3类机器
    1类是master(几台就够)
        典型比如namenode 需要超大内存推荐256G 4个ssd硬盘做raid1备份 2个做raid1的系统盘
    1类是存储型机器
        典型如datanode 12个6T/8T sata盘 不做raid 内存也尽量128G以上 2个做raid1的系统盘
    1类是client型机器
        一般用来提交job的 类似gateway的角色 一般虚拟机即可
    
    网络
        单万兆网卡即可(现在都很难买到千兆网卡了) 不做bond 
        kafka 是典型的网络瓶颈型 可配置双万兆网卡 做bond6
    
    如果条件许可 可在每台datanode配1个或2个ssd盘 用来做cache (hadoop自带此混合存储的功能)
    
```

### 系统分区和挂载
```
    主要注意点是/var 需要单独分区 给个100G/200G 主要用来存组件日志 大数据的日志都非常多,虽会自动轮转 但也怕写爆/目录
     
    master  /var /data01 /data02
    data /var /data01 /data02 /data03 /data04 /data05 /data06 /data07 /data08 /data09 /data10 /data11 /data12
    client /var
    
```

### 元数据在哪几个地方
```
    大数据组件的 主要在 hive的metaserver db , zk , db , hdfs ,kafka , hbase 
    
    linux用户数据主要在ldap 安全票据在kdc
```

#### 组件安装部署的前提
```
### 部署环境要求
1 hostname能通过dns解析
     检验方法 dig `hostname`
2 /etc/yum.repos.d 下 yum源正常
     确保能正常安装jdk,ambari-agent,openldap-client,kerberos client
     yum install -y snappy snappy-devel lzo zlib bzip2 libzip openssl libtirpc-devel tar 
     curl jdk openldap-clients nss-pam-ldapd pam_ldap pam_krb5 authconfig krb5-libs libcgroup jdk1.8.0_144 
3 ambari-server 有个rsa key能登录到被管理的机器
     2个可选方案:
         1,ambari-server生成的pub key分发到被管理机器
         2,使用安装系统的时候初始化好的本地账户,类似yunwei这种神账号
4 被管理机器要能ping通ambari-server,因为要注册agent和发送回心跳
5 从http://mirrors.aliyun.com/apache/ 下载对应的压缩安装包 到 http://yum.example.com/hadoop 目录下 
  大数据的安装包之类的都从这里下载 当然这个download_url 可以在每个组件的*-env中修改
6 下载 http://public-repo-1.hortonworks.com/ambari/centos7/2.x/updates/2.6.0.0/ambari-2.6.0.0-centos7.tar.gz 
  到自建的yum源 这包主要有ambari-agent,ambari-server,logsearch,solr,metrics 
7 能ping通cmdb(需要获取hadoop机架感知数据 如果没有这接口则没有自动机架感知功能) 
```

### 组件使用的总体原则
```
不管有多少集群 只有一套ldap(用户管理) 一套kdc(安全票据) 一套ranger(授权和审计) 一套atlas(元数据管理 数据血缘)
```
### 组件安装顺序
```
1 安装zk
2 安装kdc/openldap 或者 zookeeper-env填写对应的kdc_hosts,ldap_hosts
3 启动kerberos
```

### 自动部署和扩容大数据组件
```
blueprint介绍
 ambari用来自动部署大数据组件的功能
参考
 https://cwiki.apache.org/confluence/display/AMBARI/Blueprints
 https://community.hortonworks.com/articles/78969/automate-hdp-installation-using-ambari-blueprints-4.html
```

### 组件日志管理与扩容步骤
```
日志管理介绍
 大数据除了组件自身的job日志会汇总到hdfs外
 还有 ranger的审计日志,atlas的搜索功能,组件运行日志(logsearch)
对应的collect的关系
  ranger -> ranger_audits  默认保存14天
  logsearch -> hadoop_logs , audit_logs 默认保存7天
  atlas -> edge_index,vertex_index,fulltext_index 默认保存7天
现阶段是用solrcloud分了32个shard
 扩容步奏参考官方文档
 ```
 
### hdfs扩容
```
在ambari添加机器并安装datanode
在ambari页面 刷新namenode下的配置(做了机架感知 没刷新的时候是默认/default-rack ,datanode加不进集群)
```
 
### namenode ,ldap/kdc ,keytab 备份与恢复
 ```
 namenode 元数据(fsimage)备份与恢复
  备份在yjd-mtc-2-35.example.com中的/data/backup/hadoop目录下 。备份周期：每天凌晨一点备份 全备份,保留3天 。
  备份命令/data/backup/namenode.sh
  恢复步骤 : 
  https://docs.hortonworks.com/HDPDocuments/HDP2/HDP-2.6.2/bk_hdfs-administration/content/get_ready_to_backup.html
 kdc/ldap 备份与恢复
  备份在每台kdc的/data/backup/ldap 备份命令/data/backup/ldap.sh  备份周期: 每小时一全备 保留3天
  恢复:
      先删除所有条目： ldapdelete -x -D "cn=manager,dc=example,dc=com" -w "密码" -r "dc=example,dc=com"
      恢复：ldapadd -x -D "cn=manager,dc=example,dc=com" -f  /data/backup/ldap/ldap_时间.ldif -w 密码
 keytab 备份与恢复
   每次操作前都会备份/etc/security/keytabs下到/data/backup/keytab
   支持恢复到之前任意一次的keytab
   恢复的时候注意保留文件权限 使用 cp -rpf /data/backup/keytab/时间/keytabs/* /etc/security/keytabs/
 ```

### 安全组件ranger原理、注意项、插件开启
```
解决的问题
 hdfs,yarn,hive,spark,hbase,storm,kafka,atlas,solr等的统一权限管控
管理的数据流
 ranger-admin 负责策略存储和提供 http restful api对外提供策略查询和修改服务
 ranger-usersync 负责把用户,用户组数据从ldap中同步到ranger
 ranger-tagsync 负责把atlas上打的标签同步到ranger
插件的数据流
 1 插件启用后 会http定期(默认30秒)从ranger-admin下载策略到本地/etc/ranger
 2 通过解析策略成类似一个大hashmap 当请求过来的时候有个filter(checkPermission)做权限控制
 3 结果日志写回到solr,hdfs(新版的支持kafka没开启)
影响的组件
 主要是对应的master节点:hdfs是namenode,yarn是resourcesmanager,hive是hiveserver2,
                        hbase是master,regionserver,kafka是broker,storm是nimbus
注意点
 1 hadoop-env.sh,yarn-env.sh,hive-env.sh,hbase-env.sh等要把ranger的jar加入到对应的classpath 使能找到对应的类
 2 hdfs-site.xml,yarn-site.xml,hbase-site.xml,hiveserver2.xml要加入对应的filter类

安装的时候在 ranger-env中可以修改对应的版本 ，修改后重启对应的插件就会自动升级到指定版本的插件

```

### kerberos开启、jce验证keytab缺失怎么处理
```
 介绍
     kerberos是一个安全交互协议
     jce是jdk中用来加解密的jar包
     oracle jdk中没有带jce 需要自己安装
     openjdk 自带jce
 查看kdc地址
      cat /etc/krb5.conf |grep 'kdc ='
 查看jce状态 kerberos模式下 应该开启
     /usr/java/default/bin/java -jar /var/lib/ambari-agent/tools/jcepolicyinfo.jar -tu
  
 keytab 备份与恢复
     每次操作前都会备份/etc/security/keytabs下到/data/backup/keytab
     支持恢复到之前任意一次的keytab
     恢复的时候注意保留文件权限 使用 cp -rpf /data/backup/keytab/时间/keytabs/* /etc/security/keytabs/
     
 keytab缺失处理
     尽量不要用重新生成
```
### 组件升级
```
除了hdfs,yarn外 都支持修改配置后重启来自动升级
 1 修改对应组件的download_url
 2 重启
 3 升级完成
```

#### ambari service开发步骤
```
   1 了解对应的service的构成,基本原则是一个component管一类守护进程,有多少类守护进程就有多少个component
   2 复制示例 services\EXAMPLE 目录
   3 修改 metainfo.xml 里的名字 ,component配置 必须全局唯一 
   4 修改kerberos的配置kerberos.json
   5 修改监控配置alerts.json(可配置成自带的功能端口监控,url监控)
   6 修改configuration目录中的配置(这个主要用来在web页面做配置管理)
   7 实现package 目录 实现install,start,stop,status,restart方法(这个目录用来下发给agent执行)
   8 把对应的目录打包拷贝到ambari-server的/var/lib/ambari-server/resources/stacks/XM/1.0/services
   9 重启ambari-server /etc/init.d/ambari-server restart
   10 进入http://ambari-server_ip:8080 安装部署对应的service
```

#### 大数据参考资料
```
1 最权威的是官方文档
2  hdp官方文档 https://docs.hortonworks.com/HDPDocuments/HDP2/HDP-2.6.3/index.html
3 真实电商数据仓库全流程开发详解 https://chuanke.baidu.com/v1538386-116215-258987.html 
4 青云七天学会大数据 Pro版 https://study.163.com/course/courseMain.htm?courseId=1003605105
5 阿里大数据平台核心技术 http://www.xuetangx.com/courses/course-v1:TsinghuaX+60240202X+sp/about
6 大数据科学与应用系列讲座 http://www.xuetangx.com/courses/course-v1:TsinghuaX+60250131X+sp/about
```
### 组件监控告警

```
自带支持3种类型监控告警
 1 端口监控告警
 2 url 状态码的监控告警
 3 进程状态监控告警
具体参考 service下的　alerts.json的配置
```
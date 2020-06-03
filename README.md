
aio_enhance 是一款无侵入式的 Java AIO增强类库，解决原生 AIO 通信设计中存在的缺陷，提供更高效、更稳定的 AIO 通信服务。

## 一、适用场景

所有基于 Java 原生 AIO 技术开发的通信框架或应用服务，且对性能有极致要求。

## 二、为什么需要增强

### 原因一：解决平台兼容性问题。
Java 原生 AIO 在 Mac 操作系统下存在兼容性问题，进行性能压测会偶发性的系统崩溃。

### 原因二：修复官方AIO架构缺陷
 Java 原生 AIO 在底层架构设计上存在缺陷（参考：[Java AIO通信模型](http://openjdk.java.net/projects/nio/resources/AsynchronousIo.html)）。多核 CPU 环境下处理高并发请求，会引发比较严重的锁竞争现象，以致无法充分发挥机器性能。

普通4核机器竞争压力不大，AIO 的运行表现实测优于NIO。但随着 CPU 核数的增加，AIO 的性能优势逐渐下降。

## 三、集成增强包

**步骤一：依赖**

引入增强包：aio-enhance.jar。可以通过maven方式引入依赖，亦可直接下载 jar 包并导入classpath。

**步骤二：启动**

可以通过硬编码的方式设置系统属性，如下：

```java
System.setProperty("java.nio.channels.spi.AsynchronousChannelProvider", "org.smartboot.aio.EnhanceAsynchronousChannelProvider");
```

也可在 java 启动命令行中设置，如下：

```bash
java -Djava.nio.channels.spi.AsynchronousChannelProvider=org.smartboot.aio.EnhanceAsynchronousChannelProvider xxx.jar
```


# Easy filebeat

简单的filebeat逻辑实现

### harvester


核心逻辑，主要功能点如下



### reader

### sink

### sys

copy from filebeat 项目，主要是获取文件的 I-Node 信息，用来判断文件是不是同一个文件（不受 mv 以及 cp 的影响）

1. 大家直接将这些文件夹上传到linux机器上(比如上传到~/jobs/oozie文件夹中)，然后将所有软件上传到hdfs的/oozie/workflows文件夹中。进入linux上传的目录(这里是jobs/oozie文件夹)执行oozie job -oozie http://hh:11000/oozie -config ./xxxx/job.properties -run，这里的xxxx就是各个文件夹名称。
2. 相关命令
oozie job -oozie http://hh:11000/oozie -config ./xxxx/job.properties -submit （提交任务）
oozie job -oozie http://hh:11000/oozie -start job_id (运行已经提交的任务)
oozie job -oozie http://hh:11000/oozie -config ./xxxx/job.properties -run (submit和start命令合并)
oozie job -oozie http://hh:11000/oozie -info job_id (获取任务信息)
oozie job -oozie http://hh:11000/oozie -suspend job_id (暂停/挂起任务)
oozie job -oozie http://hh:11000/oozie -rerun job_id (重新运行暂停的任务)
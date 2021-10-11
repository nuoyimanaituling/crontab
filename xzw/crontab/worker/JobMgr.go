package worker

import (
	"context"
	"fmt"
	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/mvcc/mvccpb"
	"gocrontab/xzw/crontab/common"
	"time"
)

type JobMgr struct {

	client *clientv3.Client
	lease clientv3.Lease
	kv   clientv3.KV
	watcher clientv3.Watcher
}

var (
	//单例
	G_jobMgr *JobMgr

)

// 监听任务变化
func (jobMgr *JobMgr)watchJobs()(err error){
	var(
		getResp *clientv3.GetResponse
		kvpair  *mvccpb.KeyValue
		job *common.Job
		watchStartRevision int64
		watchChan clientv3.WatchChan
		watchResp clientv3.WatchResponse
		watchEvent *clientv3.Event
		jobName string
		jobEvent *common.JobEvent
	)

	//1 :get一下/cron/jobs目录下的所有任务，并且获知当前集群的revision
	if getResp,err = jobMgr.kv.Get(context.TODO(),common.JOB_SAVE_DIR,clientv3.WithPrevKV(),clientv3.WithPrefix());err !=nil{
		return
	}

	for _,kvpair = range getResp.Kvs {
		// 反序列化json得到job
		if job,err = common.UnpackJob(kvpair.Value);err == nil{
			//TODO:把这个任务同步给schedule（调度协程），
			jobEvent = common.BuildJobEvent(common.JOB_EVENT_SAVE,job)
			G_scheduler.PushJobEvent(jobEvent)
		}
	}
	//2：从该revision向后监听事件变化，注意区分删除和强杀的区别，强杀立即停止
	go func() {// 监听协程
		// 从Get时刻的后续版本进行开始监听变化
		watchStartRevision = getResp.Header.Revision +1
		// 监听/cron/jobs/目录的后续变化
		watchChan = jobMgr.watcher.Watch(context.TODO(),common.JOB_SAVE_DIR,clientv3.WithRev(watchStartRevision),clientv3.WithPrefix())
		// 处理监听事件
		for watchResp  = range watchChan{
			for _,watchEvent = range watchResp.Events{
				switch watchEvent.Type {
				case mvccpb.PUT: // 任务保存事件
						//TODO:反序列化job，推一个更新事件给scheduler
						if job,err = common.UnpackJob(watchEvent.Kv.Value);err!=nil{
							continue
						}

						// 构建一个更新event
						jobEvent = common.BuildJobEvent(common.JOB_EVENT_SAVE,job)

				case mvccpb.DELETE :// 任务删除事件
						// TODO:推一个删除事件给scheduler
						jobName = common.ExtractJobName(string(watchEvent.Kv.Key))
						// 构建一个删除event
						job = &common.Job{Name: jobName}
						jobEvent = common.BuildJobEvent(common.JOB_EVENT_DELETE,job)
				}
				// 推给scheduler
				G_scheduler.PushJobEvent(jobEvent)
			}
		}
	}()
	return
}

// 监听强杀任务通知
func (jobMgr *JobMgr)watchKiller(){
	var(
		job *common.Job
		watchChan clientv3.WatchChan
		watchResp clientv3.WatchResponse
		watchEvent *clientv3.Event
		jobName string
		jobEvent *common.JobEvent
	)
	// 监听/cron/killer目录
	go func() { // 监听协程
		// 监听/cron/killer/目录变化
		watchChan = jobMgr.watcher.Watch(context.TODO(),common.JOB_KILLER_DIR,clientv3.WithPrefix())
		// 处理监听事件
		for watchResp  = range watchChan{
			for _,watchEvent = range  watchResp.Events{
				switch watchEvent.Type {
				case mvccpb.PUT:// 杀死任务事件
					jobName = common.ExtractKillerName(string(watchEvent.Kv.Key))
					job = &common.Job{Name: jobName}
					jobEvent = common.BuildJobEvent(common.JOB_EVENT_KILL,job)
					G_scheduler.PushJobEvent(jobEvent)
				case mvccpb.DELETE:
				}
			}
		}
	}()
}


func InitJobMgr()(err error){
	var (
		config clientv3.Config
		client *clientv3.Client
		kv clientv3.KV
		lease clientv3.Lease
		watcher clientv3.Watcher

	)
	// 初始化配置
	config = clientv3.Config{
		Endpoints: G_config.EtcdEndpoints,
		DialTimeout: time.Duration(G_config.EtcdDialTimeout) * time.Millisecond,
	}

	// 建立连接
	if client,err = clientv3.New(config);err != nil{
		fmt.Println(err)
	}

	kv = clientv3.NewKV(client)
	lease = clientv3.NewLease(client)
	watcher = clientv3.NewWatcher(client)

	G_jobMgr = &JobMgr{
		client: client,
		kv:kv,
		lease: lease,
		watcher: watcher,
	}
	//启动监听
	G_jobMgr.watchJobs()

	// 启动监听killer
	G_jobMgr.watchKiller()
	return
}

// 创建任务执行锁：
func (jobMgr *JobMgr)CreateJobLock(jobName string)(jobLock *JobLock){
	// 返回一把锁
	jobLock = InitJobLock(jobName,jobMgr.kv,jobMgr.lease)
	return
}





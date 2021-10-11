package master

import (
	"context"
	"encoding/json"
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
}


var (
	//单例
	G_jobMgr *JobMgr
)


func InitJobMgr()(err error){
	var (
		config clientv3.Config
		client *clientv3.Client
		kv clientv3.KV
		lease clientv3.Lease
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

	G_jobMgr = &JobMgr{
		client: client,
		kv:kv,
		lease: lease,
	}
	return
}

// 保存任务，apiserver解析之后提交任务到etcd中,如果是更新任务，那么可以返回原来旧的任务
func (jobMgr *JobMgr)SaveJob(job *common.Job)(oldJob *common.Job,err error){

	var(
		jobKey string
		jobValue []byte
		putResp *clientv3.PutResponse
		oldJobObj common.Job
	)

	//把任务保存到/cron/jobs/ 任务名 -->json
	jobKey = common.JOB_SAVE_DIR + job.Name
	if jobValue,err = json.Marshal(*job);err !=nil{
		return
	}

	// 保存到etcd
	if putResp,err = jobMgr.kv.Put(context.TODO(),jobKey,string(jobValue),clientv3.WithPrevKV());err !=nil{
		return
	}
	// 如果是更新，那么返回旧的值
	if putResp.PrevKv != nil{
		// 对旧值反序列化
		if err = json.Unmarshal(putResp.PrevKv.Value,&oldJobObj);err!=nil{
			err = nil
			return
		}
	}
	oldJob = &oldJobObj
	return
}
// 删除任务
func (jobMgr *JobMgr)DeleteJob(name string)(oldJob * common.Job,err error){
	var(
		jobkey string
		oldJobObj common.Job
		delResp *clientv3.DeleteResponse

	)
	jobkey= common.JOB_SAVE_DIR+name
	if delResp, err = jobMgr.kv.Delete(context.TODO(), jobkey,clientv3.WithPrevKV());err !=nil{
		return
	}
	//返回被删除的信息
	if len(delResp.PrevKvs) != 0{
		if err = json.Unmarshal(delResp.PrevKvs[0].Value,&oldJobObj);err !=nil{
			// 代表忽略旧值带来的错误信息
			err =nil
			return
		}
		oldJob = &oldJobObj
	}
	return
}

//列举任务
func (JobMgr *JobMgr)ListJobs()(jobList[]*common.Job,err error){

	var(
		dirKey string
		getResp *clientv3.GetResponse
		kvPair *mvccpb.KeyValue
		job *common.Job
	)
	// 任务保存目录
	dirKey = common.JOB_SAVE_DIR
	// 获取目录下所有的任务信息
	if getResp, err = JobMgr.kv.Get(context.TODO(), dirKey, clientv3.WithPrefix());err !=nil{
		return
	}

	// 初始化数组空间,减少调用者复杂性判断
	jobList = make([]*common.Job,0)


	// 遍历所有定时任务反序列化
	for _,kvPair = range getResp.Kvs{
		// 申请一个job存放遍历的job
		job = &common.Job{}
		if err = json.Unmarshal(kvPair.Value,job);err !=nil{
			err = nil
			continue
		}
		jobList = append(jobList,job)
	}
	return
}

// 杀死任务
func (jobMgr *JobMgr)KillJob(name string)(err error){

	//目标：更新一下key的/cron/killer/任务名，然后所有的worker都会收到监听，然后杀任务
	var(
		killerKey string
		leaseGrantResp *clientv3.LeaseGrantResponse
		leaseId clientv3.LeaseID
	)
	//通知work杀死任务
	killerKey = common.JOB_KILLER_DIR + name

	// 更新一下etcd，让worker监听到put操作，创建一个租约让其稍后自动过期即可
	if leaseGrantResp ,err = jobMgr.lease.Grant(context.TODO(),1);err !=nil{
		return
	}
	// 租约id
	leaseId = leaseGrantResp.ID

	// 设置killerKey标记
	if _,err = jobMgr.kv.Put(context.TODO(),killerKey,"",clientv3.WithLease(leaseId));err !=nil{
		return
	}
	return
}




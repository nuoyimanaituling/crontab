package worker

import (
	"fmt"
	"gocrontab/xzw/crontab/common"
	"time"
)



var(
	G_scheduler *Scheduler
)
// 任务调度
type Scheduler struct {
	jobEventChan chan *common.JobEvent // etcd 任务事件队列
	jobPlanTable map[string]*common.JobSchedulePlan // 每一个任务都有一个任务计划表，维护了cron表达式与下次执行时间
	jobExecutingTable map[string]*common.JobExecuteInfo// 任务执行表
	jobResultChan chan *common.JobExecuteResult // 任务结果队列

}

// 处理任务事件，定义功能实时维护scheduler中的任务列表，当前内存中有哪些任务，实时的做一些内存中的数据同步，和etcd中保存一致
func( scheduler *Scheduler)handleJobEvent(jobEvent *common.JobEvent){
	var(
		jobSchedulePlan *common.JobSchedulePlan
		jobExisted bool
		jobExecuteInfo *common.JobExecuteInfo
		jobExecuting bool
		err error
	)
	switch jobEvent.EventType {
	case common.JOB_EVENT_SAVE: // 保存任务事件
	  if jobSchedulePlan,err = common.BuildJobSchedulePlan(jobEvent.Job);err!=nil{
		  return
	  }
	  scheduler.jobPlanTable[jobEvent.Job.Name] = jobSchedulePlan
	case common.JOB_EVENT_DELETE:
		// map返回的第二个值是是否存在
		if jobSchedulePlan,jobExisted = scheduler.jobPlanTable[jobEvent.Job.Name];jobExisted{
			delete(scheduler.jobPlanTable,jobEvent.Job.Name)
		}
	case common.JOB_EVENT_KILL:// 强杀任务事件
		// 取消掉command,判断任务是否在执行中
		if jobExecuteInfo,jobExecuting = scheduler.jobExecutingTable[jobEvent.Job.Name];jobExecuting{
			jobExecuteInfo.CancelFunc() // 触发command杀死shell进程，任务得到退出
		}
	}
}

// 尝试执行任务
func (scheduler *Scheduler)TryStartJob(jobPlan *common.JobSchedulePlan){
	var(
		jobExecuteInfo *common.JobExecuteInfo
		jobExecuting bool
	)
	// 调度和执行是2件事情

	// 执行的任务可能运行很久，可能有一个任务，没2s调度一次，1分钟会调度60次，但是有只能执行一次，所以需要一个状态记录任务的状态
	//如果任务正在执行，跳过本次调度
	if jobExecuteInfo,jobExecuting = scheduler.jobExecutingTable[jobPlan.Job.Name];jobExecuting{
		return
	}
	// 构建执行状态信息
	jobExecuteInfo = common.BuildJobExecuteInfo(jobPlan)

	// 保存执行状态
	scheduler.jobExecutingTable[jobPlan.Job.Name] = jobExecuteInfo
	// 执行任务
	fmt.Println("执行任务:",jobExecuteInfo.Job.Name,jobExecuteInfo.PlanTime,jobExecuteInfo.RealTime)
	G_executor.ExecuteJob(jobExecuteInfo)

}


// 重新计算任务调度状态
func (scheduler *Scheduler)TrySchedule()(scheduleAfter time.Duration){
	var(
		jobPlan *common.JobSchedulePlan
		now time.Time
		nearTime *time.Time
	)

	// 如果任务表为空的话那么随便睡眠多久
	if len(scheduler.jobPlanTable) == 0{
		scheduleAfter = 1*time.Second
		return
	}
	// 记录当前时间
	now =time.Now()
	// 1:遍历所有任务
	for _,jobPlan = range scheduler.jobPlanTable{
		if jobPlan.NextTime.Before(now) || jobPlan.NextTime.Equal(now){
			// TODO:尝试执行任务 ,有可能上次任务没有执行完，此时需要再次计算
			scheduler.TryStartJob(jobPlan)
			jobPlan.NextTime = jobPlan.Expr.Next(now)
		}
		// 统计最近一个要过期的任务时间
		if nearTime == nil || jobPlan.NextTime.Before(*nearTime){
				nearTime = &jobPlan.NextTime
		}

	}

	// 3：统计最近的要过期的任务的时间（N秒后过期 == scheduleAfter），计算下次调度间隔
	scheduleAfter = (*nearTime).Sub(now)
	return
}

// 处理任务结果
func (scheduler *Scheduler)handleJobResult(result *common.JobExecuteResult){

	var (
		jobLog *common.JobLog
	)
	// 删除任务执行状态
	delete(scheduler.jobExecutingTable,result.ExecuteInfo.Job.Name)

	// 生成执行日志
	if result.Err != common.ERR_LOCK_ALREADY_REQUIRED {
		jobLog = &common.JobLog{
			JobName:      result.ExecuteInfo.Job.Name,
			Command:      result.ExecuteInfo.Job.Command,
			Output:       string(result.Output),
			// 精确到s
			PlanTime:     result.ExecuteInfo.PlanTime.UnixNano() / 1000 / 1000,
			ScheduleTime: result.ExecuteInfo.RealTime.UnixNano() / 1000 / 1000,
			StartTime:    result.StartTime.UnixNano() / 1000 / 1000,
			EndTime:      result.EndTime.UnixNano() / 1000 / 1000,
		}
		if result.Err !=nil{
			jobLog.Err = result.Err.Error()
		}else {
			jobLog.Err =""
		}
		//TODO:存储到mongodb中，另外开启一个协程处理
		G_logSink.Append(jobLog)
	}
	fmt.Println("任务执行完成:",result.ExecuteInfo.Job.Name,string(result.Output),result.Err)


}


//调度协程
func (scheduler *Scheduler)scheduleLoop(){
	var (
		jobEvent *common.JobEvent
		scheduleAfter time.Duration
		scheduleTimer  *time.Timer
		jobResult *common.JobExecuteResult
	)

	// 初始化一次
	scheduleAfter = scheduler.TrySchedule()
	scheduleTimer = time.NewTimer(scheduleAfter)
	for{
		// select语法：满足一次case就退出select执行:所以设计下面这一段逻辑就是handleJobEvent会对jobplantable涉及改变，此时需要
		// 重新计算一下调度计划，或者计时器到时了，此时也需要重新计算一下调度计划
		select {
			case jobEvent = <- scheduler.jobEventChan: // 监听任务事件变化
			// 对内存中维护的任务列表做增删改查
			scheduler.handleJobEvent(jobEvent)
			case <- scheduleTimer.C:// 最近的任务到期了
			case jobResult = <- scheduler.jobResultChan: // 监听任务执行结果
			scheduler.handleJobResult(jobResult)

		}
		// 调度一次任务
		scheduleAfter = scheduler.TrySchedule()
		// 重置调度间隔
		scheduleTimer.Reset(scheduleAfter)

	}
}

// 推送任务事件变化
func(scheduler *Scheduler)PushJobEvent(jobEvent *common.JobEvent){
	scheduler.jobEventChan <- jobEvent
}

// 初始化调度器
func InitScheduler()(err error){

	G_scheduler = &Scheduler{
		jobEventChan: make(chan *common.JobEvent,1000),
		jobPlanTable: make(map[string]*common.JobSchedulePlan),
		jobExecutingTable: make(map[string]*common.JobExecuteInfo),
		jobResultChan: make(chan *common.JobExecuteResult,1000),

	}
	// 启动调度协程
	go G_scheduler.scheduleLoop()
	return
}

// 回传任务执行结果
func (scheduler *Scheduler)PushJobResult(jobResult *common.JobExecuteResult){

	scheduler.jobResultChan <- jobResult
}
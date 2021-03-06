package worker

import (
	"gocrontab/xzw/crontab/common"
	"math/rand"
	"os/exec"
	"time"
)

// 任务执行器

type Executor struct {

}

var(
	G_executor *Executor
)

//执行一个任务
func (executor *Executor)ExecuteJob(info *common.JobExecuteInfo){

	go func() {
		var(
			cmd *exec.Cmd
			err error
			output []byte
			result *common.JobExecuteResult
			jobLock *JobLock
		)
		result = &common.JobExecuteResult{
			ExecuteInfo: info,
			Output: make([]byte,0),
		}
		// 首先获取分布式锁
		// 1 创建对于key的锁
		jobLock = G_jobMgr.CreateJobLock(info.Job.Name)

		result.StartTime = time.Now()

		// 上锁
		// 随机睡眠0-1s
		time.Sleep(time.Duration(rand.Intn(1000))*time.Millisecond)
		err = jobLock.TryLock()
		// 释放锁：
		defer jobLock.UnLock()
		if err !=nil{
			result.Err =err
			result.EndTime = time.Now()
		} else {
			// 上锁成功后，重置任务启动时间
			result.StartTime =time.Now()
			// 执行shell命令
			cmd = exec.CommandContext(info.CancelContext, "c:\\cygwin64\\bin\\bash", "-c", info.Job.Command)
			// 执行并捕获输出
			output, err = cmd.CombinedOutput()

			// 记录任务结束时间
			result.EndTime = time.Now()
			result.Output = output
			result.Err = err
		}
		// 任务执行完成后，把执行的结果返回给Scheduler，Scheduler会从executingTable中删除掉执行记录
		G_scheduler.PushJobResult(result)

	}()

}


func InitExecutor()(err error){

	G_executor = &Executor{
	}
	return
}
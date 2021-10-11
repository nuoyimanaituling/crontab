package worker

import (
	"context"
	"fmt"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"gocrontab/xzw/crontab/common"
	"time"
)

//mongodb 存储日志
type LogSink struct {
	client *mongo.Client
	logCollection *mongo.Collection
	logChan chan *common.JobLog
	autoCommitChan chan *common.LogBatch
}
var(
	G_logSink *LogSink
)


// 批量写入日志
func (logSink *LogSink) saveLogs(batch *common.LogBatch) {
	_, err := logSink.logCollection.InsertMany(context.TODO(), batch.Logs)
	if err != nil {
		fmt.Println(err.Error())
	}
}

func (logSink *LogSink)writeLoop(){

	var(
		log *common.JobLog
		logBatch *common.LogBatch
		commitTimer *time.Timer
		timeOutBatch *common.LogBatch // 获取超时批次

	)
	for{
		// ps：8-12
		// learn go laungage 闭包
		select {
			case log =  <- logSink.logChan:
				// 把log写入到mongodb中
				if logBatch==nil {
					logBatch = &common.LogBatch{}
					// 在一个批次内设置一个定时器,超时自动提交
					commitTimer = time.AfterFunc(time.Duration(G_config.JobLogCommitTimeOut)*time.Millisecond,
						func(batch *common.LogBatch) func(){
							return func() {
								// 发出通知，而不是直接提交batch(因为loop协程与调用func函数的协程涉及到对batch的并发访问，所以体现了通过通信共享内存，而不是通过共享内存进行通信)
								logSink.autoCommitChan <- logBatch
							}
						}(logBatch),
					)
				}
				// 把新日志追加到日志中
				logBatch.Logs = append(logBatch.Logs,log)
				// 如果批次满了,那么就立即发送日志
				if len(logBatch.Logs) >= G_config.JobLogBatchSize{
					// 发送日志
					logSink.saveLogs(logBatch)
					// 清空logBatch
					logBatch = nil
					// 取消定时器
					commitTimer.Stop()
				}
			case timeOutBatch = <-logSink.autoCommitChan:// 过期的批次
				// 判断过期批次是否仍旧是当前批次
				// 此种情况是解决一个bug，恰好在1s的时间，过期，logbatch为100条，无法阻止定时器触发，此时日志以后被提交了
				// 所以判断timeOutBatch与logBatch是否一致，一致说明是过期批次先触发，否则就是timeOutBatch为nil，已经清空，两者不一致
				if timeOutBatch != logBatch{
					continue
				}
				logSink.saveLogs(timeOutBatch)
				// 设置当前批次为kong
				logBatch = nil
		}
	}
}


func InitLogSink()(err error){
	var(
		client *mongo.Client
	)
	// 建立mongodb连接
	fmt.Println(G_config.MongodbUri)
	if client, err = mongo.Connect(context.TODO(), &options.ClientOptions{Hosts: []string{G_config.MongodbUri}});err != nil {
		return
	}
	// 选择db和collection
	G_logSink = &LogSink{
		client: client,
		logCollection: client.Database("cron").Collection("log"),
		logChan: make(chan *common.JobLog, 1000),
		autoCommitChan :make(chan *common.LogBatch,1000),
	}

	// 启动一个mongodb处理协程，消费队列
	go G_logSink.writeLoop()
	return
}

// 发送日志接口
func (logSink *LogSink)Append(jobLog *common.JobLog){
	select {
	case logSink.logChan <- jobLog:
	default:
		// 队列满了就丢弃
	}
}
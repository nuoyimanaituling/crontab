package master

import (
	"context"
	"fmt"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"gocrontab/xzw/crontab/common"
)

type LogMgr struct {
	client *mongo.Client
	logCollection *mongo.Collection
}

var(
	G_logMgr *LogMgr
)






func InitLogMgr()(err error){
	var(
		client *mongo.Client
	)
	// 建立mongodb连接
	fmt.Println(G_config.MongodbUri)
	if client, err = mongo.Connect(context.TODO(), &options.ClientOptions{Hosts: []string{G_config.MongodbUri}});err != nil {
		return
	}
	G_logMgr = &LogMgr{
		client: client,
		logCollection: client.Database("cron").Collection("log"),
	}
	return
}

// 查看任务日志
func (logMgr *LogMgr)ListLog(name string,skip int,limit int)(logArr []*common.JobLog,err error){
	var(
		filter *common.JobLogFilter
		logSort *common.SortLogByStartTime
		cursor *mongo.Cursor
		jobLog *common.JobLog
	)

	logArr = make([]*common.JobLog,0)


	filter = &common.JobLogFilter{JobName: name}

	// 按照任务执行时间倒排
	logSort =&common.SortLogByStartTime{SortOrder: -1}

	if cursor,err = logMgr.logCollection.Find(context.TODO(),filter,options.Find().SetSort(logSort).SetSkip(int64(skip)).SetLimit(int64(limit)));err !=nil{
		return
	}
	defer cursor.Close(context.TODO())

	for cursor.Next(context.TODO()){
		jobLog =  &common.JobLog{}
		// 反序列化bson到joblog
		if err = cursor.Decode(jobLog);err !=nil{
			continue // 日志不合法
		}
		logArr = append(logArr,jobLog)
	}

	return

}
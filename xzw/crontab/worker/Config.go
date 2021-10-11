package worker

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
)

// 单例
var(
	G_config *Config
)


//程序配置
type Config struct {
	MongodbUri string `json:"mongodbUri"`
	MongodbConnectTimeout int `json:"mongodbConnectTimeout"`
	EtcdEndpoints  []string `json:"etcdEndpoints"`
	EtcdDialTimeout int `json:"etcdDialTimeout"`
	JobLogBatchSize int `json:"jobLogBatchSize"`
	JobLogCommitTimeOut int `json:"jobLogCommitTimeOut"`
}

func InitConfig(filename string)(err error){

	var(
		content []byte
		conf Config
	)


	// 把配置文件读进来
	if content, err = ioutil.ReadFile(filename); err !=nil{
		return
	}

	// 做json反序列化
	if err = json.Unmarshal(content, &conf);err !=nil{
		return
	}

	// 赋值单例
	G_config = &conf

	fmt.Println(conf)
	return
}
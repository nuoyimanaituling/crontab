package master

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
	ApiPort int `json:"apiPort"`
	ApiReadTimeout int `json:"apiReadTimeout"`
	ApiWriteTimeout int `json:"apiWriteTimeout"`
	EtcdEndpoints  []string `json:"etcdEndpoints"`
	EtcdDialTimeout int `json:"etcdDialTimeout"`
	WebRoot string `json:"webroot"`
	MongodbUri string `json:"mongodbUri"`
	MongodbConnectTimeout int `json:"mongodbConnectTimeout"`
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
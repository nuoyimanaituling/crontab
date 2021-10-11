package master

import (
	"encoding/json"
	"gocrontab/xzw/crontab/common"
	"net"
	"net/http"
	"strconv"
	"time"
)

// 任务的http接口

type ApiServer struct {
	httpServer *http.Server
}

// 保存任务接口
// Postjob={"name":job1,"command":"echo hello","cronExpr":"* * * * *"}
func handleJobSave(resp http.ResponseWriter, req *http.Request){
	// 将任务保存到etcd中
	var(
		err error
		postJob string
		job common.Job
		oldJob *common.Job
		bytes []byte
	)
	// 解析post表单
	if err = req.ParseForm();err !=nil{
		goto ERR
	}
	// 2:去表单中job字段的值
	postJob = req.PostForm.Get("job")
	// 3 反序列化job
	if err = json.Unmarshal([]byte(postJob), &job);err !=nil{
		goto ERR
	}
	// 保存到etcd中
	if oldJob,err = G_jobMgr.SaveJob(&job);err!=nil{
			goto ERR
	}
	// 返回正常应答,返回json串，{"errno":0,"msg":""."data":{...}}
	if bytes, err = common.BuildResponse(0, "success", oldJob);err ==nil{
		resp.Write(bytes)
	}
	return
	ERR:
		// 返回错误应答
		if bytes, err = common.BuildResponse(-1, err.Error(),nil);err ==nil{
			resp.Write(bytes)
		}

}

// 查询日志接口
func handleJobLog(resp http.ResponseWriter, req *http.Request){

	var(
		err error
		name string // 任务名字
		skipParam string  // 翻页参数
		limitParam string // 页数限制
		skip int
		limit int
		logArr []*common.JobLog
		bytes []byte
	)
	// 解析get请求
	if err = req.ParseForm();err !=nil {
		goto ERR
	}
	// 请求参数格式 /job/log?name=job10&skip=0&limit=10
	name = req.Form.Get("name")
	skipParam = req.Form.Get("skip")
	limitParam = req.Form.Get("limit")
	if skip, err = strconv.Atoi(skipParam);err !=nil{
		// 相当于传输错误数据
		skip = 0
	}
	if limit,err= strconv.Atoi(limitParam);err !=nil{
		limit =20
	}
	if logArr,err = G_logMgr.ListLog(name,skip,limit);err !=nil{
		goto ERR
	}
	// 返回正常应答,返回json串，{"errno":0,"msg":""."data":{...}}
	if bytes, err = common.BuildResponse(0, "success", logArr);err ==nil{
		resp.Write(bytes)
	}
	return
ERR:
	// 返回错误应答
	if bytes, err = common.BuildResponse(-1, err.Error(),nil);err ==nil{
		resp.Write(bytes)
	}

}


//删除任务接口，Post调用 /POST/job/delete name=job1
func handleJobDelete(resp http.ResponseWriter, req *http.Request){
	var(

		name string
		err error
		oldJob *common.Job
		bytes []byte
	)
	//解析表单，做的是字符串的切割 a=1 & b=2 & c=3
	if err = req.ParseForm(); err !=nil{
		goto ERR
	}
	// 删除的任务名
	name = req.PostForm.Get("name")
	// 去etcd中删除任务
	if oldJob,err = G_jobMgr.DeleteJob(name);err !=nil{
		goto ERR
	}
	if bytes,err = common.BuildResponse(0,"success",oldJob);err == nil{
		resp.Write(bytes)
	}
	return
	ERR:
		if bytes,err = common.BuildResponse(-1,err.Error(),nil);err ==nil{
			resp.Write(bytes)
		}
}


// 列举所有crontab任务（没有实现翻页 ）
func handleJobList(resp http.ResponseWriter, req *http.Request){
	var(
		jobList []*common.Job
		err error
		bytes []byte
	)


	if jobList, err = G_jobMgr.ListJobs();err !=nil{
		goto ERR
	}
	// 返回正常应答：
	if bytes,err = common.BuildResponse(0,"success",jobList);err == nil{
		resp.Write(bytes)
	}
	return
	ERR:
		if bytes,err = common.BuildResponse(-1,err.Error(),nil);err ==nil{
			resp.Write(bytes)
		}
}

// 强制杀死某个任务
// POST /JOB/kill name=job1
func handleJobKill(resp http.ResponseWriter, req *http.Request){
	var(
		err error
		name string
		bytes []byte
	)
	// 解析表单
	if err = req.ParseForm();err !=nil{
		goto ERR
	}

	//要杀死的任务名
	name = req.PostForm.Get("name")

	// 杀死任务
	if err = G_jobMgr.KillJob(name);err !=nil{
		goto ERR
	}

	// 返回正常应答：
	if bytes,err = common.BuildResponse(0,"success",nil);err == nil{
		resp.Write(bytes)
	}
	return
ERR:
	if bytes,err = common.BuildResponse(-1,err.Error(),nil);err ==nil{
		resp.Write(bytes)
	}
}

// 获取健康worker节点列表的接口
func handleWorkerList(resp http.ResponseWriter, req *http.Request){

	var(
		workerArr []string
		err error
		bytes []byte
	)
	if workerArr,err = G_workerMgr.ListWorkers();err !=nil{
		goto ERR
	}

	// 返回正常应答：
	if bytes,err = common.BuildResponse(0,"success",workerArr);err == nil{
		resp.Write(bytes)
	}
	return
ERR:
	if bytes,err = common.BuildResponse(-1,err.Error(),nil);err ==nil{
		resp.Write(bytes)
	}


}

//配置单例
var(
	G_ApiServer *ApiServer
)

//初始化服务
func InitApiServer()(err error){
	var(

		mux *http.ServeMux
		listener net.Listener
		httpServer *http.Server
		staticDir http.Dir // 静态文件根目录
		staticHandler http.Handler // 静态文件http回调
	)
	//配置路由
	mux = http.NewServeMux()
	mux.HandleFunc("/job/save",handleJobSave)
	mux.HandleFunc("/job/delete",handleJobDelete)
	mux.HandleFunc("/job/list",handleJobList)
	mux.HandleFunc("/job/kill",handleJobKill)
	mux.HandleFunc("/job/log",handleJobLog)
	mux.HandleFunc("/worker/list",handleWorkerList)


	// 静态文件目录
	staticDir = http.Dir(G_config.WebRoot)
	staticHandler = http.FileServer(staticDir)
	mux.Handle("/",http.StripPrefix("/",staticHandler))


	// 启动tcp监听
	if listener,err = net.Listen("tcp",":"+ strconv.Itoa(G_config.ApiPort)); err != nil {
		return
	}

	// 创建一个http服务
	httpServer = &http.Server{
		ReadTimeout: time.Duration(G_config.ApiReadTimeout)*time.Millisecond,
		WriteTimeout: time.Duration(G_config.ApiWriteTimeout)*time.Millisecond,
		Handler: mux,// 传递路由

	}

	// 赋值单例
	G_ApiServer = &ApiServer{
		httpServer: httpServer,
	}
	// 启动服务端
	go httpServer.Serve(listener)
	return
}

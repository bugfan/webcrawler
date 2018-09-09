package scheduler

import (
	"net/http"
	"webcrawler/anlz"
)

type GenHttpClient func() *http.Client

type Scheduler interface {
	//
	Start(channelLen uint,
		poolSize uint32,
		crawlDepth uint32,
		httpClientGenerator GenHttpClient,
		respParses []anlz.ParseResponse,
		itemProcessors []ipl.ProcessItem,
		firstHttpRsp *http.Request) (err error)
	Stop() bool
	Running() bool
	ErrorChan() <-chan error
	Idle() bool
	Summary(prefix string) SchedSummary
}
type SchedSummary interface {
	String() string
	Detail() string
	Same(other SchedSummary) bool //比较摘要信息
}

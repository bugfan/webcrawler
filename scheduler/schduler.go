package scheduler

import (
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync/atomic"
	anlz "webcrawler/analyzer"
	"webcrawler/base"
	dl "webcrawler/downloader"
	ipl "webcrawler/itempipeline"
	mdw "webcrawler/middleware"

	"github.com/bugfan/logrus"
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

func NewScheduler() Scheduler {
	return &myScheduler{}
}

type myScheduler struct {
	poolSize      uint32
	channelLen    uint
	crawlDepth    uint32
	primaryDomain string
	chanman       mdw.ChannelManager
	stopSign      mdw.StopSign
	dlpool        dl.PageDownloaderPool
	analyzerPool  anlz.AnalyzerPool
	itemPipeLine  ipl.ItemPipeline
	running       uint32 //运行 bool值
	// 辅助
	// reqCache requestCache
	urlMap map[string]bool
}

func (s *myScheduler) Start(channelLen uint, poolSize uint32, crawlDepth uint32, httpClientGenerator GenHttpClient, respParses []anlz.ParseResponse,
	itemProcessors []ipl.ProcessItem,
	firstHttpReq *http.Request) (err error) {
	defer func() {
		if r := recover(); r != nil {
			errMsg := fmt.Sprintf("Schduler Error is :%s\n", r)
			logrus.Fatal(errMsg)
			err = errors.New(errMsg)
		}
	}()
	if atomic.LoadUint32(&s.running) == 1 {
		return errors.New("The scheduler is started!")
	}
	atomic.StoreUint32(&s.running, 1)
	if s.channelLen == 0 {
		return errors.New(fmt.Sprintf("The channel max length (cap) can not be 0!\n"))
	}
	s.channelLen = channelLen
	if poolSize == 0 {
		return errors.New(fmt.Sprintf("The pool size can not be 0!\n"))
	}
	s.poolSize = poolSize
	s.crawlDepth = crawlDepth
	s.chanman = generateChannelManager(s.channelLen)
	if httpClientGenerator == nil {
		return errors.New(fmt.Sprintf("The http generate list is invalid!"))
	}
	dlpool, err := generatePageDownloaderPool(s.poolSize, httpClientGenerator)
	if err != nil {
		return errors.New(fmt.Sprintf("Occur error when gen page downloader pool :%s\n", err))
	}
	s.dlpool = dlpool
	analyzerPool, err := generateAnalyzerPool(s.poolSize)
	if err != nil {
		if err != nil {
			return errors.New(fmt.Sprintf("Occur error when gen analyzer pool :%s\n", err))
		}
	}
	s.analyzerPool = analyzerPool
	if itemProcessors == nil {
		return errors.New(fmt.Sprintf("The item processor list is invalid!"))
	}
	for i, ip := range itemProcessors {
		if ip == nil {
			return errors.New(fmt.Sprintf("The item %d processor list is invalid!", i))
		}
	}
	s.itemPipeLine = generateItemPipeLine(itemProcessors)
	if s.stopSign == nil {
		s.stopSign = mdw.NewStopSign()
	} else {
		s.stopSign.Reset()
	}
	s.urlMap = make(map[string]bool)

	s.startDownloading()
	s.activateAnalyzers(respParses)
	// s.openItemPipeLine()
	// s.schedule(10 * time.Millisecond)

	if firstHttpReq == nil {
		return errors.New(fmt.Sprintf("The firstHttpReq is invalid!"))
	}
	pd, err := getPrimaryDomain(firstHttpReq.Host)
	if err != nil {
		return err
	}
	s.primaryDomain = pd
	fristReq := base.NewRequest(firstHttpReq, 0)
	s.reqCache.put(fristReq)
	return nil
}
func (s *myScheduler) activateAnalyzers(respParses []anlz.ParseResponse) {
	go func() {
		for {
			resp, ok := <-s.getRespChan()
			if !ok {
				break
			}
			go s.analyze(respParses, resp)
		}
	}()
}
func (s *myScheduler) analyze(respParses []anlz.ParseResponse, resp base.Response) {
	defer func() {
		if r := recover(); r != nil {
			logrus.Fatal("Fatal analyze error :", r)
		}
	}()
	anlyzer, err := s.analyzerPool.Take()
	if err != nil {
		s.sendError(err, SCHEDULER_CODE)
		return
	}
	defer func() {
		err := s.analyzerPool.Return(anlyzer)
		if err != nil {
			s.sendError(err, SCHEDULER_CODE)
		}
	}()
	code := generateCode(ANALYZER_CODE, anlyzer.Id())
	dataList, errs := anlyzer.Analyze(respParses, resp)
	if dataList != nil {
		for _, data := range dataList {
			if data == nil {
				continue
			}
			switch d := data.(type) {
			case *base.Request:
				s.saveReqToCache(*d, code)
			case *base.Item:
				s.sendItem(*d, code)
			default:
				s.sendError(errors.New(fmt.Sprintf("Unsopported data type %T value=%v\n", d, d)), code)
			}
		}
	}
}

// method step 1
func (s *myScheduler) startDownloading() {
	go func() {
		for {
			req, ok := <-s.getReqChan()
			if !ok {
				break
			}
			go s.download(req)
		}
	}()
}
func (s *myScheduler) getReqChan() chan base.Request {
	reqChan, err := s.chanman.ReqChan()
	if err != nil {
		panic(err)
	}
	return reqChan
}
func (s *myScheduler) getRespChan() chan base.Response {
	respChan, err := s.chanman.RespChan()
	if err != nil {
		panic(err)
	}
	return respChan
}
func (s *myScheduler) getErrorChan() chan error {
	errChan, err := s.chanman.ErrorChan()
	if err != nil {
		panic(err)
	}
	return errChan
}
func (s *myScheduler) getItemChan() chan base.Item {
	itemChan, err := s.chanman.ItemChan()
	if err != nil {
		panic(err)
	}
	return itemChan
}
func (s *myScheduler) download(req base.Request) {
	defer func() {
		if r := recover(); r != nil {
			logrus.Fatal("Fatal download error :", r)
		}
	}()
	downloader, err := s.dlpool.Take()
	if err != nil {
		s.sendError(err, SCHEDULER_CODE)
		return
	}
	defer func() {
		err := s.dlpool.Return(downloader)
		if err != nil {
			s.sendError(err, SCHEDULER_CODE)
		}
	}()
	code := generateCode(DOWNLOADER_CODE, downloader.Id())
	resp, err := downloader.Download(&req)
	if resp != nil {
		s.sendResp(*resp, code)
	}
	if err != nil {
		s.sendError(err, code)
	}
}
func (s *myScheduler) sendError(err error, code string) bool {
	if err != nil {
		return false
	}
	codePrefix := parseCode(code)[0]
	var errorType base.ErrorType
	switch codePrefix {
	case DOWNLOADER_CODE:
		errorType = base.DOWNLOAD_ERROR
	case ANALYZER_CODE:
		errorType = base.ANALYZER_ERROR
	case ITEMPIPELINE_CODE:
		errorType = base.ITEM_PROCESSOR_ERROR
	}
	cError := base.NewCrawlerError(errorType, err.Error())
	if s.stopSign.Signed() {
		s.stopSign.Deal(code)
		return false
	}
	go func() {
		s.getErrorChan() <- cError
	}()

	return true
}
func (s *myScheduler) sendResp(resp base.Response, code string) bool {
	if s.stopSign.Signed() {
		s.stopSign.Deal(code)
		return false
	}
	s.getRespChan() <- resp
	return true
}

const (
	DOWNLOADER_CODE   = "downloader"
	SCHEDULER_CODE    = "scheduler"
	ANALYZER_CODE     = "analyzer"
	ITEMPIPELINE_CODE = "itempipeline"
)

// 辅助方法
func generateCode(code string, id uint32) string {
	//生成唯一的标识
	return fmt.Sprintf("%s:%s", code, string(id))
}
func parseCode(code string) []string {
	tokens := strings.Split(code, ":")
	if len(tokens) > 1 {
		return tokens
	}
	return []string{"NONE", "0"}
}
func generateChannelManager(l uint) mdw.ChannelManager {
	return mdw.NewChannelManager(l)
}
func generatePageDownloaderPool(l uint32, hcg GenHttpClient) (dl.PageDownloaderPool, error) {
	return dl.NewPageDownloaderPool(l, nil)
}
func generateAnalyzerPool(l uint32) (anlz.AnalyzerPool, error) {
	return anlz.NewAnalyzerPool(l, nil)
}
func generateItemPipeLine(itemProcessors []ipl.ProcessItem) ipl.ItemPipeline {
	return ipl.NewItemPipeline(itemProcessors)
}
func getPrimaryDomain(host string) (string, error) {
	tmp := ""
	tokens := strings.Split(host, ".")
	l := len(tokens)
	if l < 2 {
		return "", errors.New("not a domain!")
	}
	if tokens[l-1] == "cn" {
		tmp = tokens[l-3] + "." + tokens[l-1]
	}
	tmp = tokens[l-2] + "." + tokens[l-1]
	return tmp, nil
}

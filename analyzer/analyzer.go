package analyzer

import (
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"reflect"
	"webcrawler/base"
	mdw "webcrawler/middleware"

	"github.com/bugfan/logrus"
)

type ParseResponse func(httpResp *http.Response, respDepth uint32) ([]base.Data, []error)

type Analyzer interface {
	Id() uint32
	Analyze(respParses []ParseResponse, resp base.Response) ([]base.Data, []error)
}
type myAnalyzer struct {
	id uint32
}

func (s *myAnalyzer) Id() uint32 {
	return 0
}
func (s *myAnalyzer) Analyze(respParses []ParseResponse, resp base.Response) ([]base.Data, []error) {
	if respParses == nil {
		return nil, []error{errors.New("The response paeser list is invalid!")}
	}
	httpResp := resp.HttpResp()
	if httpResp == nil {
		return nil, []error{errors.New("The http resp is invalid!")}
	}
	var reqUrl *url.URL = httpResp.Request.URL
	logrus.Infof("Parse the response (reqUrl=%s) \n", reqUrl)
	respDeth := resp.Depth()
	dataList := make([]base.Data, 0)
	errorList := make([]error, 0)
	for i, respParser := range respParses {
		if respParser == nil {
			errorList = append(errorList, errors.New(fmt.Sprintf("The document parser [%d] id valid!", i)))
			continue
		}
		pDataList, pErrorList := respParser(httpResp, respDeth)
		if pDataList != nil {
			for _, pData := range pDataList {
				dataList = appendDataList(dataList, pData, respDeth)
			}
		}
		if pErrorList != nil {
			for _, pError := range pErrorList {
				errorList = appendErrorList(errorList, pError)
			}
		}

	}
	return dataList, errorList
}
func appendDataList(dataList []base.Data, data base.Data, respDeth uint32) []base.Data {
	if data == nil {
		return dataList
	}
	req, ok := data.(*base.Request)
	if !ok {
		return append(dataList, data)
	}
	newDeth := respDeth + 1
	if req.Depth() != newDeth {
		req = base.NewRequest(req.HttpReq(), newDeth)
	}
	return append(dataList, req)
}
func appendErrorList(errorList []error, err error) []error {
	if err == nil {
		return errorList
	}
	return append(errorList, err)
}
func NewAnalyzer() Analyzer {
	return &myAnalyzer{}
}

// 开始写 AnalyzerPool
type AnalyzerPool interface {
	Take() (Analyzer, error)
	Return(analyzer Analyzer) error
	Total() uint32
	Used() uint32
}
type myAnalyzerPool struct {
	pool  mdw.Pool     //实体池
	etype reflect.Type //池内实体的类型
}

func (s *myAnalyzerPool) Take() (Analyzer, error) {
	entity, err := s.pool.Take()
	if err != nil {
		return nil, err
	}
	a, ok := entity.(Analyzer)
	if !ok {
		panic(errors.New(fmt.Sprintf("The type of entity is NOT %s\n", s.etype)))
	}
	return a, nil
}
func (s *myAnalyzerPool) Return(a Analyzer) error {
	return s.pool.Return(a)
}
func (s *myAnalyzerPool) Total() uint32 {
	return s.pool.Total()
}
func (s *myAnalyzerPool) Used() uint32 {
	return s.pool.Used()
}

type GenAnalyzer func() Analyzer

func NewAnalyzerPool(total uint32, gen GenAnalyzer) (AnalyzerPool, error) {
	etype := reflect.TypeOf(gen())
	genEntity := func() mdw.Entity {
		return gen()
	}
	pool, err := mdw.NewPool(total, etype, genEntity)
	if err != nil {
		return nil, err
	}
	apool := myAnalyzerPool{
		pool:  pool,
		etype: etype,
	}
	return &apool, nil
}

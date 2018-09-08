package base

import (
	"bytes"
	"errors"
	"fmt"
	"net/http"
	"reflect"
	"sync"
)

// request
type Request struct {
	httpReq *http.Request
	depth   uint32
}

func NewRequest(httpreq *http.Request, depth uint32) *Request {
	return &Request{httpReq: httpreq, depth: depth}
}
func (s *Request) HttpReq() *http.Request {
	return s.httpReq
}
func (s *Request) Depth() uint32 {
	return s.depth
}
func (s *Request) Valid() bool {
	return s.httpReq != nil && s.httpReq.URL != nil
}

type Response struct {
	httpResp *http.Response
	depth    uint32
}

// response
func NewResponse(httpResp *http.Response, depth uint32) *Response {
	return &Response{httpResp: httpResp, depth: depth}
}
func (s *Response) HttpReq() *http.Response {
	return s.httpResp
}
func (s *Response) Depth() uint32 {
	return s.depth
}
func (s *Response) Valid() bool {
	return s.httpResp != nil && s.httpResp.Body != nil
}

// item
type Item map[string]interface{}

func (s Item) Valid() bool {
	return s != nil
}

//
type Data interface {
	Valid() bool
}

// error
type ErrorType string

const (
	DOWNLOAD_ERROR       ErrorType = "Downloader Error"
	ANALYZER_ERROR       ErrorType = "Analyzer Error"
	ITEM_PROCESSOR_ERROR ErrorType = "Item Processor Error"
)

type CrawlerError interface {
	Type() ErrorType
	Error() string
}
type myCrawlerError struct {
	errType    ErrorType
	errMsg     string
	fullErrMsg string
}

func (s *myCrawlerError) Type() ErrorType {
	return s.errType
}
func (s *myCrawlerError) Error() string {
	if s.fullErrMsg == "" {
		s.genFullErrMsg()
	}
	return s.fullErrMsg
}
func (s *myCrawlerError) genFullErrMsg() {
	var buf bytes.Buffer
	buf.WriteString("Crawler Error:")
	if s.errType != "" {
		buf.WriteString(string(s.errType))
		buf.WriteString(": ")
	}
	buf.WriteString(s.errMsg)
	s.fullErrMsg = fmt.Sprintf("%s\n", buf.String())
	return
}
func NewCrawlerError(errType ErrorType, errMsg string) CrawlerError {
	return &myCrawlerError{errMsg: errMsg, errType: errType}
}

// pool
type Pool interface {
	Take() (Entity, error)
	Return(analyzer Entity) error
	Total() uint32
	Used() uint32
}
type Entity interface {
	Id() uint32
}

// signal
type StopSign interface {
	Sign() bool
	Signed() bool
	Reset()
	Deal(code string)
	DealCount(code string) uint32
	DealTotal() uint32
	Summary() string
}

// 实体池 实现类型
type myPool struct {
	total       uint32
	etype       reflect.Type
	genEntity   func() Entity
	container   chan Entity
	idContainer map[uint32]bool
	m           sync.Mutex
}

func (s *myPool) Take() (Entity, error) {
	e, ok := <-s.container
	if !ok {
		return nil, errors.New("The inner container is invalid!")
	}
	s.idContainer[e.Id()] = false
	return e, nil
}
func (s *myPool) Total() uint32 {
	return s.total
}
func (s *myPool) Used() uint32 {
	return uint32(len(s.container))
}
func (s *myPool) Return(e Entity) error {
	if e == nil {
		return errors.New("The return entity is nil")
	}
	if s.etype != reflect.TypeOf(e) {
		return errors.New("Type is not match!")
	}
	eid := e.Id()
	caseResult := s.optidContianer(eid, false, true)
	if caseResult == 1 {
		s.container <- e
		return nil
	} else if caseResult == 0 {
		return errors.New("Entity  is already in the pool!")
	} else {
		return errors.New("Entity id is illegal!!")
	}
}
func (s *myPool) optidContianer(eid uint32, oldValue bool, newValue bool) int8 {
	s.m.Lock()
	defer s.m.Unlock()
	v, ok := s.idContainer[eid]
	if !ok {
		return -1
	}
	if v != oldValue {
		return 0
	}
	s.idContainer[eid] = newValue
	return 1
}

// 创建一个实体pool
func NewPool(total uint32, entityType reflect.Type, genEntity func() Entity) (Pool, error) {
	if total < 1 {
		return nil, errors.New(fmt.Sprintf("Pool Total can not be initialized by %d\n", total))
	}
	size := int(total)
	container := make(chan Entity, size)
	idContainer := make(map[uint32]bool)
	for i := 0; i < size; i++ {
		newEntity := genEntity()
		if entityType != reflect.TypeOf(newEntity) {
			return nil, errors.New(fmt.Sprintf("The Type of given is not real type -> %v\n", entityType))
		}
		container <- newEntity
		idContainer[newEntity.Id()] = true
	}
	pool := myPool{
		total:       total,
		etype:       entityType,
		genEntity:   genEntity,
		container:   container,
		idContainer: idContainer,
	}
	return &pool, nil
}

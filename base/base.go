package base

import (
	"bytes"
	"fmt"
	"net/http"
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

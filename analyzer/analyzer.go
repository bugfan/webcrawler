package anlz

import (
	"goreptile/base"
	"net/http"
)

type ParseResponse func(httpResp *http.Response, respDepth uint32) ([]base.Data, []error)
type Analyzer interface {
	Id() uint32
	Analyze(respParses []ParseResponse, resp base.Response) ([]base.Data, []error)
}

type AnalyzerPool interface {
	Take() (Analyzer, error)
	Return(analyzer Analyzer) error
	Total() uint32
	Used() uint32
}

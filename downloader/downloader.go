package downloader

import (
	"errors"
	"fmt"
	"net/http"
	"reflect"
	"webcrawler/base"
	mdw "webcrawler/middleware"
)

var downloaderIdGenertor mdw.IdGenertor = mdw.NewIdGenertor()

func genDownloaderId() uint32 {
	return downloaderIdGenertor.GetUint32()
}

// downloader
type PageDownloader interface {
	Id() uint32
	Download(req *base.Request) (*base.Response, error)
}
type myPageDownloader struct {
	id         uint32
	httpClient http.Client
}

func (s *myPageDownloader) Id() uint32 {
	return 0
}

func (s *myPageDownloader) Download(req *base.Request) (*base.Response, error) {
	res, err := s.httpClient.Do(req.HttpReq())
	if err != nil {
		return nil, err
	}
	return base.NewResponse(res, req.Depth()), nil
}
func NewPageDownloader(client *http.Client) PageDownloader {
	id := genDownloaderId()
	if client == nil {
		client = &http.Client{}
	}
	return &myPageDownloader{
		id:         id,
		httpClient: *client,
	}
}

// downloader pool
type PageDownloaderPool interface {
	Take() (PageDownloader, error)
	Return(dl PageDownloader) error
	Total() uint32
	Used() uint32
}

type myPageDownloaderPool struct {
	pool  mdw.Pool     //实体池
	etype reflect.Type //池内实体的类型
}

func (s *myPageDownloaderPool) Take() (PageDownloader, error) {
	entity, err := s.pool.Take()
	if err != nil {
		return nil, err
	}
	dl, ok := entity.(PageDownloader)
	if !ok {
		panic(errors.New(fmt.Sprintf("The type of entity is NOT %s\n", s.etype)))
	}
	return dl, nil
}
func (s *myPageDownloaderPool) Return(dl PageDownloader) error {
	return s.pool.Return(dl)
}
func (s *myPageDownloaderPool) Total() uint32 {
	return s.pool.Total()
}
func (s *myPageDownloaderPool) Used() uint32 {
	return s.pool.Used()
}

type GenPageDownloader func() PageDownloader

func NewPageDownloaderPool(total uint32, gen GenPageDownloader) (PageDownloaderPool, error) {
	etype := reflect.TypeOf(gen())
	genEntity := func() mdw.Entity {
		return gen()
	}
	pool, err := mdw.NewPool(total, etype, genEntity)
	if err != nil {
		return nil, err
	}
	pdpool := myPageDownloaderPool{
		pool:  pool,
		etype: etype,
	}
	return &pdpool, nil
}

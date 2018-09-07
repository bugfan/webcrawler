package downloader

import "goreptile/base"

// downloader
type PageDownloader interface {
	Id() uint32
	Download(req *base.Request) (*base.Response, error)
}

// id creator
type IdGenertor interface {
	GetUint32() uint32
}

// downloader pool
type PageDownloaderPool interface {
	Take() (PageDownloader, error)
	Return(dl PageDownloader) error
	Total() uint32
	Used() uint32
}

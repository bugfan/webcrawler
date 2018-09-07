package ipl

import (
	"goreptile/base"
)

type Itempipeline interface {
	Send(item base.Item) []error
	FailFast() bool
	SetFailFast(failFast bool)
	Count() []uint64
	ProcessingNumber() uint64
	Summary() string
}

type ProcessItem func(item base.Item) (result base.Item, err error)

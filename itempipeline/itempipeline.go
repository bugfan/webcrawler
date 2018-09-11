package itempipeline

import (
	"errors"
	"fmt"
	"sync/atomic"
	"webcrawler/base"
)

type ItemPipeline interface {
	Send(item base.Item) []error
	FailFast() bool
	SetFailFast(failFast bool)
	Count() []uint64
	ProcessingNumber() uint64
	Summary() string
}

type ProcessItem func(item base.Item) (result base.Item, err error)

func NewItemPipeline(itemProcessors []ProcessItem) ItemPipeline {
	if itemProcessors == nil {
		panic(errors.New("Invalid item processor list!"))
	}
	innerItemProcessors := make([]ProcessItem, 0)
	for i, ip := range itemProcessors {
		if ip == nil {
			panic(errors.New(fmt.Sprintf("Invalid item processor[%d]!\n", i)))
		}
		innerItemProcessors = append(innerItemProcessors, ip)
	}
	return &myItemPipeline{itemProcessors: innerItemProcessors}
}

type myItemPipeline struct {
	itemProcessors   []ProcessItem // 条目处理器列表
	failFast         bool
	sent             uint64
	accepted         uint64
	processed        uint64
	processingNumber uint64
}

func (s *myItemPipeline) Send(item base.Item) []error {
	// 原子操作
	atomic.AddUint64(&s.processingNumber, 1)
	defer atomic.AddUint64(&s.processingNumber, ^uint64(0))
	atomic.AddUint64(&s.sent, 1)

	errs := make([]error, 0)
	if item == nil {
		return append(errs, errors.New("The item is nil"))
	}
	atomic.AddUint64(&s.accepted, 1)
	var currentItem base.Item = item
	for _, itemProcessor := range s.itemProcessors {
		processItem, err := itemProcessor(currentItem)
		if err != nil {
			errs = append(errs, err)
			if s.failFast {
				break
			}
		}
		if processItem != nil {
			currentItem = processItem
		}
		atomic.AddUint64(&s.processed, 1)
	}
	return nil
}
func (s *myItemPipeline) FailFast() bool {
	return true
}
func (s *myItemPipeline) SetFailFast(failFast bool) {

	return
}
func (s *myItemPipeline) Count() (counts []uint64) {
	counts[0] = atomic.LoadUint64(&s.sent)
	counts[1] = atomic.LoadUint64(&s.accepted)
	counts[2] = atomic.LoadUint64(&s.processed)
	return
}
func (s *myItemPipeline) ProcessingNumber() uint64 {
	return atomic.LoadUint64(&s.processed)
}
func (s *myItemPipeline) Summary() string {
	count := s.Count()
	return fmt.Sprintf("failFast :%v,processorsNumber:%d,sent:%d,acceped:%d,processed:%d,processingNumber:%d ",
		s.failFast, len(s.itemProcessors), count[0], count[1], count[2], s.ProcessingNumber())
}

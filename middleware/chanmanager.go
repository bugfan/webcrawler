package middleware

import (
	"errors"
	"fmt"
	"goreptile/base"
	"sync"
)

// 通道管理器
type ChannelManager interface {
	Init(chanLen uint, reset bool) bool
	Close() bool
	ReqChan() (chan base.Request, error)
	RespChan() (chan base.Response, error)
	ItemChan() (chan base.Item, error)
	ErrorChan() (chan error, error)
	ChannelLen() uint
	Status() ChannelManagerStatus
	Summary() string
}
type ChannelManagerStatus uint8

var statusNameMap = map[ChannelManagerStatus]string{
	CHANNEL_MANAGER_STATUS_CLOSED:        "uninitialized",
	CHANNEL_MANAGER_STATUS_INITIALIZED:   "initialized",
	CHANNEL_MANAGER_STATUS_UNINITIALIZED: "closed",
}

const (
	CHANNEL_MANAGER_STATUS_UNINITIALIZED ChannelManagerStatus = 0 //未初始化
	CHANNEL_MANAGER_STATUS_INITIALIZED   ChannelManagerStatus = 1 //初始化
	CHANNEL_MANAGER_STATUS_CLOSED        ChannelManagerStatus = 2 //关闭
	defaultChanLen                                            = 50
)

type myChannelManager struct {
	channelLen uint
	reqCh      chan base.Request
	respCh     chan base.Response
	itemCh     chan base.Item
	errorCh    chan error
	status     ChannelManagerStatus //通道管理器的状态
	m          sync.RWMutex
}

func (s *myChannelManager) Init(channelLen uint, reset bool) bool {
	if channelLen <= 0 {
		panic(errors.New("The Channel Length is invalid"))
	}
	s.m.Lock()
	defer s.m.Unlock()
	if s.status == CHANNEL_MANAGER_STATUS_INITIALIZED && !reset {
		return false
	}
	s.channelLen = channelLen
	s.reqCh = make(chan base.Request, channelLen)
	s.respCh = make(chan base.Response, channelLen)
	s.itemCh = make(chan base.Item, channelLen)
	s.errorCh = make(chan error, channelLen)
	s.status = CHANNEL_MANAGER_STATUS_INITIALIZED
	return true
}
func (s *myChannelManager) Close() bool {
	s.m.Lock()
	defer s.m.Unlock()
	if s.status != CHANNEL_MANAGER_STATUS_INITIALIZED {
		return false
	}
	close(s.reqCh)
	close(s.respCh)
	close(s.itemCh)
	close(s.errorCh)
	s.status = CHANNEL_MANAGER_STATUS_CLOSED
	return true
}
func (s *myChannelManager) checkStatus() error {
	if s.status == CHANNEL_MANAGER_STATUS_INITIALIZED {
		return nil
	}
	statusName, ok := statusNameMap[s.status]
	if !ok {
		statusName = fmt.Sprintf("%d", s.status)
	}
	return errors.New(fmt.Sprintf("The undescribe status of channel manager:%s!\n", statusName))
}
func (s *myChannelManager) ReqChan() (chan base.Request, error) {
	s.m.RLock()
	defer s.m.RUnlock()
	if err := s.checkStatus(); err != nil {
		return nil, err
	}
	return s.reqCh, nil
}
func (s *myChannelManager) RespChan() (chan base.Response, error) {
	s.m.RLock()
	defer s.m.RUnlock()
	if err := s.checkStatus(); err != nil {
		return nil, err
	}
	return s.respCh, nil
}
func (s *myChannelManager) ItemChan() (chan base.Item, error) {
	s.m.RLock()
	defer s.m.RUnlock()
	if err := s.checkStatus(); err != nil {
		return nil, err
	}
	return s.itemCh, nil
}
func (s *myChannelManager) ErrorChan() (chan error, error) {
	s.m.RLock()
	defer s.m.RUnlock()
	if err := s.checkStatus(); err != nil {
		return nil, err
	}
	return s.errorCh, nil
}
func (s *myChannelManager) ChannelLen() uint {
	s.m.RLock()
	defer s.m.RUnlock()
	return s.channelLen
}
func (s *myChannelManager) Status() ChannelManagerStatus {
	s.m.RLock()
	defer s.m.RUnlock()
	return s.status
}
func (s *myChannelManager) Summary() string {
	s.m.RLock()
	defer s.m.RUnlock()
	return fmt.Sprintf("status:%s", "reqCh:%d/%d", "respCh:%d/%d", "itemCh:%d/%d", "errorCh:%s/%s",
		s.status, len(s.reqCh), cap(s.reqCh), len(s.respCh), cap(s.respCh), len(s.itemCh), cap(s.itemCh), len(s.errorCh), cap(s.errorCh))
}
func NewChannelManager(channelLen uint) ChannelManager {
	if channelLen <= 0 {
		channelLen = defaultChanLen
	}
	chanman := &myChannelManager{}
	chanman.Init(channelLen, true)
	return chanman
}

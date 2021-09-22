package message

import (
    "fmt"
    "sync"
    "container/list"
)

type MessageQueue struct {
    sync.Mutex
    msgs *list.List
}

func NewMessageQueue() *MessageQueue {
    return &MessageQueue{
        msgs: list.New(),
    }
}

func (mq *MessageQueue) Push(msg *SQSMessage) {
    mq.Lock()
    defer mq.Unlock()

    mq.msgs.PushBack(msg)
}

func (mq *MessageQueue) Pop() (*SQSMessage, error) {
    mq.Lock()
    defer mq.Unlock()

    if mq.msgs.Len() == 0 {
        return nil, fmt.Errorf("Pop Error: Queue is empty")
    }

    msg := mq.msgs.Front()
    mq.msgs.Remove(msg)
    return msg.Value.(*SQSMessage), nil
}

func (mq *MessageQueue) PopMany(amount int) []*SQSMessage {
    mq.Lock()
    defer mq.Unlock()

    result := []*SQSMessage{}
    if mq.msgs.Len() > 0 {
        for msg := mq.msgs.Front(); len(result) < amount && msg != nil; msg = mq.msgs.Front() {
            result = append(result, msg.Value.(*SQSMessage))
            mq.msgs.Remove(msg)
        }
    }
    return result
}

func (mq *MessageQueue) Len() int {
    mq.Lock()
    defer mq.Unlock()

    return mq.msgs.Len()
}

func (mq *MessageQueue) Print() {
    for elem := mq.msgs.Front(); elem != nil; elem = elem.Next() {
        fmt.Printf("%+v\n", elem.Value)
    }
}

func (mq *MessageQueue) PrintFirst() {
    elem := mq.msgs.Front()
    if elem != nil {
        fmt.Printf("first element: %+v\n", elem.Value)
    }
}

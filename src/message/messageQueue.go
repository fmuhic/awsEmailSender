package message

import (
    "math"
    "sync"
)

type MessageQueue struct {
    sync.Mutex
    queue []interface{}
    capacity int
    minCapacity int
    size int
    tail int
}

func NewMessageQueue(initCapacity int) *MessageQueue {
    if initCapacity < 1 {
        initCapacity = 1
    }

    return &MessageQueue{
        queue: make([]interface{}, initCapacity),
        capacity: initCapacity,
        minCapacity: initCapacity,
        size: 0,
        tail: 0,
    }
}

func (mq *MessageQueue) Push(msg interface{}) {
    mq.Lock()
    defer mq.Unlock()

    if mq.size >= mq.capacity {
        mq.grow()
    }

    mq.queue[(mq.tail + mq.size) % mq.capacity] = msg
    mq.size++
}

func (mq *MessageQueue) Pop() interface{} {
    mq.Lock()
    defer mq.Unlock()

    if mq.size == 0 {
        return nil
    }

    msg := mq.queue[mq.tail]
    mq.tail = (mq.tail + 1) % mq.capacity
    mq.size--

    if mq.size * 2 < mq.capacity {
        mq.shrink()
    }

    return msg
}

func (mq *MessageQueue) PopMany(amount int) []interface{} {
    mq.Lock()
    defer mq.Unlock()

    if amount > mq.size {
        amount = mq.size
    }
    msgs := mq.copyElements(mq.tail, amount, amount)
    mq.tail = (mq.tail + amount) % mq.capacity
    mq.size -= amount

    if mq.size * 2 < mq.capacity {
        mq.shrink()
    }

    return msgs
}

func (mq *MessageQueue) Len() int {
    mq.Lock()
    defer mq.Unlock()

    return mq.size
}

func (mq *MessageQueue) copyElements(start, amount, newCapacity int) []interface{} {
    newBuf := make([]interface{}, newCapacity)
    if start + amount < mq.capacity {
        copy(newBuf, mq.queue[mq.tail : mq.tail + amount])
    } else {
        copyAmount := copy(newBuf, mq.queue[mq.tail:])
        copy(newBuf[copyAmount:], mq.queue[:amount - copyAmount])
    }

    return newBuf
}

func (mq *MessageQueue) grow() {
    mq.queue = mq.copyElements(mq.tail, mq.size, mq.capacity * 2)
    mq.tail = 0
    mq.capacity *= 2
}

func (mq *MessageQueue) shrink() {
    newCapacity := int(math.Ceil(float64(mq.capacity) / 2.0))
    if newCapacity < mq.minCapacity {
        return
    }

    mq.queue = mq.copyElements(mq.tail, mq.size, newCapacity)
    mq.tail = 0
    mq.capacity = newCapacity
}

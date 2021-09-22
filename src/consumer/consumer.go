package consumer

import (
    "fmt"
    "time"
    "encoding/json"

    "github.com/aws/aws-sdk-go/aws"
    "github.com/aws/aws-sdk-go/service/sqs"

    "github.com/fmuhic/emailSender/src/message"
    "github.com/fmuhic/emailSender/src/storage"
    "github.com/fmuhic/emailSender/src/config"
)

type Consumer struct {
    sqsClient *sqs.SQS
    messages *message.MessageQueue
    storage *storage.CloudStorage
    config config.Consumer
}

func NewConsumer(sqsClient *sqs.SQS,
                 messageQueue *message.MessageQueue,
                 storage *storage.CloudStorage,
                 config config.Consumer) *Consumer {
    return &Consumer{
        sqsClient: sqsClient,
        messages: messageQueue,
        storage: storage,
        config: config,
    }
}

func (c *Consumer) Run() {
    msgChan := make(chan *sqs.Message)
    for workerId := 0; workerId <= c.config.WorkerCount; workerId++ {
        go c.consume(workerId, msgChan)
    }

    for {
        if c.messages.Len() > c.config.MaxMsgQueueSize {
            fmt.Println("Queue full, Sleeping ...")
			time.Sleep(time.Second)
            continue
        }

		msgRequest := &sqs.ReceiveMessageInput{
			MaxNumberOfMessages: aws.Int64(c.config.QueueMaxMsgsPerRequest),
			QueueUrl: aws.String(c.config.QueueUrl),
			VisibilityTimeout: aws.Int64(c.config.MsgVisibilitySec),
			WaitTimeSeconds: aws.Int64(c.config.LongPollingDurationSec),
		}
		msgResponse, _ := c.sqsClient.ReceiveMessage(msgRequest)

		for _, msg := range msgResponse.Messages {
            msgChan <- msg
		}

		if len(msgResponse.Messages) < 1 {
			fmt.Println("No messages on queue, Idling ...")
			time.Sleep(time.Second)
		}
    }
}

func (c *Consumer) consume(workerId int, msgChan <-chan *sqs.Message) {
    for sqsMsg := range msgChan {
        fmt.Printf("Worker %v is processing message\n", workerId)
        c.processMessage(sqsMsg)
    }
}

func (c *Consumer) processMessage(sqsMsg *sqs.Message) {
    var msg message.SQSMessage
    decodeErr := json.Unmarshal([]byte(*sqsMsg.Body), &msg)
    if decodeErr != nil {
        fmt.Println("Error while decoding msg ", decodeErr.Error())
        // return
    }

    content, fetchErr := c.storage.Read(msg.S3BucketName, msg.TemplateName)
    if fetchErr != nil {
        fmt.Println("Error while fetching template ", fetchErr.Error())
        // return
    }

    msg.Template = &content
    c.messages.Push(&msg)

	deleteRequest := &sqs.DeleteMessageInput{
        QueueUrl: aws.String(c.config.QueueUrl),
		ReceiptHandle: sqsMsg.ReceiptHandle,
	}

	_, err := c.sqsClient.DeleteMessage(deleteRequest)
    if err != nil {
        fmt.Println("Error while deleting message ", err)
    }
}

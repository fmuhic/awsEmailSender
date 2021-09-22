package consumer

import (
    "time"
    "encoding/json"

    "github.com/aws/aws-sdk-go/aws"
    "github.com/aws/aws-sdk-go/service/sqs"

    "github.com/fmuhic/emailSender/src/message"
    "github.com/fmuhic/emailSender/src/logging"
    "github.com/fmuhic/emailSender/src/storage"
    "github.com/fmuhic/emailSender/src/config"
)

type Consumer struct {
    sqsClient *sqs.SQS
    messages *message.MessageQueue
    storage *storage.CloudStorage
    config config.Consumer
	logger logging.Logger
}

func NewConsumer(sqsClient *sqs.SQS,
                 messageQueue *message.MessageQueue,
                 storage *storage.CloudStorage,
                 config config.Consumer,
                 logger logging.Logger) *Consumer {
    return &Consumer{
        sqsClient: sqsClient,
        messages: messageQueue,
        storage: storage,
        config: config,
		logger: logger,
    }
}

func (c *Consumer) Run() {
    c.logger.Debug("Starting consumer")

    msgChan := make(chan *sqs.Message)
    for workerId := 0; workerId <= c.config.WorkerCount; workerId++ {
        go c.consume(workerId, msgChan)
    }

    for {
        if c.messages.Len() > c.config.MaxMsgQueueSize {
            c.logger.Debug("Queue full")
			time.Sleep(time.Second)
            continue
        }

		msgRequest := &sqs.ReceiveMessageInput{
			MaxNumberOfMessages: aws.Int64(c.config.QueueMaxMsgsPerRequest),
			QueueUrl: aws.String(c.config.QueueUrl),
			VisibilityTimeout: aws.Int64(c.config.MsgVisibilitySec),
			WaitTimeSeconds: aws.Int64(c.config.LongPollingDurationSec),
		}
		msgResponse, responseErr := c.sqsClient.ReceiveMessage(msgRequest)

        if responseErr != nil {
            c.logger.Error("Msg response error: %v", responseErr)
        }

		for _, msg := range msgResponse.Messages {
            msgChan <- msg
		}

		if len(msgResponse.Messages) < 1 {
			c.logger.Debug("No messages in queue")
			time.Sleep(time.Second)
		}
    }
}

func (c *Consumer) consume(workerId int, msgChan <-chan *sqs.Message) {
    for sqsMsg := range msgChan {
        c.logger.Debug("Worker %v is processing message", workerId)
        c.processMessage(sqsMsg)
    }
}

func (c *Consumer) processMessage(sqsMsg *sqs.Message) {
    var msg message.SQSMessage
    decodeErr := json.Unmarshal([]byte(*sqsMsg.Body), &msg)
    if decodeErr != nil {
        c.logger.Error("Error while decoding SQS msg: " + decodeErr.Error())
        return
    }

    content, fetchErr := c.storage.Read(msg.S3BucketName, msg.TemplateName)
    if fetchErr != nil {
        c.logger.Error("Error while fetching template from S3: " + fetchErr.Error())
        return
    }

    msg.Template = &content
    c.messages.Push(&msg)

	deleteRequest := &sqs.DeleteMessageInput{
        QueueUrl: aws.String(c.config.QueueUrl),
		ReceiptHandle: sqsMsg.ReceiptHandle,
	}

	_, err := c.sqsClient.DeleteMessage(deleteRequest)
    if err != nil {
        c.logger.Error("Error while deleting SQS message: %v", err)
    }
}

package storage

import (
    "github.com/aws/aws-sdk-go/aws"
    "github.com/aws/aws-sdk-go/service/s3"
    "github.com/aws/aws-sdk-go/service/s3/s3manager"
)

type CloudStorage struct {
    s3Downloader *s3manager.Downloader
}

func NewCloudStorage(dm *s3manager.Downloader) *CloudStorage {
    return &CloudStorage{
        s3Downloader: dm,
    }
}

func (cs *CloudStorage) Read(bucketName, fileName string) (string, error) {
    buff := &aws.WriteAtBuffer{}
    _, err := cs.s3Downloader.Download(buff,
        &s3.GetObjectInput{
            Bucket: aws.String(bucketName),
            Key: aws.String(fileName),
        },
    )

    if err != nil {
        return "", err
    }

    return string(buff.Bytes()), nil
}

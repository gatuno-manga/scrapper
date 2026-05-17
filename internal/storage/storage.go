package storage

import (
	"bytes"
	"context"
	"fmt"
	"log"
	"sync"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

type S3Client struct {
	client         *minio.Client
	checkedBuckets sync.Map
}

func NewS3Client(endpoint, accessKey, secretKey, region string, useSSL bool) (*S3Client, error) {
	minioClient, err := minio.New(endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(accessKey, secretKey, ""),
		Secure: useSSL,
		Region: region,
	})
	if err != nil {
		return nil, err
	}
	return &S3Client{client: minioClient}, nil
}

func (s *S3Client) EnsureBucket(ctx context.Context, bucketName string) error {
	if _, ok := s.checkedBuckets.Load(bucketName); ok {
		return nil
	}

	exists, err := s.client.BucketExists(ctx, bucketName)
	if err != nil {
		return fmt.Errorf("failed to check bucket existence: %w", err)
	}

	if !exists {
		err = s.client.MakeBucket(ctx, bucketName, minio.MakeBucketOptions{})
		if err != nil {
			return fmt.Errorf("failed to create bucket: %w", err)
		}
	}

	s.checkedBuckets.Store(bucketName, true)
	return nil
}

func (s *S3Client) Upload(ctx context.Context, bucketName, objectName string, data []byte, contentType string) (string, error) {
	if err := s.EnsureBucket(ctx, bucketName); err != nil {
		return "", err
	}

	reader := bytes.NewReader(data)
	_, err := s.client.PutObject(ctx, bucketName, objectName, reader, int64(len(data)), minio.PutObjectOptions{
		ContentType: contentType,
	})
	if err != nil {
		return "", fmt.Errorf("failed to upload object: %w", err)
	}

	log.Printf("Successfully uploaded to S3: bucket=%s, object=%s", bucketName, objectName)
	return fmt.Sprintf("%s/%s", bucketName, objectName), nil
}

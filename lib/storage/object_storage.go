package storage

import (
	"bytes"
	"context"
	"fmt"
	"github.com/aliyun/alibabacloud-oss-go-sdk-v2/oss"
	"github.com/aliyun/alibabacloud-oss-go-sdk-v2/oss/credentials"
	"io"
	"log"
)

var client *oss.Client
var config OssConfig

func InitOss(c OssConfig) {
	config = c
	cfg := oss.LoadDefaultConfig().
		WithCredentialsProvider(credentials.NewEnvironmentVariableCredentialsProvider()).
		WithRegion(config.Region)
	client = oss.NewClient(cfg)
}

func ossReadData(objectName string, offset int64, size int64) []byte {

	request := &oss.GetObjectRequest{
		Bucket:        oss.Ptr(config.BucketName), // 存储空间名称
		Key:           oss.Ptr(objectName),        // 对象名称
		Range:         oss.Ptr(fmt.Sprintf("bytes=%d-%d", offset, offset+size-1)),
		RangeBehavior: oss.Ptr("standard"),
	}

	result, err := client.GetObject(context.TODO(), request)
	if err != nil {
		log.Printf("GetObject err: %v", err)
		return nil
	}
	defer result.Body.Close()
	data, err := io.ReadAll(result.Body)
	if err != nil {
		log.Fatalf("failed to read object %v", err)
	}
	return data
}

func CompleteMultipartUpload(uploadId string, objectName string, parts []oss.UploadPart) error {
	// 完成分片上传请求
	request := &oss.CompleteMultipartUploadRequest{
		Bucket:   oss.Ptr(config.BucketName),
		Key:      oss.Ptr(objectName),
		UploadId: oss.Ptr(uploadId),
		CompleteMultipartUpload: &oss.CompleteMultipartUpload{
			Parts: parts,
		},
	}
	result, err := client.CompleteMultipartUpload(context.TODO(), request)
	if err != nil {
		log.Fatalf("failed to complete multipart upload %v", err)
		return err
	}

	// 打印完成分片上传的结果
	log.Printf("complete multipart upload result:%#v, object_name = %s", result, objectName)
	return nil
}

func InitiateMultipartUpload(objectName string) (string, error) {
	// 初始化分片上传请求
	initRequest := &oss.InitiateMultipartUploadRequest{
		Bucket: oss.Ptr(config.BucketName),
		Key:    oss.Ptr(objectName),
	}
	initResult, err := client.InitiateMultipartUpload(context.TODO(), initRequest)
	if err != nil {
		log.Fatalf("failed to initiate multipart upload %v", err)
		return "", fmt.Errorf("failed to initiate multipart upload %v", err)
	}
	// 打印初始化分片上传的结果
	log.Printf("initiate multipart upload result:%#v\n", *initResult.UploadId)
	return *initResult.UploadId, nil
}

func ossWriteData(objectName string, uploadId string, partNumber int32, data []byte) (oss.UploadPart, error) {
	// 创建分片上传请求
	partRequest := &oss.UploadPartRequest{
		Bucket:     oss.Ptr(config.BucketName), // 目标存储空间名称
		Key:        oss.Ptr(objectName),        // 目标对象名称
		PartNumber: partNumber,                 // 分片编号
		UploadId:   oss.Ptr(uploadId),          // 上传ID
		Body:       bytes.NewReader(data),      // 分片内容
	}

	// 发送分片上传请求
	partResult, err := client.UploadPart(context.TODO(), partRequest)
	if err != nil {
		log.Printf("failed to upload part %v", err)
		return oss.UploadPart{}, fmt.Errorf("failed to upload part %d: %v", partNumber, err)
	}

	// 记录分片上传结果
	part := oss.UploadPart{
		PartNumber: partRequest.PartNumber,
		ETag:       partResult.ETag,
	}

	return part, nil
	//
	//request := &oss.PutObjectRequest{
	//	Bucket: oss.Ptr(GetOssConfig().BucketName),                    // 存储空间名称
	//	Key:    oss.Ptr(shardingKey + "/" + mergeIdx + "-values.bin"), // 对象名称
	//	Body:   body,                                                  // 要上传的字符串内容
	//}
	//
	//client.PutObject(context.TODO(), request)
}

package main

import (
	"context"
	"fmt"
	"log"
	"os"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"gopkg.in/yaml.v3"
)

type Locations struct {
	Source string `yaml:"source"`
	Dest   string `yaml:"dest"`
}

type ConfigFile struct {
	Locations []Locations `yaml:"locations"`
}

func (c *ConfigFile) Read() ([]Locations, error) {
	yamlFile, err := os.ReadFile("config.yaml")
	if err != nil {
		return []Locations{}, fmt.Errorf("error: %v", err)
	}

	err = yaml.Unmarshal(yamlFile, c)
	if err != nil {
		return []Locations{}, fmt.Errorf("error: %v", err)
	}

	return c.Locations, nil
}

type Syncronizer struct {
	S3        *s3.Client
	locations []Locations
}

func (s *Syncronizer) Init() {
	locations, err := s.loadLocationsConfig()
	if err != nil {
		log.Fatalf("error: %v", err)
	}

	s.locations = locations

	err = s.loadS3()
	if err != nil {
		log.Fatalf("error: %v", err)
	}
}

func (s *Syncronizer) loadLocationsConfig() ([]Locations, error) {
	configFile := ConfigFile{}
	locations, err := configFile.Read()
	if err != nil {
		log.Fatalf("error: %v", err)
	}

	return locations, nil
}

func (s *Syncronizer) loadS3() error {
	config, err := config.LoadDefaultConfig(context.TODO(), config.WithRegion("us-east-1"), config.WithSharedConfigProfile("default"))
	if err != nil {
		return fmt.Errorf("unable to load SDK config, %v", err)
	}

	s3 := s3.NewFromConfig(config)
	s.S3 = s3

	return nil
}

func (s *Syncronizer) listObjects(bucketName string) ([]string, error) {
	continuationToken := ""
	objectsList := []string{}
	objectNum := 0

	input := &s3.ListObjectsV2Input{
		Bucket: &bucketName,
	}
	if continuationToken != "" {
		input.ContinuationToken = &continuationToken
	}

	for {
		resp, err := s.S3.ListObjectsV2(context.TODO(), input)
		if err != nil {
			return nil, fmt.Errorf("unable to list objects, %v", err)
		}

		for _, obj := range resp.Contents {
			fmt.Printf("Object %d: %s\n", objectNum, *obj.Key)
			objectNum++
		}

		for _, obj := range resp.Contents {
			objectsList = append(objectsList, fmt.Sprintf("%s/%s", bucketName, *obj.Key))
		}

		if !*resp.IsTruncated {
			break
		}

		continuationToken = *resp.NextContinuationToken
	}

	return objectsList, nil
}

func (s *Syncronizer) migrate(objectsList []string, destinationBucket string) error {
	for _, obj := range objectsList {
		_, err := s.S3.CopyObject(context.TODO(), &s3.CopyObjectInput{
			Bucket:     &destinationBucket,
			CopySource: &obj,
			Key:        &obj,
		})
		if err != nil {
			return fmt.Errorf("unable to copy object, %v", err)
		}
	}

	return nil
}

// func (s *Syncronizer) activateLiveReplication(source, dest string) error {
// 	// activate versioning

// 	_, err := s.S3.PutBucketVersioning(context.TODO(), &s3.PutBucketVersioningInput{
// 		Bucket: &source,
// 		VersioningConfiguration: &types.VersioningConfiguration{
// 			Status: types.BucketVersioningStatusEnabled,
// 		},
// 	})
// 	if err != nil {
// 		return fmt.Errorf("unable to activate versioning, %v", err)
// 	}

// 	_, err = s.S3.PutBucketVersioning(context.TODO(), &s3.PutBucketVersioningInput{
// 		Bucket: &dest,
// 		VersioningConfiguration: &types.VersioningConfiguration{
// 			Status: types.BucketVersioningStatusEnabled,
// 		},
// 	})
// 	if err != nil {
// 		return fmt.Errorf("unable to activate versioning, %v", err)
// 	}

// 	_, err = s.S3.PutBucketReplication(context.TODO(), &s3.PutBucketReplicationInput{
// 		Bucket: &source,
// 		ReplicationConfiguration: &types.ReplicationConfiguration{
// 			Role: aws.String("arn:aws:iam::676344214603:role/atlantis-app-prod"),
// 			Rules: []types.ReplicationRule{
// 				{
// 					Status: types.ReplicationRuleStatusEnabled,
// 					Destination: &types.Destination{
// 						Bucket: aws.String(fmt.Sprintf("arn:aws:s3:::%s", dest)),
// 					},
// 					Filter:   &types.ReplicationRuleFilter{},
// 					Priority: aws.Int32(1),
// 					DeleteMarkerReplication: &types.DeleteMarkerReplication{
// 						Status: types.DeleteMarkerReplicationStatusEnabled,
// 					},
// 				},
// 			},
// 		},
// 	})
// 	if err != nil {
// 		return fmt.Errorf("unable to activate live replication, %v", err)
// 	}

// 	return nil
// }

func (s *Syncronizer) Sync() error {
	for _, location := range s.locations {
		objectsList, err := s.listObjects(location.Source)
		if err != nil {
			return fmt.Errorf("unable to list objects, %v", err)
		}

		err = s.migrate(objectsList, location.Dest)
		if err != nil {
			return fmt.Errorf("unable to migrate objects, %v", err)
		}

		// err = s.activateLiveReplication(location.Source, location.Dest)
		// if err != nil {
		// 	return fmt.Errorf("unable to activate live replication, %v", err)
		// }
	}

	return nil
}

func main() {
	s := Syncronizer{}
	s.Init()
	err := s.Sync()
	if err != nil {
		log.Fatalf("error: %v", err)
	}
}

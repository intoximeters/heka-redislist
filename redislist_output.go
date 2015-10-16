/*
Copyright 2015 Intoximeters, Inc

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package heka_redislist

import (
	"errors"
	"fmt"
	"sync/atomic"

	"github.com/mozilla-services/heka/message"
	"github.com/mozilla-services/heka/pipeline"
	"gopkg.in/redis.v3"
)

type RedisListOutput struct {
	processMessageCount    int64
	processMessageFailures int64
	encodingErrors         int64

	runner pipeline.OutputRunner
	client *redis.Client
	config *RedisListOutputConfig
}

type RedisListOutputConfig struct {
	// String representation of the address of the network connection on which
	// the listener should be listening (e.g. "127.0.0.1:6379").
	Address string `toml:"address"`
	// Redis database number
	Database int64 `toml:"database"`
	// Redis list to push to
	Key string `toml:"key"`
	// Number of threads to process items from buffer
	OutputThreads int `toml:"output_threads"`
}

func (r *RedisListOutput) ConfigStruct() interface{} {
	config := &RedisListOutputConfig{
		Address:       "localhost:6379",
		Database:      0,
		OutputThreads: 1,
	}
	return config
}

func (r *RedisListOutput) Init(config interface{}) error {
	conf := config.(*RedisListOutputConfig)
	r.config = conf

	if r.config.Key == "" {
		return errors.New("must specify a Redis `key` to pop messages from")
	}

	return nil
}

func (r *RedisListOutput) Prepare(or pipeline.OutputRunner, helper pipeline.PluginHelper) error {
	r.runner = or
	r.client = redis.NewClient(&redis.Options{
		Addr: r.config.Address,
		DB:   r.config.Database,
	})

	// TODO Determine if we actually need a bunch of flush threads and if so, make them here
	return nil
}

func (r *RedisListOutput) ProcessMessage(pack *pipeline.PipelinePack) (err error) {
	var record []byte

	if record, err = r.runner.Encode(pack); err != nil {
		atomic.AddInt64(&r.encodingErrors, 1)
		return fmt.Errorf("can't encode: %s", err)
	}

	atomic.AddInt64(&r.processMessageCount, 1)
	if err = r.client.RPush(r.config.Key, string(record[:])).Err(); err != nil {
		atomic.AddInt64(&r.processMessageFailures, 1)
		return pipeline.NewRetryMessageError("writing to %s: %s", r.config.Address, err)
	} else {
		r.runner.UpdateCursor(pack.QueueCursor)
	}

	return nil
}

func (r *RedisListOutput) CleanUp() {
	if r.client != nil {
		if err := r.client.Close(); err != nil {
			r.runner.LogError(fmt.Errorf("failure closing redis client: %v", err))
		}
		r.client = nil
	}
}

func (r *RedisListOutput) ReportMsg(msg *message.Message) error {
	message.NewInt64Field(msg, "EncodingErrors",
		atomic.LoadInt64(&r.encodingErrors), "count")
	message.NewInt64Field(msg, "ProcessMessageCount",
		atomic.LoadInt64(&r.processMessageCount), "count")
	message.NewInt64Field(msg, "ProcessMessageFailures",
		atomic.LoadInt64(&r.processMessageFailures), "count")
	return nil
}

func init() {
	pipeline.RegisterPlugin("RedisListOutput", func() interface{} {
		return new(RedisListOutput)
	})
}

// TODO Add apache license

package heka_redislist

import (
	"fmt"
	"io"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/mozilla-services/heka/pipeline"
	"gopkg.in/redis.v3"
)

const (
	// This is inspired by Logstash's Redis input plugin. Its basically the
	// same script except here we dont check the initial length of the queue
	// and instead keep iterating until either 1) we find a nil value or 2) we
	// hit the maximum number of interations. The 'logstash-input-redis'
	// project is licensed under Apache 2.0 license.

	// See https://github.com/logstash-plugins/logstash-input-redis/blob/843cb3d1fdcc6d8588793e52fc921bea7935281b/lib/logstash/inputs/redis.rb#L169-L183
	BATCH_SCRIPT = `
		local result = {}
		local upto = tonumber(ARGV[1])
		local ctr = 0

		while (ctr < upto) do
		  local val = redis.call("LPOP", KEYS[1])
		  if (not val) then break end

		  table.insert(result, val)
		  ctr = ctr + 1
		end

		return result
	`
)

type RedisListInput struct {
	wgIn           sync.WaitGroup
	stopChan       chan bool
	workerStopChan chan bool
	runner         pipeline.InputRunner
	client         *redis.Client
	config         *RedisListInputConfig
	luaSha         string
}

type RedisListInputConfig struct {
	// String representation of the address of the network connection on which
	// the listener should be listening (e.g. "127.0.0.1:6379").
	Address string `toml:"address"`
	// Redis database number
	Database int64 `toml:"database"`
	// Redis list to push to
	Key string `toml:"key"`
	// Set Hostname field from remote address
	SetHostname bool `toml:"set_hostname"`

	// Number of items to read off Redis list
	BatchSize int `toml:"batch_size"`
	// Size of buffer to store Redis items
	BufferPool int `toml:"buffer_pool"`
	// Number of threads to poll Redis
	InputThreads int `toml:"input_threads"`
}

func (r *RedisListInput) ConfigStruct() interface{} {
	config := &RedisListInputConfig{
		Address:      "localhost:6379",
		Database:     0,
		SetHostname:  false,
		BufferPool:   250,
		BatchSize:    25,
		InputThreads: 2,
	}
	return config
}

func (r *RedisListInput) Init(config interface{}) error {
	conf := config.(*RedisListInputConfig)
	r.config = conf

	r.client = redis.NewClient(&redis.Options{
		Addr: r.config.Address,
		DB:   r.config.Database,
	})

	r.stopChan = make(chan bool)
	r.workerStopChan = make(chan bool)
	return nil
}

func (r *RedisListInput) pollRedis(eventChan chan<- string, errChan chan<- error) {
	defer func() {
		r.wgIn.Done()
	}()

	ok := true
	for ok {
		val, err := r.client.EvalSha(
			r.luaSha,
			[]string{r.config.Key},
			[]string{strconv.Itoa(r.config.BatchSize)},
		).Result()

		if err != nil {
			errChan <- fmt.Errorf("error from redis client: %v", err)
			// If we get an get EOF lets just exit because we lost our
			// connection to Redis and we'll just exit the plugin
			if err == io.EOF {
				return
			}
		} else {
			result, ok := val.([]interface{})
			if ok {
				for _, line := range result {
					eventChan <- line.(string)
				}
			} else {
				// TODO This should probably cause a panic/kill the plugin
				errChan <- fmt.Errorf("error type assertion failure in redis result: %v - %v", result, val)
			}
		}

		select {
		case _, ok = <-r.workerStopChan:
			// we are shutting down
		case <-time.After(2 * time.Second):
		}
	}
	return
}

func (r *RedisListInput) Run(ir pipeline.InputRunner, helper pipeline.PluginHelper) (err error) {
	r.runner = ir
	deliverer := r.runner.NewDeliverer("")
	splitter := r.runner.NewSplitterRunner("")

	eventChan := make(chan string, r.config.BufferPool)
	errChan := make(chan error)

	defer func() {
		deliverer.Done()
		splitter.Done()

		close(eventChan)
		close(errChan)
	}()

	// Make sure Redis is alive first before attempting to load the batch
	// script. This way we can determine if Redis is unavailable OR if its
	// available but doesnt support Lua scripting
	if _, err = r.client.Ping().Result(); err != nil {
		r.cleanup()
		return pipeline.NewPluginExitError("error issuing redis ping: %v", err)
	}

	sha, err := r.client.ScriptLoad(BATCH_SCRIPT).Result()
	if err != nil {
		r.cleanup()
		return pipeline.NewPluginExitError("error loading lua redis script: %v", err)
	}
	r.luaSha = sha

	for i := 0; i < r.config.InputThreads; i++ {
		r.wgIn.Add(1)
		go r.pollRedis(eventChan, errChan)
	}

	if !splitter.UseMsgBytes() {
		name := r.runner.Name()
		packDec := func(pack *pipeline.PipelinePack) {
			if r.config.SetHostname {
				pack.Message.SetHostname(r.config.Address)
			}
			pack.Message.SetType(name)
			pack.Message.SetLogger(fmt.Sprintf("%s-%s", name, r.config.Key))
		}
		splitter.SetPackDecorator(packDec)
	}

	go func() {
		r.wgIn.Wait()
		close(r.stopChan)
	}()

	ok := true
	for ok {
		select {
		case msg, ok := <-eventChan:
			if !ok {
				return nil
			}

			err = splitter.SplitStream(strings.NewReader(msg), deliverer)
			if err != nil && err != io.EOF {
				return fmt.Errorf("error reading redis result: %v", err)
			}

		case err, ok = <-errChan:
			_, pluginExit := err.(pipeline.PluginExitError)
			if pluginExit {
				r.Stop()
			} else if err == io.EOF {
				// TODO Do we need to stop the workers instead? Is this right?
				return pipeline.NewPluginExitError("error reading redis result: %v", err)
			} else {
				r.runner.LogError(err)
			}

		case _, ok = <-r.stopChan:
		}
	}

	return
}

func (r *RedisListInput) Stop() {
	r.cleanup()
	close(r.workerStopChan)
}

func (r *RedisListInput) CleanupForRestart() {
	r.cleanup()
}

func (r *RedisListInput) cleanup() {
	if r.client != nil {
		if err := r.client.Close(); err != nil {
			r.runner.LogError(fmt.Errorf("error closing redis client: %v", err))
		}
		r.client = nil
	}
}

func init() {
	pipeline.RegisterPlugin("RedisListInput", func() interface{} {
		return new(RedisListInput)
	})
}

package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"slices"
	"sync"
	"syscall"
	"time"
)

type User struct {
	token string
	file  string
}

type Message struct {
	Token  string
	FileID string
	Data   string
}

type Cache struct {
	messageChannels map[string]chan Message
	validator       Validator
	mu              sync.Mutex
}

// validate user and allocate channel for user
func (c *Cache) addUser(wr User) {
	if !c.validator.isTokenValid(wr.token) {
		return
	}

	_, isChannelExists := c.messageChannels[wr.token]
	if isChannelExists {
		return
	}

	c.messageChannels[wr.token] = make(chan Message, 100)
}

func NewCache(validator Validator) Cache {
	return Cache{
		messageChannels: map[string]chan Message{},
		validator:       validator,
	}
}

type Validator struct {
	validTokens []string
}

func (v Validator) isTokenValid(token string) bool {
	return slices.Contains(v.validTokens, token)
}

func NewValidator(validTokens []string) Validator {
	return Validator{validTokens: validTokens}
}

type Worker struct {
	millisecondTimeout time.Duration
}

func NewWorker(millisecondTimeout time.Duration) Worker {
	return Worker{millisecondTimeout: millisecondTimeout}
}

var cache Cache
var validator Validator
var worker Worker

func main() {
	//config
	numUsers := 10
	numMessages := 10
	var workerTimeout time.Duration = 1000

	validator = NewValidator([]string{"token1", "token2", "token3"})
	cache = NewCache(validator)
	worker = NewWorker(workerTimeout)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM, os.Interrupt)
	defer stop()

	var wg sync.WaitGroup
	wg.Add(1)
	go WriteFiles(ctx, &wg)

	for i := 0; i < numMessages; i++ {
		for j := 0; j < numUsers; j++ {
			user := User{fmt.Sprintf("token%d", j), fmt.Sprintf("file%d.txt", j)}
			cache.addUser(user)

			SendMsg(Message{
				Token:  user.token,
				FileID: user.file,
				Data:   fmt.Sprintf("Message %d from user %d", i, j),
			})
			SendMsg(Message{
				Token:  user.token,
				FileID: "file.txt",
				Data:   fmt.Sprintf("Message %d from user %d", i, j),
			})
		}
		time.Sleep(time.Millisecond * 500)
	}

	wg.Wait()
}

func SendMsg(msg Message) {
	if !cache.validator.isTokenValid(msg.Token) {
		return
	}
	cache.mu.Lock()
	ch := cache.messageChannels[msg.Token]
	cache.mu.Unlock()
	ch <- msg
}

func WriteFiles(ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()
	ticker := time.NewTicker(time.Millisecond * worker.millisecondTimeout)
	for {
		select {
		case <-ticker.C:
			WriteMessagesToFiles()
		case <-ctx.Done():
			fmt.Println("do some cleanup")
			for _, ch := range cache.messageChannels {
			NextChannel:
				for {
					select {
					case msg := <-ch:
						WriteMessageToFile(msg)
					default:
						break NextChannel
					}
				}
			}
			return
		}
	}
}

func WriteMessagesToFiles() {
	cache.mu.Lock()
	for _, ch := range cache.messageChannels {
		select {
		case msg := <-ch:
			WriteMessageToFile(msg)
		default:
			// no message received, continue
		}
	}
	cache.mu.Unlock()
}

func WriteMessageToFile(msg Message) {
	file, err := os.OpenFile(msg.FileID, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0600)
	if err != nil {
		fmt.Println("Unable to open or create file:", err)
		return
	}
	defer file.Close()

	if _, err = file.WriteString(msg.Data + "\n"); err != nil {
		fmt.Println("Unable to write to file:", err)
		return
	}
}

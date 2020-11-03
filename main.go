package main

import (
	"context"
	"fmt"
	"github.com/go-redis/redis/v8"
	"strconv"
	"time"
)

type redisClient struct {
	rdb *redis.Client
	ctx context.Context
}

func main() {
	rdbClient := &redisClient{
		rdb: redis.NewClient(&redis.Options{
			Addr:     "localhost:6379",
			Password: "",
			DB:       0,
		}),
		ctx: context.Background(),
	}

	/*err := rdbClient.rdb.HSet(rdbClient.ctx, "user:02e2e376c81688876ebae5cea4b6b01c:crf:15",
		[]string{"expiration", "2020-11-01 19:19:58"})
	fmt.Println(err)*/

	/*val2, ok := GetUserToken(rdbClient, "02e2e376c81688876ebae5cea4b6b01c", "crf", 14)
	fmt.Println(val2, ok)*/

	err := rdbClient.rdb.HSet(rdbClient.ctx, "user:02e2e376c81688876ebae5cea4b6b01c:crf",
		[]string{"partner", "bmc", "first_interaction", "2019-01-09 22:25:43",
			"last_interaction", "2020-10-09 14:18:41", "next_interaction", "2020-10-09 15:18:41",
			"last_login", "2019-08-09 20:22:13", "last_click", "2020-10-01 16:38:38",
			"last_open", "2020-10-09 04:31:26", "interactions", "1", "status", "new"})
	fmt.Println(err)

	val2, ok := GetUser(rdbClient, "02e2e376c81688876ebae5cea4b6b01c", "crf")
	fmt.Println(val2, ok)

	SetLastInteraction(rdbClient, "02e2e376c81688876ebae5cea4b6b01c", "crf")

	val2, ok = GetUser(rdbClient, "02e2e376c81688876ebae5cea4b6b01c", "crf")
	fmt.Println(val2, ok)
}

func GetUserToken(rdbClient *redisClient, userHash, clientID string, channelID int8) (map[string]string, bool) {
	key := fmt.Sprintf("user:%s:%s:%d", userHash, clientID, channelID)

	rExists := rdbClient.rdb.Exists(rdbClient.ctx, key)
	if rExists.Err() != nil {
		panic(rExists.Err())
	}

	if rExists.Val() == 0 {
		return nil, false
	}

	val := rdbClient.rdb.HGetAll(rdbClient.ctx, key)
	val2, err2 := val.Result()
	if err2 != nil {
		panic(err2)
	}
	return val2, true
}

func GetUser(rdbClient *redisClient, userHash, clientID string) (map[string]string, bool) {
	key := fmt.Sprintf("user:%s:%s", userHash, clientID)

	rExists := rdbClient.rdb.Exists(rdbClient.ctx, key)
	if rExists.Err() != nil {
		panic(rExists.Err())
	}

	if rExists.Val() == 0 {
		return nil, false
	}

	val := rdbClient.rdb.HGetAll(rdbClient.ctx, key)
	val2, err2 := val.Result()
	if err2 != nil {
		panic(err2)
	}
	return val2, true
}

func SetLastInteraction(rdbClient *redisClient, userHash, clientID string) {
	userFromRedis, existsAtRedis := GetUser(rdbClient, userHash, clientID)
	if !existsAtRedis {
		return
	}

	now := time.Now()
	nowFormated := now.Format("2006-01-02 15:04:05")

	userFromRedis["last_interaction"] = nowFormated

	interactions, _ := strconv.Atoi(userFromRedis["interactions"])
	interactionsPlusOne := interactions + 1
	userFromRedis["interactions"] = strconv.Itoa(interactionsPlusOne)

	if interactionsPlusOne > 11 {
		userFromRedis["next_interaction"] = now.Add(time.Hour * 24 * time.Duration(interactionsPlusOne)).Format("2006-01-02 15:04:05")
	} else {
		userFromRedis["next_interaction"] = now.Add(time.Hour * 1).Format("2006-01-02 15:04:05")
	}

	key := fmt.Sprintf("user:%s:%s", userHash, clientID)
	rdbClient.rdb.HSet(rdbClient.ctx, key,
		[]string{"partner", userFromRedis["partner"],
			"first_interaction", userFromRedis["first_interaction"],
			"last_interaction", userFromRedis["last_interaction"],
			"next_interaction", userFromRedis["next_interaction"],
			"last_login", userFromRedis["last_login"],
			"last_click", userFromRedis["last_click"],
			"last_open", userFromRedis["last_open"],
			"interactions", userFromRedis["interactions"],
			"status", userFromRedis["status"]})
}

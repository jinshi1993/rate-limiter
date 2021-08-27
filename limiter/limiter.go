package limiter

import (
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/go-redis/redis"
	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"
)

const (
	extQuota = "quota"
	extRate  = "rate"
)

var (
	errOverQuato     = errors.New("over quato")
	errPeriodConvert = errors.New("period convert error")
	errEmptyConfig   = errors.New("empty config")
)

var Limit *Limiter = &Limiter{}

type Limiter struct {
	client *redis.Client
}

func (l *Limiter) Initialize() error {
	l.client = redis.NewClient(&redis.Options{
		Addr: fmt.Sprintf("%s:%d",
			viper.GetString("REDIS_HOST"),
			viper.GetInt("REDIS_PORT"),
		),
		DB:       viper.GetInt("REDIS_DATABASE"),
		Password: viper.GetString("REDIS_PASSWORD"),
	})

	l.keepalive()

	return l.loadScript()
}

func (l *Limiter) keepalive() {
	go func() {
		_, err := l.client.Ping().Result()
		if err != nil {
			logrus.Errorf("redis keepalive failed: %s", err.Error())
		}
		time.Sleep(time.Duration(viper.GetInt("REDIS_KEEPALIVE")) * time.Second)
	}()
}

func (l *Limiter) loadScript() error {
	for class, algorithm := range script.AlgorithmMap {
		exist, err := l.client.ScriptExists(algorithm.SHA1).Result()
		if err != nil {
			logrus.Errorf("check script failed: %s", err.Error())
			return err
		}

		if !exist[0] {
			_, err := l.client.ScriptLoad(algorithm.Content).Result()
			if err != nil {
				logrus.Errorf("load script failed: %s", err.Error())
				return err
			}
		} else {
			logrus.Infof("script %d exists", class)
		}
	}

	return nil
}

func (l *Limiter) CheckQuota(key string, period string) (int64, error) {
	scriptSHA1 := script.AlgorithmMap[script.CounterAlg].SHA1
	consumeTimeTag, consumeTTL := script.ConvertQuotaPeriod(period)
	if consumeTimeTag == 0 {
		return -1, errPeriodConvert
	}

	keyPrefix := fmt.Sprintf("%s_%s", key, extQuota)

	ret, err := l.client.EvalSha(
		scriptSHA1,
		[]string{keyPrefix, period},
		consumeTimeTag,
		consumeTTL,
	).Result()
	if err != nil {
		return -1, err
	}

	rest := ret.(int64)
	if rest < 0 {
		return -1, errOverQuato
	}

	return rest, nil
}

type LimiterRes struct {
	Over     bool
	Classify string
	Rest     int64
	Interval int64
	Limit    int64
}

func (l *Limiter) Check(key string) (error, []LimiterRes) {
	luaArgs := []interface{}{}

	quotaConfig, err := l.client.HGetAll(fmt.Sprintf("%s_quota", key)).Result()
	if err != nil {
		return err, nil
	}
	for key, value := range quotaConfig {
		timestampMS, ttl := script.ConvertQuotaPeriod(key)
		if timestampMS == 0 {
			return errPeriodConvert, nil
		}
		luaArgs = append(luaArgs, timestampMS, ttl, value)
	}

	rateConfig, err := l.client.HGetAll(fmt.Sprintf("%s_rate", key)).Result()
	if err != nil {
		return err, nil
	}
	for key, value := range rateConfig {
		timestampMS, ttl := script.ConvertRatePeriod(key)
		if timestampMS == 0 {
			return errPeriodConvert, nil
		}
		luaArgs = append(luaArgs, timestampMS, ttl, value)
	}

	if len(quotaConfig)+len(rateConfig) == 0 {
		return errEmptyConfig, nil
	}

	luaKeys := []string{
		key,
		strconv.Itoa(len(quotaConfig)),
		strconv.Itoa(len(rateConfig)),
	}

	scriptSHA1 := script.AlgorithmMap[script.MultiCounterAlg].SHA1
	ret, err := l.client.EvalSha(
		scriptSHA1,
		luaKeys[:],
		luaArgs...,
	).Result()
	if err != nil {
		return err, nil
	}

	logrus.Debugf("keys: %v, argv: %v, ret: %v", luaKeys, luaArgs, ret)

	limiterRes := ret.([]interface{})
	limiterResList := []LimiterRes{}

	if limiterRes[0].(int64) == 0 {
		limiterResList = append(limiterResList, LimiterRes{
			Over:     true,
			Classify: limiterRes[1].(string),
			Rest:     0,
			Interval: limiterRes[2].(int64),
			Limit:    limiterRes[3].(int64),
		},
		)
	} else {
		for index := 0; index < len(limiterRes)/4; index++ {
			limiterResList = append(limiterResList, LimiterRes{
				Over:     false,
				Classify: limiterRes[index*4+1].(string),
				Rest:     limiterRes[index*4+3].(int64) - limiterRes[index*4+4].(int64),
				Interval: limiterRes[index*4+2].(int64),
				Limit:    limiterRes[index*4+3].(int64),
			},
			)
		}
	}

	logrus.Debugf("check res: %v", limiterResList)

	return nil, limiterResList
}

func (l *Limiter) PutConfig(key, ext string, configs map[string]int64) error {
	for period, value := range configs {
		_, err := l.client.HSet(fmt.Sprintf("%s_%s", key, ext), period, value).Result()
		if err != nil {
			return err
		}
		logrus.Infof("add config %s_%s %s %d ok", key, ext, period, value)
	}
	return nil
}

func (l *Limiter) GetConfig(key, ext string) (error, map[string]int64) {
	configs := map[string]int64{}
	res, err := l.client.HGetAll(fmt.Sprintf("%s_%s", key, ext)).Result()
	if err != nil {
		return err, nil
	}
	for period, value := range res {
		configs[period], err = strconv.ParseInt(value, 10, 64)
		if err != nil {
			return err, nil
		}
	}

	logrus.Infof("get config %s_%s ok: %v", key, ext, configs)

	return nil, configs
}

func (l *Limiter) DelConfig(key, ext string, periods []string) error {
	for _, period := range periods {
		_, err := l.client.HDel(fmt.Sprintf("%s_%s", key, ext), period).Result()
		if err != nil {
			return err
		}
		logrus.Infof("del config %s_%s %s ok", key, ext, period)
	}
	return nil
}

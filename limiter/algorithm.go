package limiter

import (
	"crypto/sha1"
	"fmt"
	"time"

	"github.com/thinkeridea/go-extend/exstrings"
)

const (
	CounterAlg = iota
	SlidingWindowAlg
	MultiCounterAlg
)

const (
	periodSecond = "second"
	periodMinute = "minute"
	periodHour   = "hour"
	periodDay    = "day"
	periodMonth  = "month"
)

const (
	rateSegment = 10
)

const counterScript = `
local key_prefix = KEYS[1]
local period = KEYS[2]
local consume_timestamp_ms = tonumber(ARGV[1]) * 1000
local consume_ttl = tonumber(ARGV[2])

local total = 0
local consume = 0

local key_quota = key_prefix .. "_config_" .. period
local quota = redis.call("GET", key_quota)

local result = {}

if quota ~= false then
	total = quota
    redis.replicate_commands()
    local key_consume = key_prefix .. "_" .. consume_timestamp_ms
	local success = redis.call("SETNX", key_consume, 0)
	consume = redis.call("GET", key_consume)
	if consume < quota then
		redis.call("INCR", key_consume)
		consume = consume + 1
	end
	if success == 1 then
		redis.call("EXPIRE", key_consume, consume_ttl)
	end
end

table.insert(result, total)
table.insert(result, consume)

return result
`

const slidingwindowScript = `
local key_prefix = KEYS[1]
local period = KEYS[2]
local consume_timestamp_ms = tonumber(ARGV[1]) * 1000
local consume_ttl = tonumber(ARGV[2])

local total = 0
local consume = 0

local key_quota = key_prefix .. "_config_" .. period
local quota = redis.call("GET", key_quota)

local result = {}

if quota ~= false then
	total = quota
    redis.replicate_commands()
    local key_consume = key_prefix .. "_" .. consume_timestamp_ms
    local success = redis.call("SETNX", key_consume, 0)
    for i=0,9 do
        key_consume = key_prefix .. "_" .. consume_timestamp_ms - i * consume_ttl * 1000 / 10
	    ret = redis.call("GET", key_consume)
        if ret == false then
            break
        else
            consume = consume + ret
        end
    end
    if consume < quota then
        redis.call("INCR", key_consume)
        consume = consume + 1
    end
    if success == 1 then
		redis.call("EXPIRE", key_consume, consume_ttl)
	end
end

table.insert(result, total)
table.insert(result, consume)

return result
`

const multicounterScript = `
local service = KEYS[1]
local quota_nums = tonumber(KEYS[2])
local rate_nums = tonumber(KEYS[3])

local quota = 0
local rate = 0
local timestamp = 0
local ttl = 0

local incr_keys = {}

local check = true
local result = {}
local success_result = {}
table.insert(success_result, 1)

for index, value in pairs(ARGV) do
    if check == false then
        break
    end
    if index % 3 == 0 then
        if index > quota_nums * 3 then
            rate = tonumber(value)
            local key_rate_comsume = service .. "_rate_" .. timestamp
            local success = redis.call("SETNX", key_rate_comsume, 0)
            if success == 1 then
                redis.call("EXPIRE", key_rate_comsume, ttl)
            end
            local consume = 0
            for i=0,9 do
                local key_rate_comsume_seg = service .. "_rate_" .. timestamp - i * ttl * 1000 / 10
                local ret = redis.call("GET", key_rate_comsume_seg)
                if ret ~= false then
                    consume = consume + tonumber(ret)
                end
            end
            if consume < rate then
                table.insert(incr_keys, key_rate_comsume)
                table.insert(success_result, "rate")
                table.insert(success_result, ttl)
                table.insert(success_result, rate)
                table.insert(success_result, consume + 1)
            else
                check = false
                table.insert(result, 0)
                table.insert(result, "rate")
                table.insert(result, ttl)
                table.insert(result, rate)
            end
        else
            quota = tonumber(value)
            local key_quota_comsume = service .. "_quota_" .. timestamp
            local success = redis.call("SETNX", key_quota_comsume, 0)
            if success == 1 then
                redis.call("EXPIRE", key_quota_comsume, ttl)
            end
            local consume = tonumber(redis.call("GET", key_quota_comsume))
            if consume < quota then
                table.insert(incr_keys, key_quota_comsume)
                table.insert(success_result, "quota")
                table.insert(success_result, ttl)
                table.insert(success_result, quota)
                table.insert(success_result, consume + 1)
            else
                check = false
                table.insert(result, 0)
                table.insert(result, "quota")
                table.insert(result, ttl)
                table.insert(result, quota)
            end
        end
    elseif index % 3 == 1 then
        timestamp = tonumber(value)
    else
        ttl = tonumber(value)
    end
end

if check == true then
    result = success_result
    for index, value in pairs(incr_keys) do
        redis.call("INCR", value)
    end
end

return result
`

type Algorithm struct {
	Content string
	SHA1    string
}

var (
	AlgorithmMap map[int]Algorithm
)

func init() {
	AlgorithmMap = make(map[int]Algorithm)
	AlgorithmMap[CounterAlg] = Algorithm{
		Content: counterScript,
		SHA1:    fmt.Sprintf("%x", sha1.Sum(exstrings.UnsafeToBytes(counterScript))),
	}
	AlgorithmMap[SlidingWindowAlg] = Algorithm{
		Content: slidingwindowScript,
		SHA1:    fmt.Sprintf("%x", sha1.Sum(exstrings.UnsafeToBytes(slidingwindowScript))),
	}
	AlgorithmMap[MultiCounterAlg] = Algorithm{
		Content: multicounterScript,
		SHA1:    fmt.Sprintf("%x", sha1.Sum(exstrings.UnsafeToBytes(multicounterScript))),
	}
}

func ConvertQuotaPeriod(period string) (int64, int64) {
	switch period {
	case periodSecond:
		return ConvertTime("2006-01-02_15:04:05"), 1
	case periodMinute:
		return ConvertTime("2006-01-02_15:04"), 1 * 60
	case periodHour:
		return ConvertTime("2006-01-02_15"), 1 * 60 * 60
	case periodDay:
		return ConvertTime("2006-01-02"), 1 * 60 * 60 * 24
	case periodMonth:
		return ConvertTime("2006-01"), 1 * 60 * 60 * 24 * 31
	}
	return 0, 0
}

func ConvertRatePeriod(period string) (int64, int64) {
	var timeFormat string
	var ttl int64
	switch period {
	case periodSecond:
		ttl = 1
		timeFormat = "2006-01-02_15:04:05.000"
	case periodMinute:
		ttl = 1 * 60
		timeFormat = "2006-01-02_15:04:05"
	case periodHour:
		ttl = 1 * 60 * 60
		timeFormat = "2006-01-02_15:04"
	case periodDay:
		ttl = 1 * 60 * 60 * 24
		timeFormat = "2006-01-02_15"
	case periodMonth:
		ttl = 1 * 60 * 60 * 24 * 31
		timeFormat = "2006-01-02"
	}
	var segment int64 = ttl * 1000 / rateSegment
	return ConvertTime(timeFormat) / segment * segment, ttl
}

func ConvertTime(timeFormat string) int64 {
	loc, _ := time.LoadLocation("Local")
	timeNow, _ := time.ParseInLocation(timeFormat, time.Now().UTC().Format(timeFormat), loc)
	return timeNow.UnixNano() / 1e6
}

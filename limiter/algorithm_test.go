package limiter

import (
	"testing"
	"time"
)

func printQuotaPeriod(t *testing.T, periodSecond string) {
	timestamp, interval := ConvertQuotaPeriod(periodSecond)
	t.Logf("%s\t%d\t%s\t%d",
		periodSecond,
		timestamp,
		time.Unix(timestamp/1000, 0).Format("2006-01-02 15:04:05.000"),
		interval,
	)
}

func Test_ConvertQuotaPeriod(t *testing.T) {
	printQuotaPeriod(t, periodSecond)
	printQuotaPeriod(t, periodMinute)
	printQuotaPeriod(t, periodHour)
	printQuotaPeriod(t, periodDay)
	printQuotaPeriod(t, periodMonth)
}

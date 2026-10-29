package monitoring

import (
	"errors"
	"strings"
	"time"
	"unicode"
)

func ValidateProxAnalyticsQuery(q ProxAnalyticsQuery) error {
	if q.FromMS <= 0 || q.ToMS <= q.FromMS || q.ToMS-q.FromMS > 31*24*60*60*1000 || q.ToMS > 253402300799999 {
		return errors.New("range must be positive and at most 31 days")
	}
	if q.Granularity != "hour" && q.Granularity != "day" {
		return errors.New("granularity must be hour or day")
	}
	if q.Outcome != "all" && q.Outcome != "success" && q.Outcome != "failed" {
		return errors.New("outcome must be all, success or failed")
	}
	if q.TimeZone == "" || len(q.TimeZone) > 100 || q.TimeZone == "Local" {
		return errors.New("time_zone must be an IANA time zone")
	}
	if _, err := time.LoadLocation(q.TimeZone); err != nil {
		return errors.New("invalid time_zone")
	}
	if q.Limit < 0 || q.Limit > 200 {
		return errors.New("limit must be between 1 and 200 when supplied")
	}
	if (q.BeforeMS == nil) != (q.BeforeID == nil) {
		return errors.New("before_ms and before_id must be supplied together")
	}
	if q.BeforeMS != nil && (*q.BeforeMS < q.FromMS || *q.BeforeMS >= q.ToMS || *q.BeforeID <= 0 || *q.BeforeID > 9007199254740991) {
		return errors.New("invalid cursor")
	}
	if !boundedAnalyticsString(q.Search, 256) || !boundedAnalyticsString(q.Model, 256) || !boundedAnalyticsString(q.Provider, 128) {
		return errors.New("search or filter exceeds bounds")
	}
	if len(q.Catalog) > 1000 {
		return errors.New("catalog must contain at most 1000 models")
	}
	for _, m := range q.Catalog {
		if strings.TrimSpace(m.ID) == "" || strings.TrimSpace(m.InstanceID) == "" || strings.TrimSpace(m.Provider) == "" || !boundedAnalyticsString(m.ID, 256) || !boundedAnalyticsString(m.InstanceID, 256) || !boundedAnalyticsString(m.InstanceLabel, 256) || !boundedAnalyticsString(m.Provider, 128) {
			return errors.New("invalid catalog entry")
		}
	}
	return nil
}
func boundedAnalyticsString(s string, n int) bool {
	return len(s) <= n && strings.IndexFunc(s, unicode.IsControl) < 0
}

package main

import (
	"context"
	"database/sql"
	"fmt"
	"net/http"
	"os"
	"time"

	_ "github.com/go-sql-driver/mysql"
	//"github.com/k0kubun/pp"
	"github.com/sirupsen/logrus"
	"gopkg.in/yaml.v2"
)

type (
	MetricItem struct {
		Name     string  `yaml:"name"`
		Article  string  `yaml:"article"`
		Category string  `yaml:"category"`
		Factor   float64 `yaml:"factor"`
	}
	MetricsGroups map[string][]*MetricItem

	MetricMapper struct {
		*MetricItem
		Group string
	}
	MetricMapperMap map[string]*MetricMapper

	Metrics map[string]float64

	Config struct {
		DB            string        `yaml:"db"`
		MetricsGroups MetricsGroups `yaml:"metrics_groups"`
	}

	StatsRow struct {
		Count int64
		Name  string
	}
)

func main() {
	configFile, ok := os.LookupEnv("CONFIG")
	if !ok {
		configFile = "/etc/strichliste_exporter/config.yml"
	}
	configReader, err := os.Open(configFile)
	if err != nil {
		logrus.Fatalf("unable to open configfile: %s", err)
	}
	config := Config{}
	yaml.NewDecoder(configReader).Decode(&config)
	dsn, ok := os.LookupEnv("DB")
	if ok {
		config.DB = dsn
	}

	mapper := MetricMapperMap{}
	for group, metricItems := range config.MetricsGroups {
		for _, mi := range metricItems {
			m := MetricMapper{mi, group}
			mapper[mi.Article] = &m
		}
	}

	db, err := sql.Open("mysql", config.DB)
	if err != nil {
		logrus.Fatalf("unable to load database config: %s", err)
	}
	err = db.Ping()
	if err != nil {
		logrus.Fatalf("unable to connect to database: %s", err)
	}

	http.HandleFunc("/metrics", func(w http.ResponseWriter, r *http.Request) {
		m := metric(db, mapper)
		for k, v := range m {
			fmt.Fprintf(w, "strichliste_%s %f\n", k, v)
		}
	})
	http.ListenAndServe(":8080", nil)
}

func metric(db *sql.DB, mapper MetricMapperMap) (metrics Metrics) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	c, err := db.Conn(ctx)
	if err != nil {
		println(err.Error())
		return Metrics{"error{msg=\"connection error\"}": 1}
	}
	defer c.Close()
	q := `
	    select
		count(t.id) count,
		a.name name
	    from
		transactions t,
		article a
	    where
		t.article_id = a.id
		and t.deleted = 0
	    group by a.id
        `
	rows, err := c.QueryContext(ctx, q)
	if err != nil {
		return Metrics{"error{msg=\"query error\"}": 1}
	}
	defer rows.Close()

	metrics = Metrics{
		"not_found": 0,
	}
	for rows.Next() {
		r := StatsRow{}
		rows.Scan(&r.Count, &r.Name)

		// group based
		mi, ok := mapper[r.Name]
		if !ok {
			metrics["not_found"]++
		} else {
			key := fmt.Sprintf(
				"%s{category=\"%s\", name=\"%s\"}",
				mi.Group,
				mi.Category,
				mi.Name,
			)
			value := mi.Factor * float64(r.Count)
			_, ok = metrics[key]
			if !ok {
				metrics[key] = value
			} else {
				metrics[key] += value
			}
		}

		// general counter
		value := float64(r.Count)
		key := fmt.Sprintf("counter{name=\"%s\"}", r.Name)
		_, ok = metrics[key]
		if !ok {
			metrics[key] = value
		} else {
			metrics[key] += value
		}

	}

	bankstats := map[string]string{
		"turnover": `select sum(amount) from transactions where amount < 0 limit 1`,
		"charges":  `select sum(amount) from transactions where amount > 0 limit 1`,
	}
	for key, q := range bankstats {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		c, err := db.Conn(ctx)
		if err != nil {
			println(err.Error())
			return Metrics{"error{msg=\"connection error\"}": 1}
		}
		rows, err = c.QueryContext(ctx, q)
		if err != nil {
			println(err.Error())
			return Metrics{"error{msg=\"query error\"}": 1}
		}
		rows.Next()
		amount := 0
		rows.Scan(&amount)
		_, ok := metrics[key]
		if !ok {
			metrics[key] = float64(amount)
		} else {
			metrics[key] += float64(amount)
		}

	}
	return
}

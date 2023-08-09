package main

import (
	"github.com/bcongdon/corral"
	"testing"
	"time"
)

func BenchmarkQ1(t *testing.B) {
	date, _ := time.Parse("2006-01-02", "1998-09-01")

	wc := q1{
		shipdate: date,
	}

	job := corral.NewJob(wc, wc)

	driver := corral.NewDriver(job,corral.WithInputs("../testdata.csv"))
	driver.Main()
}

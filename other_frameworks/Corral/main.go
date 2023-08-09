package main

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/bcongdon/corral"

	b64 "encoding/base64"
)

type lineitem int

const (
	L_ORDERKEY lineitem = iota
	L_PARTKEY
	L_SUPPKEY
	L_LINENUMBER
	L_QUANTITY
	L_EXTENDEDPRICE
	L_DISCOUNT
	L_TAX
	L_RETURNFLAG
	L_LINESTATUS
	L_SHIPDATE
	L_COMMITDATE
	L_RECEIPTDATE
	L_SHIPINSTRUCT
	L_SHIPMODE
	L_COMMENT
)

/**sql
	select
		l_returnflag,
		l_linestatus,
		sum(l_quantity) as sum_qty,
		sum(l_extendedprice) as sum_base_price,
		sum(l_extendedprice*(1-l_discount)) as sum_disc_price,
		sum(l_extendedprice*(1-l_discount)*(1+l_tax)) as sum_charge,
		avg(l_quantity) as avg_qty,
		avg(l_extendedprice) as avg_price, avg(l_discount) as avg_disc,
		count(*) as count_order
	from
		lineitem
	where
		l_shipdate <= '1998-09-01’
	group by
		l_returnflag,
		l_linestatus
	order by
		l_returnflag,
		l_linestatus
**/

type q1 struct {
	shipdate time.Time
}

const verbose = true

func (w q1) Map(key, value string, emitter corral.Emitter) {
	//line := strings.FieldsFunc(value,
	//	func(c rune) bool { return c == '|' })
	line := strings.Split(value, ",")
	if len(line) < 15 {
		return
	}

	shipdate_value := line[L_SHIPDATE]

	s_year, _ := strconv.Atoi(shipdate_value[0:4])
	s_month, _ := strconv.Atoi(shipdate_value[6:7])
	s_day, _ := strconv.Atoi(shipdate_value[9:10])

	//shipdate, _ := time.Parse("2006-01-02", line[L_SHIPDATE])
	shipdate := time.Date(s_year, time.Month(s_month), s_day, 0, 0, 0, 0, time.Local)
	if shipdate.Before(w.shipdate) {
		l_returnflag := line[L_RETURNFLAG]
		l_linestatus := line[L_LINESTATUS]
		data := strings.Join([]string{
			line[L_QUANTITY],
			line[L_EXTENDEDPRICE],
			line[L_DISCOUNT],
			line[L_TAX],
		}, ",")

		sEnc := b64.StdEncoding.EncodeToString([]byte(data))

		emitter.Emit(l_returnflag+","+l_linestatus, sEnc)
	}

}

func saveFloat(line []string, key int) float64 {
	if len(line) < key {
		return 0.
	}
	val, _ := strconv.ParseFloat(line[key], 32)
	return val
}

func (w q1) Reduce(key string, values corral.ValueIterator, emitter corral.Emitter) {

	sum_base_price := 0.
	sum_qty := 0.
	sum_disc_price := 0.
	sum_discount := 0.
	sum_charge := 0.
	count := 0

	for value := range values.Iter() {
		data, _ := b64.StdEncoding.DecodeString(value)
		line := strings.Split(string(data), ",")

		l_quantity := saveFloat(line, 0)
		l_extendedprice := saveFloat(line, 1)
		l_discount := saveFloat(line, 2)
		l_tax := saveFloat(line, 3)

		sum_base_price += l_extendedprice
		sum_qty += l_quantity
		sum_discount += l_discount
		sum_disc_price += l_extendedprice * (1 - l_discount)
		sum_charge += l_extendedprice * (1 - l_tax)
		count++

	}

	avg_qty := sum_qty / float64(count)
	avg_price := sum_base_price / float64(count)
	avg_disc := sum_discount / float64(count)

	value := fmt.Sprintf("%.2f,%.2f,%.2f,%.2f,%.2f,%.2f,%.2f,%d", sum_qty, sum_base_price, sum_disc_price, sum_charge, avg_qty, avg_price, avg_disc, count)

	_ = emitter.Emit(key, value)

}

func main() {
	date, _ := time.Parse("2006-01-02", "1998-09-01")

	wc := q1{
		shipdate: date,
	}

	job := corral.NewJob(wc, wc)

	driver := corral.NewDriver(job)
	driver.Main()
}

/*
 * Copyright 2018 Xiaomi, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package database

import (
	"database/sql"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/XiaoMi/soar/common"
)

var (
	defaultBatchSize int64 = 200
	goNum            int   = 10
)

/*--------------------
* The following choice of minrows is based on the paper
* "Random sampling for histogram construction: how much is enough?"
* by Surajit Chaudhuri, Rajeev Motwani and Vivek Narasayya, in
* Proceedings of ACM SIGMOD International Conference on Management
* of Data, 1998, Pages 436-447.  Their Corollary 1 to Theorem 5
* says that for table size n, histogram size k, maximum relative
* error in bin size f, and error probability gamma, the minimum
* random sample size is
*      r = 4 * k * ln(2*n/gamma) / f^2
* Taking f = 0.5, gamma = 0.01, n = 10^6 rows, we obtain
*      r = 305.82 * k
* Note that because of the log function, the dependence on n is
* quite weak; even at n = 10^12, a 300*k sample gives <= 0.66
* bin size error with probability 0.99.  So there's no real need to
* scale for n, which is a good thing because we don't necessarily
* know it at this point.
*--------------------
 */

// SamplingData 将数据从 onlineConn 拉取到 db 中
func (db *Connector) SamplingData(onlineConn *Connector, tables ...string) error {
	var err error
	if onlineConn.Database == db.Database {
		return fmt.Errorf("SamplingData the same database, From: %s/%s, To: %s/%s", onlineConn.Addr, onlineConn.Database, db.Addr, db.Database)
	}

	// 计算需要泵取的数据量
	wantRowsCount := 300 * common.Config.SamplingStatisticTarget

	for _, table := range tables {
		// 表类型检查
		if onlineConn.IsView(table) {
			return nil
		}

		// generate where condition
		var where string
		if common.Config.SamplingCondition == "" {
			tableStatus, err := onlineConn.ShowTableStatus(table)
			if err != nil {
				return err
			}

			if len(tableStatus.Rows) == 0 {
				common.Log.Info("SamplingData, Table %s with no data, stop sampling", table)
				return nil
			}

			tableRows, err := strconv.ParseUint(string(tableStatus.Rows[0].Rows), 10, 64)
			if tableRows == 0 || err != nil {
				common.Log.Info("SamplingData, Table %s with no data, stop sampling", table)
				if err != nil {
					common.Log.Error("SamplingData, Error: ", err.Error())
				}
				return nil
			}

			factor := float64(wantRowsCount) / float64(tableRows)
			common.Log.Debug("SamplingData, tableRows: %d, wantRowsCount: %d, factor: %f", tableRows, wantRowsCount, factor)
			where = fmt.Sprintf("where RAND() <= %f LIMIT %d", factor, wantRowsCount)
			if factor >= 1 {
				where = ""
			}
		} else {
			where = common.Config.SamplingCondition
		}
		err = db.startSampling(onlineConn.Conn, onlineConn.Database, table, where)
	}
	return err
}

// SamplingVData Sampling data from true database
func (db *Connector) SamplingVData(rConn *Connector, tbName string) error {
	// todo: check the same test & true.
	// todo: table status: rows

	rTbRows, err := rConn.GetTableRows(tbName)
	if err != nil || rTbRows == 0 {
		common.Log.Warn("[TrueDB] table %s got no data", tbName)
		return err
	}
	vTbRows, err := db.GetTableRows(tbName)
	if err != nil {
		return err
	}

	// 1 default
	factor := common.Config.SamplingFactor
	if factor == 0 {
		factor = 1
	}
	expect := int64(float64(rTbRows) * factor)
	if vTbRows >= expect {
		common.Log.Warn("[VirtualDB] table %s has no data to sync", tbName)
		return nil
	}

	common.Log.Info("vTableRows: %d, rTableRows: %d", vTbRows, rTbRows)

	offset := int64(float64(vTbRows) / factor)
	common.Log.Info("Start sampling data, offset: %d, trueDBRows: %d, factor: %f", offset, rTbRows, factor)
	pri, err := rConn.ShowPrimaryKey(rConn.Database, tbName)
	if err != nil {
		return err
	}
	return db.samplingVData(rConn.Conn, offset, rTbRows, defaultBatchSize, factor, tbName, pri)
}

// samplingVData "sync" sampling data from rConn database table.
// todo: factor change, database change.
// `SELECT * FROM (SELECT * FROM `actor` ORDER BY actor_id LIMIT 0,200) AS tb WHERE RAND() < 0.500000;`
func (db *Connector) samplingVData(rDB *sql.DB, offset, srcRows, batchSize int64, factor float64, tbName, pri string) error {
	var err error
	preSql := fmt.Sprintf("SELECT * FROM (SELECT * FROM `%s` ORDER BY %s LIMIT",
		//Escape(dbName, false),
		Escape(tbName, false),
		Escape(pri, false))
	var limit = batchSize
	var wg sync.WaitGroup
	ch := make(chan struct{}, goNum)
	for offset < srcRows {
		if (srcRows - offset) <= batchSize {
			limit = srcRows - offset
		}
		querySql := fmt.Sprintf("%s %d,%d) AS tb WHERE RAND() < %f;", preSql, offset, limit, factor)
		common.Log.Info("[TrueDB] query sql: %s", querySql)
		ch <- struct{}{}
		wg.Add(1)
		go func(qSql string) {
			defer func() {
				<- ch
				wg.Done()
			}()

			err = db.samplingWithQuerySql(rDB, querySql, tbName)
			if err != nil {
				common.Log.Error("samplingWithQuerySql error %s ", err)
			}
		}(querySql)
		offset += limit
	}
	wg.Wait()
	return nil
}

func (db *Connector)samplingWithQuerySql(rDB *sql.DB, sqlStr, table string) error {
	res, err := rDB.Query(sqlStr)
	if err != nil {
		return err
	}

	// columns list
	columns, err := res.Columns()
	if err != nil {
		return err
	}
	row := make([][]byte, len(columns))
	tableFields := make([]interface{}, 0)
	for i := range columns {
		tableFields = append(tableFields, &row[i])
	}
	columnTypes, err := res.ColumnTypes()
	if err != nil {
		return err
	}

	// sampling data
	var valuesStr []string
	columnsStr := "`" + strings.Join(columns, "`,`") + "`"
	for res.Next() {
		var values []string
		err = res.Scan(tableFields...)
		if err != nil {
			common.Log.Debug(err.Error())
		}
		for i, val := range row {
			if val == nil {
				values = append(values, "NULL")
			} else {
				switch columnTypes[i].DatabaseTypeName() {
				case "JSON":
					// https://github.com/XiaoMi/soar/issues/178
					values = append(values, fmt.Sprintf(`convert(X'%s' using utf8mb4)`, fmt.Sprintf("%x", val)))
				case "TIMESTAMP", "DATETIME":
					t, err := time.Parse(time.RFC3339, string(val))
					if err != nil {
						values = append(values, fmt.Sprintf(`"%s"`, string(val)))
					} else {
						values = append(values, fmt.Sprintf(`"%s"`, TimeString(t)))
					}
				default:
					values = append(values, fmt.Sprintf(`unhex("%s")`, fmt.Sprintf("%x", val)))
				}
			}
		}
		valuesStr = append(valuesStr, "("+strings.Join(values, `,`)+")")
	}
	if len(valuesStr) > 0 {
		err = db.doSampling(table, columnsStr, strings.Join(valuesStr, `,`))
		if err != nil {
			common.LogIfWarn(err, "")
		}
	}
	_ = res.Close()
	return err
}

// startSampling sampling data from OnlineDSN to TestDSN
func (db *Connector) startSampling(onlineConn *sql.DB, database, table string, where string) error {
	samplingQuery := fmt.Sprintf("select * from `%s`.`%s` %s",
		Escape(database, false),
		Escape(table, false),
		Escape(where, false))
	common.Log.Debug("startSampling with Query: %s", samplingQuery)
	res, err := onlineConn.Query(samplingQuery)
	if err != nil {
		return err
	}

	// columns list
	columns, err := res.Columns()
	if err != nil {
		return err
	}
	row := make([][]byte, len(columns))
	tableFields := make([]interface{}, 0)
	for i := range columns {
		tableFields = append(tableFields, &row[i])
	}
	columnTypes, err := res.ColumnTypes()
	if err != nil {
		return err
	}

	// sampling data
	var valuesCount int
	var valuesStr []string
	maxValuesCount := 200 // one time insert values count, TODO: config able
	columnsStr := "`" + strings.Join(columns, "`,`") + "`"
	for res.Next() {
		var values []string
		err = res.Scan(tableFields...)
		if err != nil {
			common.Log.Debug(err.Error())
		}
		for i, val := range row {
			if val == nil {
				values = append(values, "NULL")
			} else {
				switch columnTypes[i].DatabaseTypeName() {
				case "JSON":
					// https://github.com/XiaoMi/soar/issues/178
					values = append(values, fmt.Sprintf(`convert(X'%s' using utf8mb4)`, fmt.Sprintf("%x", val)))
				case "TIMESTAMP", "DATETIME":
					t, err := time.Parse(time.RFC3339, string(val))
					if err != nil {
						values = append(values, fmt.Sprintf(`"%s"`, string(val)))
					} else {
						values = append(values, fmt.Sprintf(`"%s"`, TimeString(t)))
					}
				default:
					values = append(values, fmt.Sprintf(`unhex("%s")`, fmt.Sprintf("%x", val)))
				}
			}
		}
		valuesStr = append(valuesStr, "("+strings.Join(values, `,`)+")")
		valuesCount++
		if maxValuesCount <= valuesCount {
			err = db.doSampling(table, columnsStr, strings.Join(valuesStr, `,`))
			if err != nil {
				break
			}
			valuesStr = make([]string, 0)
			valuesCount = 0
		}
	}
	if len(valuesStr) > 0 {
		err = db.doSampling(table, columnsStr, strings.Join(valuesStr, `,`))
		if err != nil {
			common.LogIfWarn(err, "")
		}
	}
	res.Close()
	return err
}

// 将泵取的数据转换成 insert 语句并在 testConn 数据库中执行
func (db *Connector) doSampling(table, colDef, values string) error {
	// db.Database is hashed database name
	query := fmt.Sprintf("insert into `%s`.`%s` (%s) values %s;",
		Escape(db.Database, false),
		Escape(table, false),
		Escape(colDef, false), values)
	res, err := db.Query(query)
	if res.Rows != nil {
		res.Rows.Close()
	}
	return err
}

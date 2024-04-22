package main

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	"bitbucket.org/isbtotogroup/wigo_engine_backup_summary/db"
	"bitbucket.org/isbtotogroup/wigo_engine_backup_summary/helpers"
	"bitbucket.org/isbtotogroup/wigo_engine_backup_summary/models"
	"github.com/go-co-op/gocron"
	"github.com/joho/godotenv"
	"github.com/nleeper/goment"
)

func main() {
	local, err_local := time.LoadLocation("Asia/Jakarta")
	if err_local != nil {
		local = time.UTC
	}
	err := godotenv.Load()
	if err != nil {
		panic("Failed to load env file")
	}
	initRedis := helpers.RedisHealth()

	if !initRedis {
		panic("cannot load redis")
	}
	db.Init()
	envCompany := os.Getenv("DB_CONF_COMPANY")
	// envCurr := os.Getenv("DB_CONF_CURR")

	s := gocron.NewScheduler(local)

	s.Every(1).Day().At("01:00").Do(func() {
		fmt.Println("RUNNING 01:00 AM BACKUP DB")
		loop_backupdaily(envCompany)
	})
	s.StartBlocking()
}
func loop_backupdaily(idcompany string) {
	con := db.CreateCon()
	ctx := context.Background()
	tglnow, _ := goment.New()
	dateminus := tglnow.Add(-1, "days").Format("YYYY-MM-DD")
	startdate := dateminus + " 00:00:00"
	enddate := dateminus + " 23:59:59"
	flag := false

	tglrecord := strings.Split(startdate, " ")
	perioderecord := tglrecord[0]
	idrecord := strings.Replace(string(perioderecord), "-", "", -1)
	fmt.Println("idrecord : " + idrecord)
	fmt.Println("Periode : " + perioderecord)
	fmt.Println(startdate)
	fmt.Println(enddate)
	_, tbl_trx_transaksi, _, tbl_trx_transaksi_summarydaily, _ := models.Get_mappingdatabase(idcompany)

	sql_select_parent := `SELECT 
		sum(total_bet) as total_bet,
		sum(total_win) as total_win 
		FROM ` + tbl_trx_transaksi + `  
		WHERE createdate_transaksi >='` + startdate + `'  
		AND createdate_transaksi <='` + enddate + `'   
		`

	row, err := con.QueryContext(ctx, sql_select_parent)
	helpers.ErrorCheck(err)
	for row.Next() {
		var (
			total_bet_db, total_win_db int
		)

		err = row.Scan(&total_bet_db, &total_win_db)
		helpers.ErrorCheck(err)

		// fmt.Println(total_bet_db)
		// fmt.Println(total_win_db)

		flag = models.CheckDB(tbl_trx_transaksi_summarydaily, "idtransaksidaily", idrecord)
		if !flag {
			sql_insert_summarydaily := `
				insert into
				` + tbl_trx_transaksi_summarydaily + ` (
					idtransaksidaily , periodetransaksidaily,
					total_betdaily, total_windaily,
					create_transaksidaily, createdate_transaksidaily
				) values (
					$1, $2,
					$3, $4,
					$5, $6
				)
			`

			flag_insert, msg_insert := models.Exec_SQL(sql_insert_summarydaily, tbl_trx_transaksi_summarydaily, "INSERT",
				idrecord, perioderecord,
				total_bet_db, total_win_db,
				"SYSTEM", tglnow.Format("YYYY-MM-DD HH:mm:ss"))

			if flag_insert {
				fmt.Println(msg_insert)
			} else {
				fmt.Println(msg_insert)
			}
		} else {
			sql_update_summarydaily := `
				UPDATE 
				` + tbl_trx_transaksi_summarydaily + `  
				SET total_betdaily=$1, total_windaily=$2, 
				update_transaksidaily=$3, updatedate_transaksidaily=$4    
				WHERE idtransaksidaily=$5    
			`

			flag_update, msg_update := models.Exec_SQL(sql_update_summarydaily, tbl_trx_transaksi_summarydaily, "UPDATE",
				total_bet_db, total_win_db,
				"SYSTEM", tglnow.Format("YYYY-MM-DD HH:mm:ss"), idrecord)

			if flag_update {
				fmt.Println(msg_update)
			} else {
				fmt.Println(msg_update)
			}
		}

	}

	Fieldtransaksi2d30s_summarydaily_redis := "AGEN:12D30S:LISTINVOICE:SUMMARYDAILY"
	key_redis_invoicesummarydaily := strings.ToLower(idcompany) + ":" + Fieldtransaksi2d30s_summarydaily_redis + "_0_"
	val_invoicesummarydaily_agen := helpers.DeleteRedis(key_redis_invoicesummarydaily)
	fmt.Println("")
	fmt.Printf("Redis Delete AGEN INVOICE SUMMARY DAILY : %d - %s \r", val_invoicesummarydaily_agen, key_redis_invoicesummarydaily)
	fmt.Println("")
	defer row.Close()

}

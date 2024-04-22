package helpers

import (
	"math/rand"
)

type nomor_result_data struct {
	nomor_id         string
	nomor_value      string
	nomor_flag       bool
	nomor_css        string
	nomor_gangen     string
	nomor_besarkecil string
	nomor_line       string
	nomor_zona       string
	nomor_redblack   string
}

func Shuffle_nomor() string {
	var cards = []nomor_result_data{
		{nomor_id: "01", nomor_value: "01", nomor_zona: "ZONA_A", nomor_flag: false, nomor_css: "btn btn-error", nomor_gangen: "GANJIL", nomor_besarkecil: "KECIL", nomor_line: "LINE1", nomor_redblack: "RED"},
		{nomor_id: "04", nomor_value: "04", nomor_zona: "ZONA_A", nomor_flag: false, nomor_css: "btn", nomor_gangen: "GENAP", nomor_besarkecil: "KECIL", nomor_line: "LINE2", nomor_redblack: "BLACK"},
		{nomor_id: "07", nomor_value: "07", nomor_zona: "ZONA_A", nomor_flag: false, nomor_css: "btn btn-error", nomor_gangen: "GANJIL", nomor_besarkecil: "BESAR", nomor_line: "LINE3", nomor_redblack: "RED"},
		{nomor_id: "10", nomor_value: "10", nomor_zona: "ZONA_A", nomor_flag: false, nomor_css: "btn", nomor_gangen: "GENAP", nomor_besarkecil: "BESAR", nomor_line: "LINE4", nomor_redblack: "BLACK"},
		{nomor_id: "02", nomor_value: "02", nomor_zona: "ZONA_B", nomor_flag: false, nomor_css: "btn btn-error", nomor_gangen: "GENAP", nomor_besarkecil: "KECIL", nomor_line: "LINE1", nomor_redblack: "RED"},
		{nomor_id: "05", nomor_value: "05", nomor_zona: "ZONA_B", nomor_flag: false, nomor_css: "btn", nomor_gangen: "GANJIL", nomor_besarkecil: "KECIL", nomor_line: "LINE2", nomor_redblack: "BLACK"},
		{nomor_id: "08", nomor_value: "08", nomor_zona: "ZONA_B", nomor_flag: false, nomor_css: "btn btn-error", nomor_gangen: "GENAP", nomor_besarkecil: "BESAR", nomor_line: "LINE3", nomor_redblack: "RED"},
		{nomor_id: "11", nomor_value: "11", nomor_zona: "ZONA_B", nomor_flag: false, nomor_css: "btn", nomor_gangen: "GANJIL", nomor_besarkecil: "BESAR", nomor_line: "LINE4", nomor_redblack: "BLACK"},
		{nomor_id: "03", nomor_value: "03", nomor_zona: "ZONA_C", nomor_flag: false, nomor_css: "btn btn-error", nomor_gangen: "GANJIL", nomor_besarkecil: "KECIL", nomor_line: "LINE2", nomor_redblack: "RED"},
		{nomor_id: "06", nomor_value: "06", nomor_zona: "ZONA_C", nomor_flag: false, nomor_css: "btn", nomor_gangen: "GENAP", nomor_besarkecil: "KECIL", nomor_line: "LINE2", nomor_redblack: "BLACK"},
		{nomor_id: "09", nomor_value: "09", nomor_zona: "ZONA_C", nomor_flag: false, nomor_css: "btn btn-error", nomor_gangen: "GANJIL", nomor_besarkecil: "BESAR", nomor_line: "LINE3", nomor_redblack: "RED"},
		{nomor_id: "12", nomor_value: "12", nomor_zona: "ZONA_C", nomor_flag: false, nomor_css: "btn", nomor_gangen: "GENAP", nomor_besarkecil: "BESAR", nomor_line: "LINE4", nomor_redblack: "BLACK"},
		{nomor_id: "01", nomor_value: "01", nomor_zona: "ZONA_A", nomor_flag: false, nomor_css: "btn btn-error", nomor_gangen: "GANJIL", nomor_besarkecil: "KECIL", nomor_line: "LINE1", nomor_redblack: "RED"},
		{nomor_id: "04", nomor_value: "04", nomor_zona: "ZONA_A", nomor_flag: false, nomor_css: "btn", nomor_gangen: "GENAP", nomor_besarkecil: "KECIL", nomor_line: "LINE2", nomor_redblack: "BLACK"},
		{nomor_id: "07", nomor_value: "07", nomor_zona: "ZONA_A", nomor_flag: false, nomor_css: "btn btn-error", nomor_gangen: "GANJIL", nomor_besarkecil: "BESAR", nomor_line: "LINE3", nomor_redblack: "RED"},
		{nomor_id: "10", nomor_value: "10", nomor_zona: "ZONA_A", nomor_flag: false, nomor_css: "btn", nomor_gangen: "GENAP", nomor_besarkecil: "BESAR", nomor_line: "LINE4", nomor_redblack: "BLACK"},
		{nomor_id: "02", nomor_value: "02", nomor_zona: "ZONA_B", nomor_flag: false, nomor_css: "btn btn-error", nomor_gangen: "GENAP", nomor_besarkecil: "KECIL", nomor_line: "LINE1", nomor_redblack: "RED"},
		{nomor_id: "05", nomor_value: "05", nomor_zona: "ZONA_B", nomor_flag: false, nomor_css: "btn", nomor_gangen: "GANJIL", nomor_besarkecil: "KECIL", nomor_line: "LINE2", nomor_redblack: "BLACK"},
		{nomor_id: "08", nomor_value: "08", nomor_zona: "ZONA_B", nomor_flag: false, nomor_css: "btn btn-error", nomor_gangen: "GENAP", nomor_besarkecil: "BESAR", nomor_line: "LINE3", nomor_redblack: "RED"},
		{nomor_id: "11", nomor_value: "11", nomor_zona: "ZONA_B", nomor_flag: false, nomor_css: "btn", nomor_gangen: "GANJIL", nomor_besarkecil: "BESAR", nomor_line: "LINE4", nomor_redblack: "BLACK"},
		{nomor_id: "03", nomor_value: "03", nomor_zona: "ZONA_C", nomor_flag: false, nomor_css: "btn btn-error", nomor_gangen: "GANJIL", nomor_besarkecil: "KECIL", nomor_line: "LINE2", nomor_redblack: "RED"},
		{nomor_id: "06", nomor_value: "06", nomor_zona: "ZONA_C", nomor_flag: false, nomor_css: "btn", nomor_gangen: "GENAP", nomor_besarkecil: "KECIL", nomor_line: "LINE2", nomor_redblack: "BLACK"},
		{nomor_id: "09", nomor_value: "09", nomor_zona: "ZONA_C", nomor_flag: false, nomor_css: "btn btn-error", nomor_gangen: "GANJIL", nomor_besarkecil: "BESAR", nomor_line: "LINE3", nomor_redblack: "RED"},
		{nomor_id: "12", nomor_value: "12", nomor_zona: "ZONA_C", nomor_flag: false, nomor_css: "btn", nomor_gangen: "GENAP", nomor_besarkecil: "BESAR", nomor_line: "LINE4", nomor_redblack: "BLACK"},
		{nomor_id: "01", nomor_value: "01", nomor_zona: "ZONA_A", nomor_flag: false, nomor_css: "btn btn-error", nomor_gangen: "GANJIL", nomor_besarkecil: "KECIL", nomor_line: "LINE1", nomor_redblack: "RED"},
		{nomor_id: "04", nomor_value: "04", nomor_zona: "ZONA_A", nomor_flag: false, nomor_css: "btn", nomor_gangen: "GENAP", nomor_besarkecil: "KECIL", nomor_line: "LINE2", nomor_redblack: "BLACK"},
		{nomor_id: "07", nomor_value: "07", nomor_zona: "ZONA_A", nomor_flag: false, nomor_css: "btn btn-error", nomor_gangen: "GANJIL", nomor_besarkecil: "BESAR", nomor_line: "LINE3", nomor_redblack: "RED"},
		{nomor_id: "10", nomor_value: "10", nomor_zona: "ZONA_A", nomor_flag: false, nomor_css: "btn", nomor_gangen: "GENAP", nomor_besarkecil: "BESAR", nomor_line: "LINE4", nomor_redblack: "BLACK"},
		{nomor_id: "02", nomor_value: "02", nomor_zona: "ZONA_B", nomor_flag: false, nomor_css: "btn btn-error", nomor_gangen: "GENAP", nomor_besarkecil: "KECIL", nomor_line: "LINE1", nomor_redblack: "RED"},
		{nomor_id: "05", nomor_value: "05", nomor_zona: "ZONA_B", nomor_flag: false, nomor_css: "btn", nomor_gangen: "GANJIL", nomor_besarkecil: "KECIL", nomor_line: "LINE2", nomor_redblack: "BLACK"},
		{nomor_id: "08", nomor_value: "08", nomor_zona: "ZONA_B", nomor_flag: false, nomor_css: "btn btn-error", nomor_gangen: "GENAP", nomor_besarkecil: "BESAR", nomor_line: "LINE3", nomor_redblack: "RED"},
		{nomor_id: "11", nomor_value: "11", nomor_zona: "ZONA_B", nomor_flag: false, nomor_css: "btn", nomor_gangen: "GANJIL", nomor_besarkecil: "BESAR", nomor_line: "LINE4", nomor_redblack: "BLACK"},
		{nomor_id: "03", nomor_value: "03", nomor_zona: "ZONA_C", nomor_flag: false, nomor_css: "btn btn-error", nomor_gangen: "GANJIL", nomor_besarkecil: "KECIL", nomor_line: "LINE2", nomor_redblack: "RED"},
		{nomor_id: "06", nomor_value: "06", nomor_zona: "ZONA_C", nomor_flag: false, nomor_css: "btn", nomor_gangen: "GENAP", nomor_besarkecil: "KECIL", nomor_line: "LINE2", nomor_redblack: "BLACK"},
		{nomor_id: "09", nomor_value: "09", nomor_zona: "ZONA_C", nomor_flag: false, nomor_css: "btn btn-error", nomor_gangen: "GANJIL", nomor_besarkecil: "BESAR", nomor_line: "LINE3", nomor_redblack: "RED"},
		{nomor_id: "12", nomor_value: "12", nomor_zona: "ZONA_C", nomor_flag: false, nomor_css: "btn", nomor_gangen: "GENAP", nomor_besarkecil: "BESAR", nomor_line: "LINE4", nomor_redblack: "BLACK"},
		{nomor_id: "01", nomor_value: "01", nomor_zona: "ZONA_A", nomor_flag: false, nomor_css: "btn btn-error", nomor_gangen: "GANJIL", nomor_besarkecil: "KECIL", nomor_line: "LINE1", nomor_redblack: "RED"},
		{nomor_id: "04", nomor_value: "04", nomor_zona: "ZONA_A", nomor_flag: false, nomor_css: "btn", nomor_gangen: "GENAP", nomor_besarkecil: "KECIL", nomor_line: "LINE2", nomor_redblack: "BLACK"},
		{nomor_id: "07", nomor_value: "07", nomor_zona: "ZONA_A", nomor_flag: false, nomor_css: "btn btn-error", nomor_gangen: "GANJIL", nomor_besarkecil: "BESAR", nomor_line: "LINE3", nomor_redblack: "RED"},
		{nomor_id: "10", nomor_value: "10", nomor_zona: "ZONA_A", nomor_flag: false, nomor_css: "btn", nomor_gangen: "GENAP", nomor_besarkecil: "BESAR", nomor_line: "LINE4", nomor_redblack: "BLACK"},
		{nomor_id: "02", nomor_value: "02", nomor_zona: "ZONA_B", nomor_flag: false, nomor_css: "btn btn-error", nomor_gangen: "GENAP", nomor_besarkecil: "KECIL", nomor_line: "LINE1", nomor_redblack: "RED"},
		{nomor_id: "05", nomor_value: "05", nomor_zona: "ZONA_B", nomor_flag: false, nomor_css: "btn", nomor_gangen: "GANJIL", nomor_besarkecil: "KECIL", nomor_line: "LINE2", nomor_redblack: "BLACK"},
		{nomor_id: "08", nomor_value: "08", nomor_zona: "ZONA_B", nomor_flag: false, nomor_css: "btn btn-error", nomor_gangen: "GENAP", nomor_besarkecil: "BESAR", nomor_line: "LINE3", nomor_redblack: "RED"},
		{nomor_id: "11", nomor_value: "11", nomor_zona: "ZONA_B", nomor_flag: false, nomor_css: "btn", nomor_gangen: "GANJIL", nomor_besarkecil: "BESAR", nomor_line: "LINE4", nomor_redblack: "BLACK"},
		{nomor_id: "03", nomor_value: "03", nomor_zona: "ZONA_C", nomor_flag: false, nomor_css: "btn btn-error", nomor_gangen: "GANJIL", nomor_besarkecil: "KECIL", nomor_line: "LINE2", nomor_redblack: "RED"},
		{nomor_id: "06", nomor_value: "06", nomor_zona: "ZONA_C", nomor_flag: false, nomor_css: "btn", nomor_gangen: "GENAP", nomor_besarkecil: "KECIL", nomor_line: "LINE2", nomor_redblack: "BLACK"},
		{nomor_id: "09", nomor_value: "09", nomor_zona: "ZONA_C", nomor_flag: false, nomor_css: "btn btn-error", nomor_gangen: "GANJIL", nomor_besarkecil: "BESAR", nomor_line: "LINE3", nomor_redblack: "RED"},
		{nomor_id: "12", nomor_value: "12", nomor_zona: "ZONA_C", nomor_flag: false, nomor_css: "btn", nomor_gangen: "GENAP", nomor_besarkecil: "BESAR", nomor_line: "LINE4", nomor_redblack: "BLACK"},
		{nomor_id: "JP", nomor_value: "JP", nomor_flag: false, nomor_css: "btn", nomor_gangen: "GANJIL", nomor_besarkecil: "BESAR", nomor_line: "LINE3", nomor_redblack: "RED"}}
	min := 0
	max := len(cards)
	var n = rand.Intn(max-min) + min
	return cards[n].nomor_id
}

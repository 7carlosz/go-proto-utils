package utils

import (
	"database/sql"
	"fmt"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"time"
	"unicode"

	field_mask "google.golang.org/genproto/protobuf/field_mask"
)

type Pageable struct {
	Offset int64
	Limit  int64
	Sort   string
}

type Retorno struct {
	Djcp string
}

func Init() {

}

func ValidateFormatDate(str1 string) bool {

	re := regexp.MustCompile("((19|20)\\d\\d)-(0?[1-9]|1[012])-(0?[1-9]|[12][0-9]|3[01])")

	if str1 != "" {
		return re.MatchString(str1)
	}
	return false

}

func ValidateFormatHours(str1 string) bool {

	re := regexp.MustCompile("^([0-1]?[0-9]|2[0-3]):[0-5][0-9]:[0-5][0-9]$")

	if str1 != "" {
		return re.MatchString(str1)
	}
	return false

}
func BuildSelect(req interface{}) (string, []string) {
	val := reflect.Indirect(reflect.ValueOf(req))
	var selectString string = ""
	selectArray := make([]string, 0)
	for i := 0; i < reflect.ValueOf(req).Elem().NumField()-3; i++ {
		var field = val.Type().Field(i).Name
		selectString = selectString + ", " + convertFiledNameColumn(field)
		selectArray = append(selectArray, field)
	}
	return selectString[1:], selectArray
}

//var call util.NewEntityInterface = NewEntity{}
func BuildWherePageable(req interface{}, isLike bool) (string, []interface{}, string, string) {

	pageable := ConvertPageable(req)
	val := reflect.Indirect(reflect.ValueOf(req))
	var where string = ""
	var index int = 0
	var count int = 0
	for i := 0; i < reflect.ValueOf(req).Elem().NumField()-3; i++ {
		var field = val.Type().Field(i).Name

		if field != "Offset" && field != "Limit" && field != "Sort" {
			var fieldData = reflect.ValueOf(req).Elem().FieldByName(field)
			if fieldData.IsValid() && !fieldData.IsNil() {
				var data = fieldData.Interface().(*field_mask.FieldMask)
				if data.Paths[0] != "[null]" {
					count++
				}
			}

		}

	}
	vals := make([]interface{}, count+2)

	for i := 0; i < reflect.ValueOf(req).Elem().NumField()-3; i++ {

		var field = val.Type().Field(i).Name
		var fieldData = reflect.ValueOf(req).Elem().FieldByName(field)
		if field != "Offset" && field != "Limit" && field != "Sort" {
			if fieldData.IsValid() && !fieldData.IsNil() {
				var data = fieldData.Interface().(*field_mask.FieldMask)
				if data.Paths[0] == "[null]" {
					where = where + convertFiledNameColumn(field) + " is null and "
				} else {
					if isLike {
						vals[index] = "%" + strings.ToUpper(data.Paths[0]) + "%"
						where = where + "upper(CAST (" + convertFiledNameColumn(field) + " as VARCHAR)      )" + " like $" + strconv.Itoa(index+1) + " and "
					} else {
						vals[index] = data.Paths[0]
						where = where + convertFiledNameColumn(field) + " = $" + strconv.Itoa(index+1) + " and "
					}
					index++
				}
			}

		}

	}

	var order = " order by " + strings.ReplaceAll(pageable.Sort, "[concat]", ",")
	if pageable.Sort == "default" {
		order = " order by 1"
	}

	var limitOrder = " limit $" + strconv.Itoa(len(vals)-1) + " offset $" + strconv.Itoa(len(vals))
	vals[len(vals)-2] = pageable.Limit
	vals[len(vals)-1] = pageable.Offset

	if where != "" {
		where = " where " + where[:len(where)-4]
	}

	return where, vals, order, limitOrder
}

func BuildCreate(entity interface{}) (string, string, []interface{}) {

	val := reflect.Indirect(reflect.ValueOf(entity))
	var columnsString = ""
	var valueReferenceString = ""

	var count int = 0
	for i := 0; i < reflect.ValueOf(entity).Elem().NumField()-3; i++ {

		var fieldData = reflect.ValueOf(entity).Elem().FieldByName(val.Type().Field(i).Name)
		if !strings.EqualFold(val.Type().Field(i).Name, "id") && fieldData.IsValid() && fieldData.String() != "" {
			count++
		}

	}

	vals := make([]interface{}, count)
	var index int = 0
	for i := 0; i < reflect.ValueOf(entity).Elem().NumField()-3; i++ {

		var field = val.Type().Field(i).Name
		var fieldData = reflect.ValueOf(entity).Elem().FieldByName(field)
		if !strings.EqualFold(field, "id") && fieldData.IsValid() && fieldData.String() != "" {
			field = convertFiledNameColumn(field)
			vals[index] = fieldData.String()
			columnsString = columnsString + ", " + field
			valueReferenceString = valueReferenceString + ", $" + strconv.Itoa(index+1)
			index++
		}

	}
	if columnsString != "" {
		return columnsString[1:], valueReferenceString[1:], vals
	} else {
		return "", "", vals
	}

}

func BuildUpdate(entity interface{}) (string, []interface{}) {

	val := reflect.Indirect(reflect.ValueOf(entity))
	var setString = ""

	var count int = 0
	for i := 0; i < reflect.ValueOf(entity).Elem().NumField()-3; i++ {

		var fieldData = reflect.ValueOf(entity).Elem().FieldByName(val.Type().Field(i).Name)
		if fieldData.IsValid() && fieldData.String() != "" {

			count++
		}

	}

	vals := make([]interface{}, count)
	var index int = 1
	var dataId int64 = 0
	for i := 0; i < reflect.ValueOf(entity).Elem().NumField()-3; i++ {

		var field = val.Type().Field(i).Name
		var fieldData = reflect.ValueOf(entity).Elem().FieldByName(field)
		if !strings.EqualFold(field, "id") && fieldData.IsValid() && fieldData.String() != "" {
			field = convertFiledNameColumn(field)

			if fieldData.String() == "[null]" {
				vals[index] = nil
			} else if fieldData.String() == "[clear]" {
				vals[index] = ""
			} else {
				vals[index] = fieldData.String()
			}

			setString = setString + ", " + field + "  = $" + strconv.Itoa(index+1)
			index++
		} else if strings.EqualFold(field, "id") {
			dataId = fieldData.Int()

		}

	}

	vals[0] = dataId
	if len(setString) < 1 {
		return "", nil
	}

	return setString[1:], vals
}

func convertFiledNameColumn(field string) string {
	var ret = ""
	for pos, char := range field {
		_ = pos
		_ = char
		if pos < 1 {
			ret = ret + strings.ToLower(string(char))
		} else {
			if unicode.IsUpper(char) {
				ret = ret + "_" + strings.ToLower(string(char))
			} else {
				ret = ret + string(char)

			}

		}
	}

	return ret
}

func FormatDate(str1 string) string {

	i := strings.Index(str1, "T")

	if i > -1 {
		t, err := time.Parse(time.RFC3339, str1)
		if err != nil {
			fmt.Println("error parseo forma date ")
			fmt.Println(err)
		}
		return t.Format("2006-01-02")
	} else {
		i := strings.Index(str1, "/")
		if i > -1 {
			dt, _ := time.Parse("01/02/2006", str1)
			return dt.Format("2006-01-02")
		} else {
			dt, _ := time.Parse("01-02-2006", str1)
			if dt.Format("2006-01-02") == "0001-01-01" {
				dt, _ := time.Parse("2006-02-01", str1)
				return dt.Format("2006-01-02")
			} else {
				return dt.Format("2006-01-02")
			}

		}
	}

}

func FormatHours(str1 string) string {
	i := strings.Index(str1, "T")

	if i > -1 {
		t, err := time.Parse(time.RFC3339, str1)
		if err != nil {
			fmt.Println("error parseo forma date ")
			fmt.Println(err)
		}
		return t.Format("15:04:05")
	} else {
		i := strings.Index(str1, "/")
		if i > -1 {
			dt, _ := time.Parse("01/02/2006 15:04:05", str1)
			return dt.Format("15:04:05")
		} else {
			dt, _ := time.Parse("01-02-2006 15:04:05", str1)
			if dt.Format("15:04:05") == "00:00:00" {
				dt, _ := time.Parse("2006-02-01 15:04:05", str1)
				return dt.Format("15:04:05")
			} else {
				return dt.Format("15:04:05")
			}

		}
	}

}

func FormatDateHours(str1 string) string {
	i := strings.Index(str1, "T")

	if i > -1 {
		t, err := time.Parse(time.RFC3339, str1)
		if err != nil {
			fmt.Println("error parseo forma date ")
			fmt.Println(err)
		}
		return t.Format("2006-01-02 15:04:05")
	} else {

		i := strings.Index(str1, "/")
		if i > -1 {
			dt, _ := time.Parse("01/02/2006 15:04:05", str1)
			return dt.Format("2006-01-02 15:04:05")
		} else {
			dt, _ := time.Parse("01-02-2006 15:04:05", str1)
			if dt.Format("2006-01-02 15:04:05") == "0001-01-01 00:00:00" {
				dt, _ := time.Parse("2006-02-01 15:04:05", str1)
				return dt.Format("2006-01-02 15:04:05")
			} else {
				return dt.Format("2006-01-02 15:04:05")
			}

		}
	}
}

func NewNullString(s string) sql.NullString {

	if s == "" {
		return sql.NullString{}
	}
	return sql.NullString{
		String: s,
		Valid:  true,
	}
}

func NewNullInt64(s string) sql.NullInt64 {

	if s == "" {
		return sql.NullInt64{}
	}
	var intVal, err = strconv.Atoi(s)

	if err != nil {
		return sql.NullInt64{}
	}

	return sql.NullInt64{
		Int64: int64(intVal),
		Valid: true,
	}

}

func AddField_mask(val string) *field_mask.FieldMask {
	var x []string
	var s = new(field_mask.FieldMask)
	s.Paths = append(x, val)
	return s
}

func emptyInterface(interf interface{}, listColumn []string) interface{} {

	v := reflect.ValueOf(interf).Elem()
	for index := 0; index < len(listColumn); index++ {
		ptr := v.FieldByName(listColumn[index])
		if ptr.Type().String() == "int64" {
			ptr.Set(reflect.ValueOf(int64(0)))
		} else {

			ptr.Set(reflect.ValueOf(""))

		}

	}

	return interf
}

func ScanData(interf interface{}, rows *sql.Rows, listColumn []string, dateValidate, hourValidate, dateHourValidate string) interface{} {

	var listDates = splitString(dateValidate)
	var listHours = splitString(hourValidate)
	var listDateHour = splitString(dateHourValidate)

	listRec := make([]interface{}, 0)
	for i := 0; i < len(listColumn); i++ {

		var temp sql.NullString
		listRec = append(listRec, &temp)
	}

	if err := rows.Scan(listRec...); err != nil {
		fmt.Println("failed to retrieve field values from  row-> " + err.Error())
	}
	interf = emptyInterface(interf, listColumn)
	v := reflect.ValueOf(interf).Elem()

	for index := 0; index < len(listColumn); index++ {

		s, _ := listRec[index].(*sql.NullString)

		if s.Valid {
			//fmt.Println("es valido" + listColumn[index])
			ptr := v.FieldByName(listColumn[index])
			if ptr.Type().String() == "int64" {
				data, err := strconv.Atoi(s.String)
				if err != nil {
					fmt.Println(err)
				}
				ptr.Set(reflect.ValueOf(int64(data)))
			} else {

				if stringInSlice(listColumn[index], listDates) {
					ptr.Set(reflect.ValueOf(FormatDate(s.String)))
				} else if stringInSlice(listColumn[index], listHours) {
					ptr.Set(reflect.ValueOf(FormatHours(s.String)))
				} else if stringInSlice(listColumn[index], listDateHour) {
					ptr.Set(reflect.ValueOf(FormatDateHours(s.String)))
				} else {
					//fmt.Println(s.String)
					ptr.Set(reflect.ValueOf(s.String))
				}

			}

		}
	}
	return interf
}

func BuildWhere(req interface{}) (string, []interface{}) {

	val := reflect.Indirect(reflect.ValueOf(req))
	var where string = ""
	var index int = 0
	var count int = 0
	for i := 0; i < reflect.ValueOf(req).Elem().NumField()-3; i++ {
		var field = val.Type().Field(i).Name
		var fieldData = reflect.ValueOf(req).Elem().FieldByName(field)
		if fieldData.IsValid() && !fieldData.IsNil() {
			var data = fieldData.Interface().(*field_mask.FieldMask)
			if data.Paths[0] != "[null]" {
				count++
			}
		}

	}
	vals := make([]interface{}, count)

	for i := 0; i < reflect.ValueOf(req).Elem().NumField()-3; i++ {

		var field = val.Type().Field(i).Name
		var fieldData = reflect.ValueOf(req).Elem().FieldByName(field)
		if fieldData.IsValid() && !fieldData.IsNil() {
			var data = fieldData.Interface().(*field_mask.FieldMask)
			if data.Paths[0] == "[null]" {
				where = where + convertFiledNameColumn(field) + " is null and "
			} else {
				vals[index] = data.Paths[0]
				where = where + convertFiledNameColumn(field) + " = $" + strconv.Itoa(index+1) + " and "
				index++
			}
		}

	}

	if where != "" {
		where = " where " + where[:len(where)-4]
	}

	return where, vals
}

func splitString(dat string) []string {
	var ret = make([]string, 0)
	var temp = make([]string, 0)
	if dat != "" {
		temp = strings.SplitAfter(dat, ",")
	}

	for index := 0; index < len(temp); index++ {
		ret = append(ret, strings.Replace(temp[index], ",", "", -1))

	}

	return ret
}

func stringInSlice(a string, list []string) bool {

	for index := 0; index < len(list); index++ {

		if a == list[index] {
			return true
		}
	}

	return false
}

func ValidateFechas(interf interface{}, date, hour, dateHour string) string {

	var listDates = splitString(date)
	var listHours = splitString(hour)

	val := reflect.Indirect(reflect.ValueOf(interf))

	for i := 0; i < reflect.ValueOf(interf).Elem().NumField()-3; i++ {
		var field = val.Type().Field(i).Name
		var fieldData = reflect.ValueOf(interf).Elem().FieldByName(field)
		if field != "Offset" && field != "Limit" && field != "Sort" {

			if !strings.EqualFold(field, "id") && fieldData.IsValid() && fieldData.String() != "" {

				if stringInSlice(field, listDates) && !ValidateFormatDate(fieldData.String()) {
					return "Formato fecha invalida para fecha : yyyy-mm-dd "
				}

				if stringInSlice(field, listHours) && !ValidateFormatHours(fieldData.String()) {
					return "Formato fecha invalida para fecha : HH:mm:ss "
				}

			}

		}

	}

	return ""
}
func ConvertPageable(interf interface{}) Pageable {
	var pageable = Pageable{Offset: 0, Limit: 50, Sort: "default"}

	count := reflect.ValueOf(interf).Elem().NumField()
	val := reflect.Indirect(reflect.ValueOf(interf))
	for i := 0; i < count; i++ {
		var field = val.Type().Field(i).Name
		var fieldData = reflect.ValueOf(interf).Elem().FieldByName(field)

		if fieldData.IsValid() {

			if field == "Offset" {
				//data.Paths[0]
				pageable.Offset = GetDataPageableInt(fieldData.Interface().(*field_mask.FieldMask), pageable.Offset)
			} else if field == "Limit" {
				pageable.Limit = GetDataPageableInt(fieldData.Interface().(*field_mask.FieldMask), pageable.Limit)
			} else if field == "Sort" {
				pageable.Sort = GetDataPageableString(fieldData.Interface().(*field_mask.FieldMask), pageable.Sort)
			}

		}

	}

	if pageable.Limit == 0 {
		pageable.Limit = 50
	}
	return pageable
}

func GetDataPageableInt(mask *field_mask.FieldMask, defaultData int64) int64 {

	if mask != nil && mask.Paths[0] != "" {
		data, err := strconv.Atoi(mask.Paths[0])
		if err != nil {
			// handle error
			fmt.Println(err)
		}
		defaultData = int64(data)
	}

	return defaultData
}

func GetDataPageableString(mask *field_mask.FieldMask, defaultData string) string {

	if mask != nil && mask.Paths[0] != "" {
		defaultData = mask.Paths[0]
	}

	return defaultData
}

func GetFields(interf interface{}) []string {
	var ret = make([]string, 0)

	count := reflect.ValueOf(interf).Elem().NumField()
	val := reflect.Indirect(reflect.ValueOf(interf))
	for i := 0; i < count; i++ {
		var field = val.Type().Field(i).Name
		ret = append(ret, field)

	}
	return ret
}

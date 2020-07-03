package v1

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"reflect"
	"strings"
	"unicode"

	utils "github.com/7carlosz/go-proto-utils/utils"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func Init() {

}

func CoreReadBySearchLikeCustom(query string, req interface{}, entity interface{}, ctx context.Context, c *sql.Conn, dateValidate, hourValidate, dateHourValidate string, tabla string) ([]interface{}, error) {

	where, vals, order, limitOrder := utils.BuildWherePageable(req, true)
	_, selectArray := utils.BuildSelect(entity)

	query = query + where + " " + order + " " + limitOrder
	log.Println(vals)
	rows, err := c.QueryContext(ctx, query,
		vals...,
	)
	if err != nil {
		return nil, status.Error(codes.Unknown, "failed to select -> "+err.Error())
	}
	defer rows.Close()

	list := TraducirRespuestaListCore(entity, rows, selectArray, dateValidate, hourValidate, dateHourValidate)
	if len(list) < 1 {
		return nil, status.Error(codes.NotFound, "Recurso no encontrado")
	}
	return list, nil

}

func CoreReadCustom(query string, req interface{}, entity interface{}, ctx context.Context, c *sql.Conn, dateValidate, hourValidate, dateHourValidate string, tabla string) ([]interface{}, error) {

	_, vals := utils.BuildWhere(req)
	_, selectArray := utils.BuildSelect(entity)

	log.Println(query)
	rows, err := c.QueryContext(ctx, query,
		vals...,
	)
	if err != nil {
		return nil, status.Error(codes.Unknown, "failed to select -> "+err.Error())
	}
	defer rows.Close()

	list := TraducirRespuestaListCore(entity, rows, selectArray, dateValidate, hourValidate, dateHourValidate)
	if len(list) < 1 {
		return nil, status.Error(codes.NotFound, "Recurso no encontrado")
	}
	return list, nil
}

func CoreReadBySearch(req interface{}, entity interface{}, ctx context.Context, c *sql.Conn, dateValidate, hourValidate, dateHourValidate string, tabla string) ([]interface{}, error) {

	where, vals, order, limitOrder := utils.BuildWherePageable(req, false)
	selectString, selectArray := utils.BuildSelect(entity)

	var query string = "SELECT " + selectString + "	FROM  " + tabla + " " + where + " " + order + " " + limitOrder
	log.Println(query)
	rows, err := c.QueryContext(ctx, query,
		vals...,
	)
	if err != nil {
		return nil, status.Error(codes.Unknown, "failed to select -> "+err.Error())
	}
	defer rows.Close()

	list := TraducirRespuestaListCore(entity, rows, selectArray, dateValidate, hourValidate, dateHourValidate)
	if len(list) < 1 {
		return nil, status.Error(codes.NotFound, "Recurso no encontrado")
	}
	return list, nil
}

func remove(slice []interface{}, s int) []interface{} {
	ret := make([]interface{}, 0)

	for i := 0; i < len(slice); i++ {
		if s != i {
			ret = append(ret, slice[i])
		}

	}
	return ret
}

func CoreReadBySearchLike(req interface{}, entity interface{}, ctx context.Context, c *sql.Conn, dateValidate, hourValidate, dateHourValidate string, tabla string) ([]interface{}, error) {

	where, vals, order, limitOrder := utils.BuildWherePageable(req, true)
	selectString, selectArray := utils.BuildSelect(entity)

	var query string = "SELECT " + selectString + "	FROM  " + tabla + " " + where + " " + order + " " + limitOrder
	log.Println(query)
	rows, err := c.QueryContext(ctx, query,
		vals...,
	)
	if err != nil {
		return nil, status.Error(codes.Unknown, "failed to select -> "+err.Error())
	}
	defer rows.Close()

	list := TraducirRespuestaListCore(entity, rows, selectArray, dateValidate, hourValidate, dateHourValidate)
	if len(list) < 1 {
		return nil, status.Error(codes.NotFound, "Recurso no encontrado")
	}
	return list, nil
}

func CoreReadDistinctBySearch(disctintColumn string, req interface{}, entity interface{}, ctx context.Context, c *sql.Conn, dateValidate, hourValidate, dateHourValidate string, tabla string) ([]interface{}, error) {

	where, vals, _, limitOrder := utils.BuildWherePageable(req, false)

	query := "SELECT distinct " + disctintColumn + "	FROM  " + tabla + " " + where + " order by 1 " + limitOrder

	rows, err := c.QueryContext(ctx, query,
		vals...,
	)
	if err != nil {
		return nil, status.Error(codes.Unknown, "failed to select -> "+err.Error())
	}
	defer rows.Close()

	retColumnDistinct := make([]string, 0)
	retColumnDistinct = append(retColumnDistinct, disctintColumn)

	list := TraducirRespuestaListCore(new(utils.Retorno), rows, retColumnDistinct, dateValidate, hourValidate, dateHourValidate)
	if len(list) < 1 {
		return nil, status.Error(codes.NotFound, "Recurso no encontrado")
	}
	return list, nil
}

func CoreReadAll(entity interface{}, req interface{}, ctx context.Context, c *sql.Conn, dateValidate, hourValidate, dateHourValidate string, tabla string) ([]interface{}, error) {

	pageable := utils.ConvertPageable(req)
	selectString, selectArray := utils.BuildSelect(entity)
	var order = " order by " + strings.ReplaceAll(pageable.Sort, "[concat]", ",")
	if pageable.Sort == "default" {
		order = " order by 1"
	}

	query := " select " + selectString + "	FROM " + tabla + order + " limit $1 offset $2"
	log.Println(query)

	rows, err := c.QueryContext(ctx, query,
		pageable.Limit, pageable.Offset,
	)
	if err != nil {
		return nil, status.Error(codes.Unknown, "failed to select -> "+err.Error())
	}
	defer rows.Close()

	list := TraducirRespuestaListCore(entity, rows, selectArray, dateValidate, hourValidate, dateHourValidate)
	if len(list) < 1 {
		return nil, status.Error(codes.NotFound, "Recurso no encontrado")
	}
	return list, nil
}

func CoreQueryReadAll(query string, entity interface{}, ctx context.Context, c *sql.Conn, dateValidate, hourValidate, dateHourValidate string, tabla string) ([]interface{}, error) {
	_, selectArray := utils.BuildSelect(entity)
	log.Println(query)
	rows, err := c.QueryContext(ctx, query)
	if err != nil {
		return nil, status.Error(codes.Unknown, "failed to select -> "+err.Error())
	}
	defer rows.Close()

	list := TraducirRespuestaListCore(entity, rows, selectArray, dateValidate, hourValidate, dateHourValidate)
	if len(list) < 1 {
		return nil, status.Error(codes.NotFound, "Recurso no encontrado")
	}
	return list, nil
}

func CoreRead(ctx context.Context, c *sql.Conn, id int64, req interface{}, dateValidate, hourValidate, dateHourValidate string, tabla string) (interface{}, error) {
	rows, selectArray, err := CorebuscarPorId(ctx, c, id, req, tabla)

	if err != nil {
		return nil, status.Error(codes.Unknown, "failed to select  "+err.Error())
	}

	defer rows.Close()
	list := make([]interface{}, 0)
	list = TraducirRespuestaListCore(req, rows, selectArray, dateValidate, hourValidate, dateHourValidate)

	if len(list) > 0 {
		return list[0], nil
	} else {
		return nil, status.Error(codes.NotFound, "Recurso no encontrado")
	}

}

func CoreCreate(s interface{}, ctx context.Context, c *sql.Conn, req interface{}, dateValidate, hourValidate, dateHourValidate string, tabla string) (interface{}, error) {

	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "datos invalidos ")
	}

	var msg = utils.ValidateFechas(req, dateValidate, hourValidate, dateHourValidate)

	if msg != "" {
		return nil, status.Error(codes.Unknown, msg)
	}

	columnsString, valuesReference, values := utils.BuildCreate(req)

	if columnsString == "" {
		return nil, status.Error(codes.Unknown, "Datos invalidos-> ")
	}

	stmt, err := c.PrepareContext(ctx, "INSERT INTO "+tabla+" ("+columnsString+") VALUES("+valuesReference+") RETURNING id")

	if err != nil {
		log.Println(err.Error())
		return nil, status.Error(codes.Unknown, "failed to insert "+err.Error())
	}

	defer stmt.Close()
	var id int64

	err = stmt.QueryRow(values...,
	).Scan(&id)
	if err != nil {
		log.Println(err.Error())
		return nil, status.Error(codes.Unknown, "failed to insert "+err.Error())
	}

	rows, selectArray, err := CorebuscarPorId(ctx, c, id, req, tabla)

	if err != nil {
		return nil, status.Error(codes.Unknown, "failed to select  "+err.Error())
	}

	defer rows.Close()
	list := make([]interface{}, 0)
	list = TraducirRespuestaListCore(req, rows, selectArray, dateValidate, hourValidate, dateHourValidate)

	if len(list) > 0 {
		return list[0], nil
	} else {
		return nil, status.Error(codes.NotFound, "Recurso no encontrado")
	}

}

func CorebuscarPorId(ctx context.Context, c *sql.Conn, id int64, req interface{}, tabla string) (*sql.Rows, []string, error) {

	selectString, selectArray := utils.BuildSelect(req)
	var query string = "SELECT " + selectString + " FROM " + tabla + " WHERE ID=$1"

	rows, err := c.QueryContext(ctx, query,
		id)
	if err != nil {
		return nil, nil, err //status.Error(codes.Unknown, "failed to select -> "+)
	}

	return rows, selectArray, nil
}

func CoreUpdate(id int64, entity interface{}, ctx context.Context, c *sql.Conn, dateValidate, hourValidate, dateHourValidate string, tabla string) (interface{}, error) {

	setString, values := utils.BuildUpdate(entity)

	if setString == "" {
		return nil, status.Error(codes.Unknown, "Datos invalidos ")
	}

	res, err := c.ExecContext(ctx, "UPDATE "+tabla+" SET "+setString+" WHERE ID=$1",
		values...)
	if err != nil {
		return nil, status.Error(codes.Unknown, "failed to update "+err.Error())
	}

	rowsAffect, err := res.RowsAffected()
	if err != nil {
		return nil, status.Error(codes.Unknown, "failed to retrieve rows affected value-> "+err.Error())
	}

	if rowsAffect == 0 {
		return nil, status.Error(codes.NotFound, fmt.Sprintf("with ID='%d' is not found",
			id))
	}

	rows, selectArray, err := CorebuscarPorId(ctx, c, id, entity, tabla)

	if err != nil {
		return nil, status.Error(codes.Unknown, "failed to select  "+err.Error())
	}

	defer rows.Close()
	list := make([]interface{}, 0)
	list = TraducirRespuestaListCore(entity, rows, selectArray, dateValidate, hourValidate, dateHourValidate)

	if len(list) > 0 {
		return list[0], nil
	} else {
		return nil, status.Error(codes.NotFound, "Recurso no encontrado")
	}
}

func CoreDelete(id int64, ctx context.Context, c *sql.Conn, tabla string) (int64, error) {

	res, err := c.ExecContext(ctx, "DELETE FROM "+tabla+" WHERE ID=$1", id)
	if err != nil {
		return 0, status.Error(codes.Unknown, "failed to delete "+err.Error())
	}

	rows, err := res.RowsAffected()
	if err != nil {
		return 0, status.Error(codes.Unknown, "failed to retrieve rows affected value-> "+err.Error())
	}

	if rows == 0 {
		return 0, status.Error(codes.NotFound, "Recurso no encontrado")
	}

	return rows, nil
}

func nuevoInstance(i interface{}) interface{} {
	return reflect.Indirect(reflect.ValueOf(i)).Interface()
}

func TraducirRespuestaListCore(entity interface{}, rows *sql.Rows, listColumn []string, dateValidate, hourValidate, dateHourValidate string) []interface{} {
	list := make([]interface{}, 0)

	for rows.Next() {

		i := utils.ScanData(entity, rows, listColumn, dateValidate, hourValidate, dateHourValidate)
		list = append(list, nuevoInstance(i))
	}

	return list
}

func IsValidoQueryParam(req *http.Request, i interface{}) {

	queryParam := req.URL.Query()
	listFields := utils.GetFields(i)
	for i := 0; i < len(listFields)-3; i++ {

		for key, element := range queryParam {
			fmt.Println("Key:", key, "=>", "Element:", element)
		}
		fmt.Println("validando " + listFields[i])
	}
}

func isValidoNotQueryParam(req *http.Request) (bool, string) {
	param := ""
	queryParam := req.URL.Query()
	if len(queryParam) > 0 {
		for key, _ := range queryParam {
			param = key
			break
		}
		return false, "Parametro no esperado " + param
	} else {
		return true, param

	}

}

func isObjecBodyValido(bodyJson map[string]interface{}, i interface{}) (bool, string, []string) {
	ret := make([]string, 0)
	listFields := utils.GetFields(i)
	for key := range bodyJson {
		var existe bool = false
		for i := 0; i < len(listFields)-3; i++ {
			if convertNameField(key) == listFields[i] {
				existe = true
				ret = append(ret, key)
			}
		}
		if !existe {
			return false, key + " no es un parametro valido", ret
		}
	}
	return true, "", ret
}

func isObjecBodyInterfaceValido(bodyJson map[string]interface{}, i interface{}) (bool, string) {

	listFields := utils.GetFields(i)
	for key := range bodyJson {
		var existe bool = false
		for i := 0; i < len(listFields)-3; i++ {
			if convertNameField(key) == listFields[i] {
				existe = true
			}
		}
		if !existe {
			return false, key + " no es un parametro valido"
		}
	}
	return true, ""
}

func isValidoBody(req *http.Request, i interface{}) (bool, string) {
	buf, _ := ioutil.ReadAll(req.Body)
	rdr1 := ioutil.NopCloser(bytes.NewBuffer(buf))
	rdr2 := ioutil.NopCloser(bytes.NewBuffer(buf))

	req.Body = rdr2

	body, _ := ioutil.ReadAll(rdr1)
	if len(body) < 1 {
		return false, "Body incompleto"
	}
	var bodyJson map[string]interface{}
	json.Unmarshal(body, &bodyJson)
	ok, errMsg, listKey := isObjecBodyValido(bodyJson, i)
	if !ok {
		return false, errMsg
	}

	for _, keyBody := range listKey {
		Bodyobject := bodyJson[keyBody]

		for keyBodyobject, _ := range Bodyobject.(map[string]interface{}) {
			fmt.Println("key object object " + keyBodyobject)
			f := reflect.ValueOf(i).Elem().FieldByName(convertNameField(keyBody)).Type()

			existeKey := false
			for in3 := 0; in3 < f.Elem().NumField()-3; in3++ {

				if convertNameField(keyBodyobject) == f.Elem().Field(in3).Name {
					existeKey = true
				}

			}

			if !existeKey {
				return false, keyBodyobject + " no es un parametro valido"
			}

		}
	}

	return true, ""
}

func isValidoQueryParam(req *http.Request, i interface{}) (bool, string) {
	queryParam := req.URL.Query()
	listFields := utils.GetFields(i)
	for keyParam, _ := range queryParam {
		existe := false
		for _, fieldInterface := range listFields {
			if convertNameField(keyParam) == fieldInterface {
				existe = true
				break
			}
		}

		if !existe {
			return false, keyParam + " no es un parametro valido"
		}

	}

	return true, ""
}

func existeKeyObject(fieldName string, i interface{}) bool {
	var existe bool = false
	v := reflect.ValueOf(i).Elem()
	for in := 0; in < reflect.ValueOf(i).Elem().NumField()-3; in++ {
		var field = v.Type().Field(in).Name

		fmt.Println(field)
		fmt.Println("----------")
		fmt.Println(fieldName)

		if field == fieldName {
			existe = true
		}
	}
	return existe
}

func convertNameField(field string) string {
	var ret = ""
	for pos, char := range field {
		_ = pos
		_ = char
		if pos < 1 {
			if !unicode.IsUpper(char) {
				ret = ret + strings.ToUpper(string(char))
			}

		} else {
			ret = ret + string(char)

		}
	}

	return ret
}

func IsValidoCreate(req *http.Request, i interface{}) (bool, string) {
	ok, msgErr := isValidoBody(req, i)
	if !ok {
		return false, msgErr
	}

	ok, msgErr = isValidoNotQueryParam(req)
	if !ok {
		return false, msgErr
	}
	return true, msgErr
}

func IsValidoRead(req *http.Request) (bool, string) {

	ok, msgErr := isValidoNotQueryParam(req)
	return ok, msgErr

}

func IsValidoUpdate(req *http.Request, i interface{}) (bool, string) {

	ok, msgErr := isValidoBody(req, i)
	if !ok {
		return false, msgErr
	}

	ok, msgErr = isValidoNotQueryParam(req)
	if !ok {
		return false, msgErr
	}
	return true, msgErr

}

func IsValidoDelete(req *http.Request) (bool, string) {

	ok, msgErr := isValidoNotQueryParam(req)
	return ok, msgErr

}
func IsValidoReadAll(req *http.Request, i interface{}) (bool, string) {
	ok, msgErr := isValidoQueryParam(req, i)
	if !ok {
		return false, msgErr
	}

	return true, msgErr
}

func IsValidoReadBySearch(req *http.Request, i interface{}) (bool, string) {
	ok, msgErr := isValidoQueryParam(req, i)
	if !ok {
		return false, msgErr
	}

	return true, msgErr
}

func IsValidoReadBySearchLike(req *http.Request, i interface{}) (bool, string) {
	ok, msgErr := isValidoQueryParam(req, i)
	if !ok {
		return false, msgErr
	}

	return true, msgErr
}

func CoreCountBySearch(req interface{}, entity interface{}, ctx context.Context, c *sql.Conn, dateValidate, hourValidate, dateHourValidate string, tabla string) ([]interface{}, error) {

	where, vals, order, limitOrder := utils.BuildWherePageable(req, false)
	_, selectArray := utils.BuildSelect(entity)

	var query string = "SELECT count(*) total	FROM  " + tabla + " " + where + " " + order + " " + limitOrder

	rows, err := c.QueryContext(ctx, query,
		vals...,
	)
	if err != nil {
		return nil, status.Error(codes.Unknown, "failed to select -> "+err.Error())
	}
	defer rows.Close()

	list := TraducirRespuestaListCore(entity, rows, selectArray, dateValidate, hourValidate, dateHourValidate)
	if len(list) < 1 {
		return nil, status.Error(codes.NotFound, "Recurso no encontrado")
	}
	return list, nil
}

func CoreCountBySearchLike(req interface{}, entity interface{}, ctx context.Context, c *sql.Conn, dateValidate, hourValidate, dateHourValidate string, tabla string) ([]interface{}, error) {

	where, vals, order, limitOrder := utils.BuildWherePageable(req, true)
	_, selectArray := utils.BuildSelect(entity)

	var query string = "SELECT count(*) total	FROM  " + tabla + " " + where + " " + order + " " + limitOrder

	rows, err := c.QueryContext(ctx, query,
		vals...,
	)
	if err != nil {
		return nil, status.Error(codes.Unknown, "failed to select -> "+err.Error())
	}
	defer rows.Close()

	list := TraducirRespuestaListCore(entity, rows, selectArray, dateValidate, hourValidate, dateHourValidate)
	if len(list) < 1 {
		return nil, status.Error(codes.NotFound, "Recurso no encontrado")
	}
	return list, nil
}

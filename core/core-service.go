package v1

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"reflect"

	utils "github.com/7carlosz/go-proto-utils/utils"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func Init() {

}

func CoreReadBySearch(req interface{}, entity interface{}, ctx context.Context, c *sql.Conn, dateValidate, hourValidate, dateHourValidate string, tabla string) ([]interface{}, error) {

	where, vals, order, limitOrder := utils.BuildWherePageable(req)
	selectString, selectArray := utils.BuildSelect(entity)

	var query string = "SELECT " + selectString + "	FROM  " + tabla + " " + where + " " + order + " " + limitOrder

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
func CoreReadDistinctBySearch(disctintColumn string, req interface{}, entity interface{}, ctx context.Context, c *sql.Conn, dateValidate, hourValidate, dateHourValidate string, tabla string) ([]interface{}, error) {

	where, vals, _, limitOrder := utils.BuildWherePageable(req)

	queryDinamic := "SELECT distinct " + disctintColumn + "	FROM  " + tabla + " " + where + " order by 1 " + limitOrder
	rows, err := c.QueryContext(ctx, queryDinamic,
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

	rows, err := c.QueryContext(ctx, " select "+selectString+"	FROM "+tabla+" order by "+pageable.Sort+" limit $1 offset $2",
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

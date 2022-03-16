package repository

import (
	"bitbucket.org/pkg/inflect"
	"fmt"
	"reflect"
	"strconv"
	"strings"
)

const MAIN_TABLE_ALIAS = "m0_"

type inExpressionInterface interface {
	AddValue(interface{})
	GetOrIsNull() bool
	ToString() string
}

type QueryBuilder struct {
}

func (qb *QueryBuilder) escapeValueForSQL(typeStr string, value interface{}, nullable bool, zeroToNull bool) string {

	switch typeStr {
	case "string":
		if value == "" && nullable {
			return "null"
		}
		return "'" + fmt.Sprint(value) + "'"
	case "int", "int2", "int4", "int8":
		if value == nil && nullable {
			return "null"
		}

		if zeroToNull && fmt.Sprint(value) == "0" {
			if !nullable {
				panic("Int value is converting to null, but nullable field param is not set")
			}

			return "null"
		}

		return fmt.Sprint(value)

	case "bool":
		if value.(bool) == true {
			return "true"
		}
		return "false"
	}

	return fmt.Sprint(value)
}

func (qb *QueryBuilder) Update(cfg *TableConfig, object interface{}) string {
	var tableColumnLabels []string
	var tableColumnValues []string

	t := reflect.Indirect(reflect.ValueOf(object))
	fields, fieldsNotFound := GetTableColumnMap(cfg, reflect.TypeOf(object))

	updateExpr := []string{}

	var pkVal int64

	for colName, colCfg := range cfg.TableColumns {
		if colName == "id" {
			pkVal = t.FieldByName("ID").Int()
			continue
		}

		colNotFound := false
		for _, notFoundKey := range fieldsNotFound {
			if notFoundKey == colName {
				colNotFound = true
				break
			}
		}

		if colNotFound {
			continue
		}

		if classField, ok := fields[colName]; ok {
			tableColumnLabels = append(tableColumnLabels, colName)

			classFieldValue := t.FieldByName(classField).Interface()

			classFieldValueStr := qb.escapeValueForSQL(colCfg.Type, classFieldValue, colCfg.Nullable, colCfg.ZeroToNull)

			tableColumnValues = append(tableColumnValues, classFieldValueStr)

			updateExpr = append(updateExpr, colName+" = "+classFieldValueStr)
		}
	}

	for relName, relCfg := range cfg.Relations {
		if relCfg.Type == "one_to_one" || relCfg.Type == "many_to_one" {
			if fk, ok := relCfg.Params["foreign_key"]; ok {
				relValue := t.FieldByName(relName)
				if !relValue.IsValid() {
					continue
				}

				if relValue.IsZero() {
					updateExpr = append(updateExpr, fk.(string)+" = null")
				} else {
					if reflect.ValueOf(relValue).IsZero() {
						updateExpr = append(updateExpr, fk.(string)+" = null")
					} else {
						far := reflect.Indirect(reflect.ValueOf(relValue))
						if !far.IsZero() {
							farKey := reflect.Indirect(reflect.ValueOf(relValue.Interface())).FieldByName("ID").Int()
							updateExpr = append(updateExpr, fk.(string)+" = "+strconv.Itoa(int(farKey)))
						} else {
							updateExpr = append(updateExpr, fk.(string)+" = null")
						}
					}

				}
			}
		}
	}

	sql := "UPDATE \"" + cfg.TableName + "\" SET " + strings.Join(updateExpr, ", ") + " WHERE id = " + strconv.Itoa(int(pkVal))

	return sql
}

func (qb *QueryBuilder) Insert(cfg *TableConfig, object interface{}) string {
	var tableColumnLabels []string
	var tableColumnValues []string

	t := reflect.Indirect(reflect.ValueOf(object))
	fields, fieldsNotFound := GetTableColumnMap(cfg, reflect.TypeOf(object))
	for colName, colCfg := range cfg.TableColumns {
		if colName == "id" {
			continue
		}

		colNotFound := false
		for _, notFoundKey := range fieldsNotFound {
			if notFoundKey == colName {
				colNotFound = true
				break
			}
		}

		if colNotFound {
			continue
		}

		if classField, ok := fields[colName]; ok {
			tableColumnLabels = append(tableColumnLabels, colName)

			classFieldValue := t.FieldByName(classField).Interface()

			classFieldValueStr := qb.escapeValueForSQL(colCfg.Type, classFieldValue, colCfg.Nullable, colCfg.ZeroToNull)

			tableColumnValues = append(tableColumnValues, classFieldValueStr)
		}
	}

	sql := "INSERT INTO \"" + cfg.TableName + "\" (\"" + strings.Join(tableColumnLabels, "\", \"") + "\") VALUES (" +
		strings.Join(tableColumnValues, ", ") + ") RETURNING id"

	return sql
}

func (qb *QueryBuilder) SelectById(cfg *TableConfig, t reflect.Type, id interface{}) string {
	var tableColumns []string

	fields, notFound := GetTableColumnMap(cfg, t)
	for _, colName := range cfg.TableColumnsArr {

		colNotFound := false
		for _, notFoundKey := range notFound {
			if notFoundKey == colName {
				colNotFound = true
				break
			}
		}

		if colNotFound {
			continue
		}

		if _, ok := fields[colName]; ok {
			tableColumns = append(tableColumns, colName)
		}
	}

	sql := "SELECT \"" + strings.Join(tableColumns, "\", \"") + "\" FROM \"" + cfg.TableName + "\" WHERE id = " + fmt.Sprint(id)

	return sql
}

func (qb *QueryBuilder) SelectBy(cfg *TableConfig, t reflect.Type, filters map[string]interface{}, limit int, offset int, asc bool) string {
	var tableColumns []string
	var tableFilters []string
	var tableJoins []string

	if limit == 0 {
		limit = 999999
	}

	m0 := MAIN_TABLE_ALIAS

	filtersNotEmpty := false
	fields, fieldsNotFound := GetTableColumnMap(cfg, t)
	relations, _ := GetTableRelationMap(cfg, t)

	for _, colName := range cfg.TableColumnsArr {

		colCfg := cfg.TableColumns[colName]

		colNotFound := false
		for _, notFoundKey := range fieldsNotFound {
			if notFoundKey == colName {
				colNotFound = true
				break
			}
		}

		if colNotFound {
			continue
		}

		if _, ok := fields[colName]; ok {
			tableColumns = append(tableColumns, m0+"\".\""+colName)
		}

		for filterField, filterValue := range filters {

			if inflect.Camelize(colName) == filterField {
				filtersNotEmpty = true
				if reflect.TypeOf(filterValue).Kind() == reflect.Array {
					arr := reflect.ValueOf(&filterValue).Elem()
					if reflect.ValueOf(filterValue).Len() == 2 {
						array := arr.Interface().([2]string)

						tableFilters = append(tableFilters, fmt.Sprintf("\""+m0+"\".\""+colName+"\" BETWEEN %s AND %s",
							qb.escapeValueForSQL(colCfg.Type, array[0], colCfg.Nullable, colCfg.ZeroToNull),
							qb.escapeValueForSQL(colCfg.Type, array[1], colCfg.Nullable, colCfg.ZeroToNull),
						))
					} else {
						panic(fmt.Sprint("Некорректный массив в фильтрах: ", filterValue))
					}
				} else {

					inter := reflect.TypeOf((*inExpressionInterface)(nil)).Elem()

					if reflect.TypeOf(filterValue).Implements(inter) {
						str := reflect.ValueOf(filterValue).MethodByName("ToString").Call([]reflect.Value{})
						orIsNull := reflect.ValueOf(filterValue).MethodByName("GetOrIsNull").Call([]reflect.Value{})
						if orIsNull[0].Interface().(bool) {
							tableFilters = append(tableFilters, "(\""+m0+"\".\""+colName+"\" IN ("+fmt.Sprint(str[0])+") OR \""+m0+"\".\""+colName+"\" IS NULL)")
						} else {
							tableFilters = append(tableFilters, "\""+m0+"\".\""+colName+"\" IN ("+fmt.Sprint(str[0])+")")
						}

					} else {
						tableFilters = append(tableFilters, "\""+m0+"\".\""+colName+"\" = "+qb.escapeValueForSQL(colCfg.Type, filterValue, colCfg.Nullable, colCfg.ZeroToNull))
					}

				}
			}
		}
	}

	for filterField, filterValue := range filters {
		if ind := strings.Index(filterField, "."); ind != -1 {
			rel := filterField[:ind]
			fld := filterField[ind+1:]
			colName := inflect.Underscore(fld)
			for _, relation := range relations {

				if rel == relation {
					relCfg := cfg.Relations[relation]
					if relCfg.Type == "one_to_one" {
						if fk, ok := relCfg.Params["foreign_key"]; ok {
							relTargetCfg := CreateTableConfig(cfg.Dir, relCfg.Target)

							colCfg := relTargetCfg.TableColumns[colName]

							tableJoins = append(tableJoins, fmt.Sprintf("JOIN \"%s\" as \"%s\" ON \"%s\".\"%s\" = \"%s\".\"%s\"",
								relTargetCfg.TableName, rel, rel, relTargetCfg.PK, m0, fk))

							filtersNotEmpty = true
							if reflect.TypeOf(filterValue).Kind() == reflect.Array {
								arr := reflect.ValueOf(&filterValue).Elem()
								if reflect.ValueOf(filterValue).Len() == 2 {
									array := arr.Interface().([2]string)

									tableFilters = append(tableFilters, fmt.Sprintf("\""+rel+"\".\""+colName+"\" BETWEEN %s AND %s",
										qb.escapeValueForSQL(colCfg.Type, array[0], colCfg.Nullable, colCfg.ZeroToNull),
										qb.escapeValueForSQL(colCfg.Type, array[1], colCfg.Nullable, colCfg.ZeroToNull),
									))
								} else {
									panic(fmt.Sprint("Некорректный массив в фильтрах: ", filterValue))
								}
							} else {
								tableFilters = append(tableFilters, "\""+rel+"\".\""+colName+"\" = "+qb.escapeValueForSQL(colCfg.Type, filterValue, colCfg.Nullable, colCfg.ZeroToNull))
							}

							//fmt.Println(tableJoins)
						}
					}
				}
			}
			//fld := filterField[ind+1:]

		}
	}

	//fmt.Println("RRR", tableColumns)

	filtersStr := ""
	if filtersNotEmpty {
		filtersStr = "WHERE " + strings.Join(tableFilters, " AND ")
	}

	orderBy := " ORDER BY \"" + m0 + "\".\"" + cfg.PK + "\" ASC "
	if !asc {
		orderBy = " ORDER BY \"" + m0 + "\".\"" + cfg.PK + "\" DESC "
	}

	sql := "SELECT \"" + strings.Join(tableColumns, "\", \"") + "\" FROM \"" + cfg.TableName + "\" AS \"" +
		m0 + "\" " + strings.Join(tableJoins, " ") + " " + filtersStr + orderBy + " LIMIT " + strconv.Itoa(limit) + " OFFSET " + strconv.Itoa(offset)

	return sql
}

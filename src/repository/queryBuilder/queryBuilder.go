package queryBuilder

import (
	"alex-shkadov/repository/src/repository/config"
	"alex-shkadov/repository/src/repository/dbHelper"
	"bitbucket.org/pkg/inflect"
	"fmt"
	"reflect"
	"strconv"
	"strings"
)

const MAIN_TABLE_ALIAS = "m0_"

type INExpression interface {
	AddValue(interface{})
	ToString() string
}

func escapeValueForSQL(typeStr string, value interface{}, nullable bool) string {

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
		return fmt.Sprint(value)

	case "bool":
		if value.(bool) == true {
			return "true"
		}
		return "false"
	}

	return fmt.Sprint(value)
}

func Update(cfg *config.TableConfig, object interface{}) string {
	var tableColumnLabels []string
	var tableColumnValues []string

	t := reflect.Indirect(reflect.ValueOf(object))
	fields, fieldsNotFound := dbHelper.GetTableColumnMap(cfg, reflect.TypeOf(object))

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

			classFieldValueStr := escapeValueForSQL(colCfg.Type, classFieldValue, colCfg.Nullable)

			tableColumnValues = append(tableColumnValues, classFieldValueStr)

			updateExpr = append(updateExpr, colName+" = "+classFieldValueStr)
		}
	}

	sql := "UPDATE \"" + cfg.TableName + "\" SET " + strings.Join(updateExpr, ", ") + " WHERE id = " + strconv.Itoa(int(pkVal))

	return sql
}

func Insert(cfg *config.TableConfig, object interface{}) string {
	var tableColumnLabels []string
	var tableColumnValues []string

	t := reflect.Indirect(reflect.ValueOf(object))
	fields, fieldsNotFound := dbHelper.GetTableColumnMap(cfg, reflect.TypeOf(object))
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

			classFieldValueStr := escapeValueForSQL(colCfg.Type, classFieldValue, colCfg.Nullable)

			tableColumnValues = append(tableColumnValues, classFieldValueStr)
		}
	}

	sql := "INSERT INTO \"" + cfg.TableName + "\" (\"" + strings.Join(tableColumnLabels, "\", \"") + "\") VALUES (" +
		strings.Join(tableColumnValues, ", ") + ") RETURNING id"

	return sql
}

func SelectById(cfg *config.TableConfig, t reflect.Type, id interface{}) string {
	var tableColumns []string

	fields, notFound := dbHelper.GetTableColumnMap(cfg, t)
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

type F struct {
	Ids []int64
}

func (f *F) AddValue(value interface{}) {
	f.Ids = append(f.Ids, value.(int64))
}

func (f *F) ToString() string {
	strs := []string{}
	for _, i := range f.Ids {
		strs = append(strs, strconv.Itoa(int(i)))
	}

	return strings.Join(strs, ", ")
}

func SelectBy(cfg *config.TableConfig, t reflect.Type, filters map[string]interface{}, limit int, offset int, asc bool) string {
	var tableColumns []string
	var tableFilters []string
	var tableJoins []string

	if limit == 0 {
		limit = 999999
	}

	m0 := MAIN_TABLE_ALIAS

	filtersNotEmpty := false
	fields, fieldsNotFound := dbHelper.GetTableColumnMap(cfg, t)
	relations, _ := dbHelper.GetTableRelationMap(cfg, t)

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
							escapeValueForSQL(colCfg.Type, array[0], colCfg.Nullable),
							escapeValueForSQL(colCfg.Type, array[1], colCfg.Nullable),
						))
					} else {
						panic(fmt.Sprint("Некорректный массив в фильтрах: ", filterValue))
					}
				} else {

					inter := reflect.TypeOf((*INExpression)(nil)).Elem()

					if reflect.TypeOf(filterValue).Implements(inter) {
						str := reflect.ValueOf(filterValue).MethodByName("ToString").Call([]reflect.Value{})
						tableFilters = append(tableFilters, "\""+m0+"\".\""+colName+"\" IN ("+fmt.Sprint(str[0])+")")
					} else {
						tableFilters = append(tableFilters, "\""+m0+"\".\""+colName+"\" = "+escapeValueForSQL(colCfg.Type, filterValue, colCfg.Nullable))
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
							relTargetCfg := config.CreateTableConfig(relCfg.Target)

							colCfg := relTargetCfg.TableColumns[colName]

							tableJoins = append(tableJoins, fmt.Sprintf("JOIN \"%s\" as \"%s\" ON \"%s\".\"%s\" = \"%s\".\"%s\"",
								relTargetCfg.TableName, rel, rel, relTargetCfg.PK, m0, fk))

							filtersNotEmpty = true
							if reflect.TypeOf(filterValue).Kind() == reflect.Array {
								arr := reflect.ValueOf(&filterValue).Elem()
								if reflect.ValueOf(filterValue).Len() == 2 {
									array := arr.Interface().([2]string)

									tableFilters = append(tableFilters, fmt.Sprintf("\""+rel+"\".\""+colName+"\" BETWEEN %s AND %s",
										escapeValueForSQL(colCfg.Type, array[0], colCfg.Nullable),
										escapeValueForSQL(colCfg.Type, array[1], colCfg.Nullable),
									))
								} else {
									panic(fmt.Sprint("Некорректный массив в фильтрах: ", filterValue))
								}
							} else {
								tableFilters = append(tableFilters, "\""+rel+"\".\""+colName+"\" = "+escapeValueForSQL(colCfg.Type, filterValue, colCfg.Nullable))
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

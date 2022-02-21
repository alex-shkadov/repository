package repository

import (
	"bitbucket.org/pkg/inflect"
	"reflect"
)

func GetTableColumnMap(cfg *TableConfig, t reflect.Type) (map[string]string, []string) {

	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}

	result := make(map[string]string)
	notFound := []string{}
	for _, colName := range cfg.TableColumnsArr {
		colCfg := cfg.TableColumns[colName]
		var typeStruct reflect.StructField
		var fieldFound bool
		if colName == "id" {
			typeStruct, fieldFound = t.FieldByName("Id")
			if !fieldFound {
				typeStruct, fieldFound = t.FieldByName("ID")
			}
		} else {
			if colCfg.FieldName != "" {
				typeStruct, fieldFound = t.FieldByName(colCfg.FieldName)
			} else {
				typeStruct, fieldFound = t.FieldByName(inflect.Camelize(colName))
			}

		}

		if fieldFound {
			result[colName] = typeStruct.Name
		} else {
			notFound = append(notFound, colName)
		}
	}

	return result, notFound
}

func GetTableRelationMap(cfg *TableConfig, t reflect.Type) (map[string]string, []string) {

	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}

	result := make(map[string]string)
	notFound := []string{}
	for relName, _ := range cfg.Relations {
		typeStruct, relFound := t.FieldByName(inflect.Camelize(relName))

		if relFound {
			result[relName] = typeStruct.Name
		} else {
			notFound = append(notFound, relName)
		}
	}

	return result, notFound
}

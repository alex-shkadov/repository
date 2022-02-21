package repository

import (
	"database/sql"
	"errors"
	"fmt"
	"github.com/alex-shkadov/repository/src/repository/queryBuilder"
	"reflect"
)

type Repository interface {
	FindOneBy(filters map[string]interface{}) (interface{}, error)
	FindBy(filters map[string]interface{}) ([]interface{}, error)
	Find(id int64) (interface{}, error)
	Save(packet interface{}) (int64, error)
}

type AbstractRepo struct {
	db          *sql.DB
	config      *TableConfig
	reflectType reflect.Type
	identityMap *identityMap
}

func NewAbstractRepo(db *sql.DB, config *TableConfig, reflectType reflect.Type, work *identityMap) *AbstractRepo {
	return &AbstractRepo{db: db, config: config, reflectType: reflectType, identityMap: work}
}

type RowScanner interface {
	Scan(dest ...interface{}) error
}

func (a *AbstractRepo) Update(packet interface{}) error {

	//logger.Debug(fmt.Sprint("Update object of type ", reflect.TypeOf(packet), ": ", packet))
	sql := queryBuilder.Update(a.config, packet)
	//logger.DebugSQL(sql)

	saveResult := a.db.QueryRow(sql)
	//fmt.Println("saveResult", saveResult.LastInsertId(), "error", error)
	if saveResult == nil {
		return errors.New("Error occured")
	}

	return nil
}

func (a *AbstractRepo) Save(packet interface{}) (int64, error) {

	for colName, colCfg := range a.config.TableColumns {
		if colName == "id" {
			t := reflect.Indirect(reflect.ValueOf(packet))
			pk := t.FieldByName("ID").Interface()

			exists := false
			var pkVal int64
			switch colCfg.Type {
			case "int":
				if pk.(int) > 0 {
					pkVal = int64(pk.(int))
					exists = true
				}
				break
			case "int2":
				if pk.(int16) > 0 {
					pkVal = int64(pk.(int16))
					exists = true
				}
				break
			case "int4":
				if pk.(int32) > 0 {
					pkVal = int64(pk.(int32))
					exists = true
				}
				break
			case "int8":
				if pk.(int64) > 0 {
					pkVal = pk.(int64)
					exists = true
				}
				break
			}

			if exists {

				return pkVal, a.Update(packet)
			}
		}
	}

	//logger.Debug(fmt.Sprint("Insert object of type ", reflect.TypeOf(packet), ": ", packet))
	sql := queryBuilder.Insert(a.config, packet)
	//logger.DebugSQL(sql)

	saveResult := a.db.QueryRow(sql)
	//fmt.Println("saveResult", saveResult.LastInsertId(), "error", error)
	if saveResult == nil {
		return 0, errors.New("Error occured")
	}

	var lastInsertId int64
	err := saveResult.Scan(&lastInsertId)

	if err != nil {
		return 0, err
	}

	pkFieldName := a.config.PK
	if pkFieldName == "id" {
		pkFieldName = "ID"
	}

	if _, ok := a.reflectType.FieldByName(pkFieldName); ok {
		v := reflect.ValueOf(packet)
		v = reflect.Indirect(v)
		packetId := v.FieldByName(pkFieldName).Interface()
		fmt.Println("LAST N", lastInsertId)
		switch a.config.TableColumns[a.config.PK].Type {
		case "int":
			{
				if packetId.(int) == 0 {
					v.FieldByName(pkFieldName).SetInt(lastInsertId)
				}
			}
		case "int2":
			{
				if packetId.(int16) == 0 {
					v.FieldByName(pkFieldName).SetInt(lastInsertId)
				}
			}
		case "int4":
			{
				if packetId.(int32) == 0 {
					v.FieldByName(pkFieldName).SetInt(lastInsertId)
					fmt.Println("LAST N", lastInsertId)
				}
			}
		case "int8":
			{
				if packetId.(int64) == 0 {
					v.FieldByName(pkFieldName).SetInt(lastInsertId)
				}
			}
		}
	}

	//fmt.Println(saveResult)
	//sql := fmt.Sprintf("INSERT INTO %s (\"" + strings.Join(tableColumnLabels, "\", \"" + "\") VALUES ("))
	return lastInsertId, nil
}

func (a *AbstractRepo) Find(id int64) (interface{}, error) {

	sql := queryBuilder.SelectById(a.config, a.reflectType, id)
	//logger.DebugSQL(sql)

	fetchResult := a.db.QueryRow(sql)

	if fetchResult.Err() != nil {
		return nil, fetchResult.Err()
	}

	object := reflect.New(a.reflectType).Interface()

	a.fillRecordData(object, a.config, fetchResult)

	//sql := fmt.Sprintf("INSERT INTO %s (\"" + strings.Join(tableColumnLabels, "\", \"" + "\") VALUES ("))
	return object, nil
}

func (a *AbstractRepo) FindOneBy(filters map[string]interface{}, asc bool) (interface{}, error) {
	sql := queryBuilder.SelectBy(a.config, a.reflectType, filters, 1, 0, asc)

	//logger.DebugSQL(sql)

	fetchResult := a.db.QueryRow(sql)

	if fetchResult.Err() != nil {
		return nil, fetchResult.Err()
	}

	object := reflect.New(a.reflectType).Interface()

	err := a.fillRecordData(object, a.config, fetchResult)
	if err != nil {
		return nil, err
	}

	return object, nil
}

func (a *AbstractRepo) FindBy(filters map[string]interface{}, asc bool) ([]interface{}, error) {
	sql := queryBuilder.SelectBy(a.config, a.reflectType, filters, 0, 0, asc)

	//logger.DebugSQL(sql)

	fetchResult, err := a.db.Query(sql)
	if err != nil {
		return nil, err
	}

	defer fetchResult.Close()

	if fetchResult.Err() != nil {
		return []interface{}{}, fetchResult.Err()
	}

	result, err := a.fillRecordsData(a.config, fetchResult)

	return result, err
}

func (a *AbstractRepo) FindAll() ([]interface{}, error) {
	filtersDummy := make(map[string]interface{})
	return a.FindBy(filtersDummy, true)
}

func (a *AbstractRepo) fillRecordsData(cfg *TableConfig, rows *sql.Rows) ([]interface{}, error) {
	result := []interface{}{}

	rows.Scan()
	for rows.Next() {
		//fmt.Println(reflect.New(t).Elem().Interface())
		//fmt.Println(reflect.ValueOf(reflect.New(t).Elem().Interface()).Type())
		//os.Exit(0)
		object := reflect.New(a.reflectType).Interface()
		//object2 := &codec8.Tracker{}
		//fmt.Printf("%T\n", object)
		//fmt.Printf("%T\n", object2)

		err := a.fillRecordData(object, cfg, rows)

		if err != nil {
			return []interface{}{}, err
		}
		result = append(result, object)
	}

	return result, nil
}

func (a *AbstractRepo) fillRecordData(object interface{}, cfg *TableConfig, row RowScanner) error {
	err := a.fillRecordDataFields(object, cfg, row)
	if err != nil {
		return err
	}

	err = a.fillRecordDataRelations(object, cfg, row)
	if err != nil {
		return err
	}

	return nil
}

func (a *AbstractRepo) fillRecordDataRelations(object interface{}, cfg *TableConfig, row RowScanner) error {

	return nil
	t := reflect.TypeOf(object)
	//fmt.Println("TTT1", t, object)
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}

	//fmt.Println("TTT2", t)

	tableRelationFields, _ := GetTableRelationMap(cfg, t)

	for relName, relCfg := range a.config.Relations {
		classField, ok := t.FieldByName(tableRelationFields[relName])
		if !ok {
			return errors.New("Field not found for relation " + relName + " in type " + t.Name())
		}

		classField = classField

		switch relCfg.Type {
		case "one_to_one":
			break
		case "many_to_one":
			break
		case "one_to_many":
			break
		}
	}
	return nil
}

func (a *AbstractRepo) fillRecordDataFields(object interface{}, cfg *TableConfig, row RowScanner) error {

	t := reflect.TypeOf(object)
	//fmt.Println("TTT1", t, object)
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}

	//fmt.Println("TTT2", t)

	tableColumnFields, _ := GetTableColumnMap(cfg, t)
	fieldValuesArr := []interface{}{}
	fieldValuesArrCallbacks := []func() (interface{}, string, string){}

	for _, colName := range a.config.TableColumnsArr {
		colCfg := a.config.TableColumns[colName]
		classField, ok := t.FieldByName(tableColumnFields[colName])
		if !ok {
			return errors.New("Field not found for column " + colName + " in type " + t.Name())
		}

		switch colCfg.Type {
		case "string":

			val := sql.NullString{}
			fieldValuesArr = append(fieldValuesArr, &val)
			fieldValuesArrCallbacks = append(fieldValuesArrCallbacks, func() (interface{}, string, string) {
				return val.String, classField.Name, classField.Type.Name()
			})
			break
		case "float64":

			val := sql.NullFloat64{}
			fieldValuesArr = append(fieldValuesArr, &val)
			fieldValuesArrCallbacks = append(fieldValuesArrCallbacks, func() (interface{}, string, string) {
				return val.Float64, classField.Name, classField.Type.Name()
			})
			break
		case "int":

			if classField.Type.Name() == "uint8" {
				val := sql.NullInt16{}
				fieldValuesArr = append(fieldValuesArr, &val)
				fieldValuesArrCallbacks = append(fieldValuesArrCallbacks, func() (interface{}, string, string) {
					return val.Int16, classField.Name, classField.Type.Name()
				})
			} else {
				val := sql.NullByte{}
				fieldValuesArr = append(fieldValuesArr, &val)
				fieldValuesArrCallbacks = append(fieldValuesArrCallbacks, func() (interface{}, string, string) {
					return val.Byte, classField.Name, classField.Type.Name()
				})
			}
			break
		case "int2":

			if classField.Type.Name() == "uint16" {
				val := sql.NullInt32{}
				fieldValuesArr = append(fieldValuesArr, &val)
				fieldValuesArrCallbacks = append(fieldValuesArrCallbacks, func() (interface{}, string, string) {
					return val.Int32, classField.Name, classField.Type.Name()
				})
			} else {
				val := sql.NullInt16{}
				fieldValuesArr = append(fieldValuesArr, &val)
				fieldValuesArrCallbacks = append(fieldValuesArrCallbacks, func() (interface{}, string, string) {
					return val.Int16, classField.Name, classField.Type.Name()
				})
			}

			break
		case "int4":

			val := sql.NullInt32{}
			fieldValuesArr = append(fieldValuesArr, &val)
			fieldValuesArrCallbacks = append(fieldValuesArrCallbacks, func() (interface{}, string, string) {
				return val.Int32, classField.Name, classField.Type.Name()
			})
			break
		case "int8":

			val := sql.NullInt64{}
			fieldValuesArr = append(fieldValuesArr, &val)
			fieldValuesArrCallbacks = append(fieldValuesArrCallbacks, func() (interface{}, string, string) {
				return val.Int64, classField.Name, classField.Type.Name()
			})
			break
		case "bool":

			val := sql.NullBool{}
			fieldValuesArr = append(fieldValuesArr, &val)
			fieldValuesArrCallbacks = append(fieldValuesArrCallbacks, func() (interface{}, string, string) {
				return val.Bool, classField.Name, classField.Type.Name()
			})
			break
		}
		//reflect.ValueOf(object).FieldByName(tableColumnFields[colName])

	}

	err := row.Scan(fieldValuesArr...)
	if err != nil {
		if err == sql.ErrNoRows {
			return err
		} else {
			panic(err)
		}

	}

	value := reflect.ValueOf(object)

	if value.Kind() == reflect.Ptr {
		value = value.Elem()
	}

	tmp := reflect.New(value.Type()).Elem()
	//fmt.Println("TTT3", tmp)
	//fmt.Println("TTT4", value)
	tmp.Set(value)
	//fmt.Println("TTT5", tmp)
	for _, callback := range fieldValuesArrCallbacks {
		val, classFieldName, Type := callback()
		//fmt.Println(val, classFieldName, Type)
		switch Type {
		case "string":
			tmp.FieldByName(classFieldName).SetString(val.(string))
			break
		case "float64":
			tmp.FieldByName(classFieldName).SetFloat(val.(float64))
			break
		case "int":
			tmp.FieldByName(classFieldName).SetInt(int64(val.(int32)))
			break
		case "uint8":
			tmp.FieldByName(classFieldName).SetUint(uint64(val.(int16)))
			break
		case "uint16":
			tmp.FieldByName(classFieldName).SetUint(uint64(val.(int32)))
			break
		case "int16":
			tmp.FieldByName(classFieldName).SetInt(int64(val.(int16)))
			break
		case "int32":
			tmp.FieldByName(classFieldName).SetInt(int64(val.(int32)))
			break
		case "int64":
			tmp.FieldByName(classFieldName).SetInt(val.(int64))
			break
		case "bool":
			tmp.FieldByName(classFieldName).SetBool(val.(bool))
			break
		}

		//value := reflect.ValueOf(object).FieldByName(classFieldName)
	}

	value.Set(tmp)

	//fmt.Println("value.Interface()", value.Interface())
	//fmt.Println("object.Interface()", object)

	return nil
}

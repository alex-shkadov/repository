package repository

import (
	"fmt"
	"github.com/spf13/viper"
)

type TableColumnConfig struct {
	Nullable  bool
	Type      string
	FieldName string
}

type TableRelationConfig struct {
	Type   string
	Target string
	Params map[string]interface{}
}

func NewTableRelationConfig(typeStr string, target string) *TableRelationConfig {
	c := &TableRelationConfig{Type: typeStr, Target: target}
	c.Params = make(map[string]interface{})
	return c
}

func NewTableColumnConfig(nullable bool, typeStr string) *TableColumnConfig {
	return &TableColumnConfig{Nullable: nullable, Type: typeStr}
}

type TableConfig struct {
	TableName       string
	PK              string
	TableColumns    map[string]*TableColumnConfig
	TableColumnsArr []string
	Relations       map[string]*TableRelationConfig
}

func NewTableConfig(tableName string, PK string) *TableConfig {

	return &TableConfig{
		TableName:       tableName,
		PK:              PK,
		TableColumns:    make(map[string]*TableColumnConfig),
		TableColumnsArr: []string{},
		Relations:       make(map[string]*TableRelationConfig),
	}
}

func (cfg *TableConfig) dump() {
	// TODO: implement
}

func CreateTableConfig(dir string, tableName string) *TableConfig {
	viper.SetConfigName(tableName)
	viper.SetConfigType("yaml")
	viper.AddConfigPath(dir)

	err := viper.ReadInConfig()
	if err != nil { // Handle errors reading the config file
		panic(fmt.Errorf("Fatal error config file: %w \n", err))
	}

	tbl := viper.Get("table_name")

	pk := viper.Get("pk")
	columns := viper.Get("columns")
	relations := viper.Get("relations")

	//fmt.Println(columns)

	newConfig := NewTableConfig(tbl.(string), pk.(string))

	//columnConfigs := []TableColumnConfig{}

	for _, colConfig := range columns.([]interface{}) {

		for colName, cf := range colConfig.(map[interface{}]interface{}) {
			configData := cf.(map[interface{}]interface{})
			c := NewTableColumnConfig(configData["nullable"].(bool), configData["type"].(string))

			if val, ok := configData["fieldName"]; ok {
				c.FieldName = val.(string)
			}

			newConfig.TableColumns[colName.(string)] = c
			newConfig.TableColumnsArr = append(newConfig.TableColumnsArr, colName.(string))
		}
	}

	if relations != nil {
		for _, relConfig := range relations.([]interface{}) {

			for relName, cf := range relConfig.(map[interface{}]interface{}) {
				configData := cf.(map[interface{}]interface{})
				c := NewTableRelationConfig(configData["type"].(string), configData["target"].(string))

				if val, ok := configData["foreign_key"]; ok {
					c.Params["foreign_key"] = val.(string)
				}

				if val, ok := configData["cascade_persist"]; ok {
					if val == "true" || val == "1" {
						c.Params["cascade_persist"] = true
					}
					if val == "false" || val == "0" {
						c.Params["cascade_persist"] = false
					}
				}
				newConfig.Relations[relName.(string)] = c
			}
		}
	}

	//fmt.Println(columnConfigs)

	return newConfig
}

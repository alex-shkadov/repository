package repository

// Класс IdentityMap для хранения идентификаторов
// Не используется, т.к. это вызовет хранение всей БД в памяти
import (
	"encoding/json"
	"fmt"
	"reflect"
)

var _identityMap *identityMap

func GetIdentityMap() *identityMap {
	if _identityMap == nil {
		_identityMap = &identityMap{}
	}

	return _identityMap
}

type identityMap struct {
	// первый ключ - тип, второй - ID объекта
	entities    map[string]map[string]interface{}
	entitiesIds map[string]map[int64]interface{}
}

func (u *identityMap) GetEntity(t reflect.Type, entityId interface{}) interface{} {
	if _, ok := u.entitiesIds[t.String()]; !ok {
		panic(fmt.Sprint("Unit of work GetEntity method fail:", "record not exist for ID", entityId))
	}

	if _, ok := u.entitiesIds[t.String()][entityId.(int64)]; !ok {
		panic(fmt.Sprint("Unit of work GetEntity method fail:", "record not exist for ID", entityId))
	}

	return u.entitiesIds[t.String()][entityId.(int64)]
}

func (u *identityMap) HasEntity(entity interface{}) bool {
	t := reflect.TypeOf(entity).String()
	if _, ok := u.entities[t]; !ok {
		return false
	}

	json, err := json.Marshal(entity)
	if err != nil {
		panic(fmt.Sprint("Unit of work hasEntity method fail:", err))
	}

	if _, ok := u.entities[t][string(json)]; !ok {
		return false
	}

	return true
}

func (u *identityMap) FindById(t reflect.Type, id int64) interface{} {
	filters := make(map[string]interface{})
	filters["ID"] = id
	return u.FindOneBy(t, filters)
}

func (u *identityMap) FindOneBy(t reflect.Type, filters map[string]interface{}) interface{} {
	result := u.FindBy(t, filters)
	if len(result) > 0 {
		for _, object := range result {
			return object
		}
	}

	return nil
}

func (u *identityMap) FindBy(t reflect.Type, filters map[string]interface{}) map[int64]interface{} {
	if _, ok := u.entitiesIds[t.String()]; !ok {
		return nil
	}

	found := make(map[int64]interface{})

	for id, entity := range u.entitiesIds[t.String()] {
		recordFiltered := true
		for classField, fieldValue := range filters {
			if reflect.ValueOf(entity).FieldByName(classField).Interface() != fieldValue {
				recordFiltered = false
				break
			}
		}

		if recordFiltered {
			found[id] = entity
		}
	}

	return found
}

func (u *identityMap) HasEntityById(t reflect.Type, entityId interface{}) bool {

	if _, ok := u.entitiesIds[t.String()]; !ok {
		return false
	}

	if _, ok := u.entitiesIds[t.String()][entityId.(int64)]; !ok {
		return false
	}

	return true
}

func (u *identityMap) AddEntity(entity interface{}) {
	t := reflect.TypeOf(entity).String()
	if _, ok := u.entities[t]; !ok {
		u.entities[t] = make(map[string]interface{})
	}

	json, err := json.Marshal(entity)
	if err != nil {
		panic(fmt.Sprint("Unit of work hasEntity method fail:", err))
	}

	if _, ok := u.entities[t][string(json)]; !ok {
		panic(fmt.Sprint("Unit of work addEntity method fail: Entity already exists for type", t, json))
	}

	u.entities[t][string(json)] = entity
	id := reflect.ValueOf(entity).FieldByName("ID").Interface()
	id64 := id.(int64)
	u.entitiesIds[t][id64] = entity
}

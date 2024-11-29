package remongo

import (
	"context"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type IMongoModel interface {
	Collection() string
}

type IMongoRepository[T IMongoModel] interface {
	GetDB() *mongo.Database
	FindOne(model *T, filter interface{}, opts ...*options.FindOneOptions) error
	Find(
		models []*T,
		filter interface{},
		aggregate interface{},
		opts ...*options.FindOptions,
	) error
	InsertOne(model *T, opts ...*options.InsertOneOptions) (error, interface{})
	InsertMany(models *[]T, opts ...*options.InsertManyOptions) (error, interface{})
	ReplaceOne(filter interface{}, model *T, opts ...*options.ReplaceOptions) (error, int64)
	UpdateOne(filter interface{}, update interface{}, opts ...*options.UpdateOptions) (error, int64)
	UpdateMany(filter interface{}, update interface{}, opts ...*options.UpdateOptions) (error, int64)
	DeleteOne(filter interface{}, opts ...*options.DeleteOptions) (error, int64)
	DeleteMany(filter interface{}, opts ...*options.DeleteOptions) (error, int64)
}

type MongoRepository[T IMongoModel] struct {
	IMongoRepository[T]
	Model    T
	Database *mongo.Database
}

func (mr *MongoRepository[T]) GetDB() *mongo.Database {
	return mr.Database
}

func (mr *MongoRepository[T]) GetCollection() *mongo.Collection {
	return mr.Database.Collection(mr.Model.Collection())
}

func (mr *MongoRepository[T]) FindOne(
	model *T,
	filter interface{},
	opts ...*options.FindOneOptions,
) error {
	bson, err := ToBson(filter)

	if err != nil {
		return err
	}

	res := mr.GetCollection().FindOne(context.TODO(), bson)

	err = res.Decode(&model)

	if err != nil {
		if err == mongo.ErrNoDocuments {
			return nil
		}

		return err
	}

	return nil
}

func (mr *MongoRepository[T]) Find(
	models []*T,
	filter interface{},
	aggregate interface{},
	opts ...*options.FindOptions,
) error {
	bson, err := ToBson(filter)

	if err != nil {
		return err
	}

	coll := mr.GetCollection()

	if aggregate != nil {
		coll.Aggregate(context.TODO(), aggregate)
	}

	cursor, err := coll.Find(context.TODO(), bson, opts...)

	if err = cursor.All(context.TODO(), &models); err != nil {
		return err
	}

	return nil
}

func (mr *MongoRepository[T]) InsertOne(
	model *T,
	opts ...*options.InsertOneOptions,
) (error, interface{}) {
	result, err := mr.GetCollection().
		InsertOne(context.TODO(), model, opts...)

	if err != nil {
		return err, nil
	}

	return nil, result.InsertedID
}

func (mr *MongoRepository[T]) InsertMany(
	models *[]T,
	opts ...*options.InsertManyOptions,
) (error, interface{}) {
	results, err := mr.GetCollection().
		InsertMany(context.TODO(), []interface{}{models}, opts...)

	if err != nil {
		return err, nil
	}

	return nil, results.InsertedIDs
}

func (mr *MongoRepository[T]) ReplaceOne(
	filter interface{},
	model *T,
	opts ...*options.ReplaceOptions,
) (error, int64) {
	result, err := mr.GetCollection().ReplaceOne(context.TODO(), filter, model, opts...)

	if err != nil {
		return err, 0
	}

	return nil, result.ModifiedCount
}

func (mr *MongoRepository[T]) UpdateOne(
	filter interface{},
	update interface{},
	opts ...*options.UpdateOptions,
) (error, int64) {
	result, err := mr.GetCollection().
		UpdateOne(context.TODO(), filter, update, opts...)

	if err != nil {
		return err, 0
	}

	return nil, result.ModifiedCount
}

func (mr *MongoRepository[T]) UpdateMany(
	filter interface{},
	update interface{},
	opts ...*options.UpdateOptions,
) (error, int64) {
	result, err := mr.GetCollection().UpdateMany(context.TODO(), filter, update)

	if err != nil {
		return err, 0
	}

	return nil, result.ModifiedCount
}

func (mr *MongoRepository[T]) DeleteOne(
	filter interface{},
	opts ...*options.DeleteOptions,
) (error, int64) {
	result, err := mr.GetCollection().DeleteOne(context.TODO(), filter, opts...)

	if err != nil {
		return err, 0
	}

	return nil, result.DeletedCount
}

func (mr *MongoRepository[T]) DeleteMany(
	filter interface{},
	opts ...*options.DeleteOptions,
) (error, int64) {
	result, err := mr.GetCollection().DeleteMany(context.TODO(), filter, opts...)

	if err != nil {
		return err, 0
	}

	return nil, result.DeletedCount
}

func InitRepository[T IMongoModel](database *mongo.Database, model IMongoModel) IMongoRepository[T] {
	return &MongoRepository[T]{
		Database: database,
		Model:    model.(T),
	}
}

func ToBson(v interface{}) (doc *bson.D, err error) {
	if r, ok := v.(*bson.D); ok {
		return r, nil
	}

	data, err := bson.Marshal(v)
	if err != nil {
		return
	}

	err = bson.Unmarshal(data, &doc)

	return
}

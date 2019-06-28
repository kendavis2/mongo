package mongo

import (
	"context"
	"encoding/json"
	"sync"
	"time"

	"github.com/go-session/session"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

var (
	_             session.ManagerStore = &managerStore{}
	_             session.Store        = &store{}
	jsonMarshal                        = json.Marshal
	jsonUnmarshal                      = json.Unmarshal
)

// NewStore Create an instance of a mongo store
func NewStore(ctx context.Context, url, dbName, cName string) session.ManagerStore {

	client, err := mongo.NewClient(options.Client().ApplyURI(url))
	if err != nil {
		panic(err)
	}

	if err := client.Connect(ctx); err != nil {
		panic(err)
	}

	if err := client.Ping(ctx, nil); err != nil {
		panic(err)
	}

	return newManagerStore(ctx, client, dbName, cName)
}

// NewStoreWithClient Create an instance of a mongo store
func NewStoreWithClient(ctx context.Context, client *mongo.Client, dbName, cName string) session.ManagerStore {
	return newManagerStore(ctx, client, dbName, cName)
}

func newManagerStore(ctx context.Context, client *mongo.Client, dbName, cName string) *managerStore {

	db := client.Database(dbName)
	c := db.Collection(cName)

	ex := int32(1)

	index := mongo.IndexModel{
		Keys: bson.D{{"expired_at", 1}},
		Options: &options.IndexOptions{
			ExpireAfterSeconds: &ex,
		},
	}

	_, err := c.Indexes().CreateOne(ctx, index)
	if err != nil {
		panic(err)
	}

	return &managerStore{
		client: client,
		dbName: dbName,
		cName:  cName,
	}
}

type managerStore struct {
	client *mongo.Client
	dbName string
	cName  string
}

func (s *managerStore) getValue(ctx context.Context, sid string) (string, error) {
	var item sessionItem

	if err := s.client.UseSession(ctx, func(sctx mongo.SessionContext) error {

		c := s.client.Database(s.dbName).Collection(s.cName)

		filter := bson.D{{"_id", sid}}

		err := c.FindOne(sctx, filter).Decode(&item)
		if err != nil {
			if err == mongo.ErrNoDocuments {
				return nil
			}
			return err
		} else if item.ExpiredAt.Before(time.Now()) {
			return nil
		}

		return nil
	}); err != nil {
		return "", err
	}

	return item.Value, nil
}

func (s *managerStore) parseValue(value string) (map[string]interface{}, error) {
	var values map[string]interface{}
	if len(value) > 0 {
		err := jsonUnmarshal([]byte(value), &values)
		if err != nil {
			return nil, err
		}
	}

	return values, nil
}

func (s *managerStore) Check(ctx context.Context, sid string) (bool, error) {
	val, err := s.getValue(ctx, sid)
	if err != nil {
		return false, err
	}
	return val != "", nil
}

func (s *managerStore) Create(ctx context.Context, sid string, expired int64) (session.Store, error) {
	return newStore(ctx, s, sid, expired, nil), nil
}

func (s *managerStore) Update(ctx context.Context, sid string, expired int64) (session.Store, error) {
	value, err := s.getValue(ctx, sid)
	if err != nil {
		return nil, err
	} else if value == "" {
		return newStore(ctx, s, sid, expired, nil), nil
	}

	if err := s.client.UseSession(context.Background(), func(sctx mongo.SessionContext) error {

		c := s.client.Database(s.dbName).Collection(s.cName)

		d := time.Now().Add(time.Duration(expired) * time.Second)

		filter := bson.D{{"_id", sid}}
		update := bson.D{{"$set", bson.D{{"expired_at", d}}}}

		_, err := c.UpdateOne(sctx, filter, update)
		if err != nil {
			if err == mongo.ErrNoDocuments {
				return nil
			}
			return err
		}

		return nil
	}); err != nil {
		return nil, err
	}

	values, err := s.parseValue(value)
	if err != nil {
		return nil, err
	}

	return newStore(ctx, s, sid, expired, values), nil
}

func (s *managerStore) Delete(_ context.Context, sid string) error {
	return s.client.UseSession(context.Background(), func(sctx mongo.SessionContext) error {

		c := s.client.Database(s.dbName).Collection(s.cName)

		if _, err := c.DeleteOne(sctx, bson.D{{"_id", sid}}); err != nil {
			return err
		}

		return nil
	})
}

func (s *managerStore) Refresh(ctx context.Context, oldsid, sid string, expired int64) (session.Store, error) {
	value, err := s.getValue(ctx, oldsid)
	if err != nil {
		return nil, err
	} else if value == "" {
		return newStore(ctx, s, sid, expired, nil), nil
	}

	if err := s.client.UseSession(context.Background(), func(sctx mongo.SessionContext) error {

		c := s.client.Database(s.dbName).Collection(s.cName)

		update := bson.D{{"$set", bson.D{
			{"_id", sid},
			{"value", value},
			{"expired_at", time.Now().Add(time.Duration(expired) * time.Second)},
		}}}

		upsert := true
		if _, err := c.UpdateOne(sctx, bson.D{{"_id", sid}}, update, &options.UpdateOptions{
			Upsert: &upsert,
		}); err != nil {
			return err
		}

		if _, err := c.DeleteOne(sctx, bson.D{{"_id", oldsid}}); err != nil {
			return err
		}

		return nil
	}); err != nil {
		return nil, err
	}

	values, err := s.parseValue(value)
	if err != nil {
		return nil, err
	}

	return newStore(ctx, s, sid, expired, values), nil
}

func (s *managerStore) Close() error {
	return s.client.Disconnect(context.Background())
}

func newStore(ctx context.Context, s *managerStore, sid string, expired int64, values map[string]interface{}) *store {
	if values == nil {
		values = make(map[string]interface{})
	}

	return &store{
		client:  s.client,
		dbName:  s.dbName,
		cName:   s.cName,
		ctx:     ctx,
		sid:     sid,
		expired: expired,
		values:  values,
	}
}

type store struct {
	sync.RWMutex
	ctx     context.Context
	client  *mongo.Client
	dbName  string
	cName   string
	sid     string
	expired int64
	values  map[string]interface{}
}

func (s *store) Context() context.Context {
	return s.ctx
}

func (s *store) SessionID() string {
	return s.sid
}

func (s *store) Set(key string, value interface{}) {
	s.Lock()
	s.values[key] = value
	s.Unlock()
}

func (s *store) Get(key string) (interface{}, bool) {
	s.RLock()
	val, ok := s.values[key]
	s.RUnlock()
	return val, ok
}

func (s *store) Delete(key string) interface{} {
	s.RLock()
	v, ok := s.values[key]
	s.RUnlock()
	if ok {
		s.Lock()
		delete(s.values, key)
		s.Unlock()
	}
	return v
}

func (s *store) Flush() error {
	s.Lock()
	s.values = make(map[string]interface{})
	s.Unlock()
	return s.Save()
}

func (s *store) Save() error {
	var value string

	s.RLock()
	if len(s.values) > 0 {
		buf, err := jsonMarshal(s.values)
		if err != nil {
			s.RUnlock()
			return err
		}
		value = string(buf)
	}
	s.RUnlock()

	return s.client.UseSession(s.ctx, func(sctx mongo.SessionContext) error {

		c := s.client.Database(s.dbName).Collection(s.cName)

		update := bson.D{{"$set", bson.D{
			{"_id", s.sid},
			{"value", value},
			{"expired_at", time.Now().Add(time.Duration(s.expired) * time.Second)},
		}}}

		upsert := true
		if _, err := c.UpdateOne(sctx, bson.D{{"_id", s.sid}}, update, &options.UpdateOptions{
			Upsert: &upsert,
		}); err != nil {
			return err
		}

		return nil
	})
}

// Data items stored in mongo
type sessionItem struct {
	ID        string    `bson:"_id"`
	Value     string    `bson:"value"`
	ExpiredAt time.Time `bson:"expired_at"`
}

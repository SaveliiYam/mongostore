// Package mongostore — адаптер go.mongodb.org/mongo-driver к портам
// github.com/SaveliiYam/docstore. Аналог pgxdb, только для MongoDB.
//
//	client, err := mongostore.Connect(ctx, mongostore.Config{URI: "mongodb://localhost:27017"})
//	coll := client.Database("app").Collection("users")
//	err = coll.FindOne(ctx, bson.M{"email": e}, &u)
package mongostore

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/SaveliiYam/docstore"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
)

// Config — настройки подключения.
type Config struct {
	URI            string        `env:"MONGO_URI" envDefault:"mongodb://localhost:27017"`
	ConnectTimeout time.Duration `env:"CONNECT_TIMEOUT" envDefault:"10s"`
	AppName        string        `env:"APP_NAME"`
}

// Client оборачивает *mongo.Client и реализует docstore.Client / docstore.DB.
type Client struct {
	cli *mongo.Client
}

// Connect создаёт клиент и устанавливает соединение.
func Connect(ctx context.Context, cfg Config) (*Client, error) {
	if cfg.URI == "" {
		return nil, errors.New("mongostore: URI is empty")
	}
	opts := options.Client().ApplyURI(cfg.URI)
	if cfg.ConnectTimeout > 0 {
		opts.SetConnectTimeout(cfg.ConnectTimeout)
	}
	if cfg.AppName != "" {
		opts.SetAppName(cfg.AppName)
	}
	cli, err := mongo.Connect(ctx, opts)
	if err != nil {
		return nil, fmt.Errorf("mongostore: connect: %w", err)
	}
	c := &Client{cli: cli}
	if err := c.Ping(ctx); err != nil {
		_ = cli.Disconnect(ctx)
		return nil, err
	}
	return c, nil
}

// NewFromClient оборачивает готовый *mongo.Client.
func NewFromClient(cli *mongo.Client) *Client { return &Client{cli: cli} }

// Native — escape hatch.
func (c *Client) Native() *mongo.Client { return c.cli }

func (c *Client) Database(name string) docstore.Database {
	return &Database{db: c.cli.Database(name)}
}

func (c *Client) Ping(ctx context.Context) error {
	if err := c.cli.Ping(ctx, readpref.Primary()); err != nil {
		return fmt.Errorf("mongostore: ping: %w", err)
	}
	return nil
}

func (c *Client) Disconnect(ctx context.Context) error {
	if err := c.cli.Disconnect(ctx); err != nil {
		return fmt.Errorf("mongostore: disconnect: %w", err)
	}
	return nil
}

func (c *Client) WithinTx(ctx context.Context, fn func(ctx context.Context) error) error {
	session, err := c.cli.StartSession()
	if err != nil {
		return fmt.Errorf("mongostore: start session: %w", err)
	}
	defer session.EndSession(ctx)

	_, err = session.WithTransaction(ctx, func(sc mongo.SessionContext) (any, error) {
		return nil, fn(sc)
	})
	if err != nil {
		return fmt.Errorf("mongostore: transaction: %w", err)
	}
	return nil
}

// --- database ---

type Database struct {
	db *mongo.Database
}

func (d *Database) Name() string { return d.db.Name() }

func (d *Database) Collection(name string) docstore.Collection {
	return &Collection{coll: d.db.Collection(name)}
}

func (d *Database) Drop(ctx context.Context) error {
	return d.db.Drop(ctx)
}

func (d *Database) ListCollectionNames(ctx context.Context) ([]string, error) {
	return d.db.ListCollectionNames(ctx, bson.M{})
}

func (d *Database) Native() *mongo.Database { return d.db }

// --- collection ---

type Collection struct {
	coll *mongo.Collection
}

func (c *Collection) Name() string                 { return c.coll.Name() }
func (c *Collection) Native() *mongo.Collection    { return c.coll }
func (c *Collection) Drop(ctx context.Context) error { return c.coll.Drop(ctx) }

func (c *Collection) InsertOne(ctx context.Context, document any) (*docstore.InsertOneResult, error) {
	res, err := c.coll.InsertOne(ctx, document)
	if err != nil {
		return nil, mapError(err)
	}
	return &docstore.InsertOneResult{InsertedID: res.InsertedID}, nil
}

func (c *Collection) InsertMany(ctx context.Context, documents []any) (*docstore.InsertManyResult, error) {
	res, err := c.coll.InsertMany(ctx, documents)
	if err != nil {
		return nil, mapError(err)
	}
	return &docstore.InsertManyResult{InsertedIDs: res.InsertedIDs}, nil
}

func (c *Collection) FindOne(ctx context.Context, filter any, dest any, opts ...*docstore.FindOneOptions) error {
	o := options.FindOne()
	if len(opts) > 0 && opts[0] != nil {
		if opts[0].Sort != nil {
			o.SetSort(opts[0].Sort)
		}
		if opts[0].Projection != nil {
			o.SetProjection(opts[0].Projection)
		}
	}
	err := c.coll.FindOne(ctx, filter, o).Decode(dest)
	return mapError(err)
}

func (c *Collection) Find(ctx context.Context, filter any, opts ...*docstore.FindOptions) (docstore.Cursor, error) {
	o := options.Find()
	if len(opts) > 0 && opts[0] != nil {
		applyFindOptions(o, opts[0])
	}
	cur, err := c.coll.Find(ctx, filter, o)
	if err != nil {
		return nil, mapError(err)
	}
	return &Cursor{cur: cur}, nil
}

func (c *Collection) UpdateOne(ctx context.Context, filter, update any, opts ...*docstore.UpdateOptions) (*docstore.UpdateResult, error) {
	o := options.Update()
	if len(opts) > 0 && opts[0] != nil && opts[0].Upsert != nil {
		o.SetUpsert(*opts[0].Upsert)
	}
	res, err := c.coll.UpdateOne(ctx, filter, update, o)
	if err != nil {
		return nil, mapError(err)
	}
	return mapUpdateResult(res), nil
}

func (c *Collection) UpdateMany(ctx context.Context, filter, update any, opts ...*docstore.UpdateOptions) (*docstore.UpdateResult, error) {
	o := options.Update()
	if len(opts) > 0 && opts[0] != nil && opts[0].Upsert != nil {
		o.SetUpsert(*opts[0].Upsert)
	}
	res, err := c.coll.UpdateMany(ctx, filter, update, o)
	if err != nil {
		return nil, mapError(err)
	}
	return mapUpdateResult(res), nil
}

func (c *Collection) ReplaceOne(ctx context.Context, filter, replacement any, opts ...*docstore.ReplaceOptions) (*docstore.UpdateResult, error) {
	o := options.Replace()
	if len(opts) > 0 && opts[0] != nil && opts[0].Upsert != nil {
		o.SetUpsert(*opts[0].Upsert)
	}
	res, err := c.coll.ReplaceOne(ctx, filter, replacement, o)
	if err != nil {
		return nil, mapError(err)
	}
	return mapUpdateResult(res), nil
}

func (c *Collection) DeleteOne(ctx context.Context, filter any) (*docstore.DeleteResult, error) {
	res, err := c.coll.DeleteOne(ctx, filter)
	if err != nil {
		return nil, mapError(err)
	}
	return &docstore.DeleteResult{DeletedCount: res.DeletedCount}, nil
}

func (c *Collection) DeleteMany(ctx context.Context, filter any) (*docstore.DeleteResult, error) {
	res, err := c.coll.DeleteMany(ctx, filter)
	if err != nil {
		return nil, mapError(err)
	}
	return &docstore.DeleteResult{DeletedCount: res.DeletedCount}, nil
}

func (c *Collection) CountDocuments(ctx context.Context, filter any, opts ...*docstore.CountOptions) (int64, error) {
	o := options.Count()
	if len(opts) > 0 && opts[0] != nil {
		if opts[0].Limit != nil {
			o.SetLimit(*opts[0].Limit)
		}
		if opts[0].Skip != nil {
			o.SetSkip(*opts[0].Skip)
		}
	}
	n, err := c.coll.CountDocuments(ctx, filter, o)
	return n, mapError(err)
}

// --- cursor ---

type Cursor struct {
	cur *mongo.Cursor
}

func (c *Cursor) Next(ctx context.Context) bool { return c.cur.Next(ctx) }
func (c *Cursor) Decode(dest any) error         { return c.cur.Decode(dest) }
func (c *Cursor) All(ctx context.Context, results any) error {
	return c.cur.All(ctx, results)
}
func (c *Cursor) Close(ctx context.Context) error { return c.cur.Close(ctx) }
func (c *Cursor) Err() error                      { return c.cur.Err() }

// --- helpers ---

func applyFindOptions(o *options.FindOptions, fo *docstore.FindOptions) {
	if fo.Limit != nil {
		o.SetLimit(*fo.Limit)
	}
	if fo.Skip != nil {
		o.SetSkip(*fo.Skip)
	}
	if fo.Sort != nil {
		o.SetSort(fo.Sort)
	}
	if fo.Projection != nil {
		o.SetProjection(fo.Projection)
	}
}

func mapUpdateResult(res *mongo.UpdateResult) *docstore.UpdateResult {
	return &docstore.UpdateResult{
		MatchedCount:  res.MatchedCount,
		ModifiedCount: res.ModifiedCount,
		UpsertedCount: res.UpsertedCount,
		UpsertedID:    res.UpsertedID,
	}
}

func mapError(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, mongo.ErrNoDocuments) {
		return docstore.ErrNotFound
	}
	if mongo.IsDuplicateKeyError(err) {
		return fmt.Errorf("%w: %v", docstore.ErrDuplicateKey, err)
	}
	return err
}

// IsNotFound / MapNotFound — удобные хелперы на стороне адаптера.
func IsNotFound(err error) bool { return docstore.IsNotFound(err) }

func MapNotFound(err error) error {
	if errors.Is(err, mongo.ErrNoDocuments) {
		return docstore.ErrNotFound
	}
	return err
}

// --- app.Component ---

func (c *Client) Name() string { return "mongodb" }

func (c *Client) Start(ctx context.Context) error {
	if err := c.Ping(ctx); err != nil {
		return err
	}
	<-ctx.Done()
	return nil
}

func (c *Client) Stop(ctx context.Context) error { return c.Disconnect(ctx) }

var (
	_ docstore.Client     = (*Client)(nil)
	_ docstore.DB         = (*Client)(nil)
	_ docstore.Database   = (*Database)(nil)
	_ docstore.Collection = (*Collection)(nil)
	_ docstore.Cursor     = (*Cursor)(nil)
)

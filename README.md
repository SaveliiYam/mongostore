# mongostore — MongoDB-адаптер для docstore

Реализует порты [`github.com/SaveliiYam/docstore`](https://github.com/SaveliiYam/docstore) поверх официального `mongo-driver`.

```go
client, err := mongostore.Connect(ctx, mongostore.Config{
    URI: "mongodb://localhost:27017",
})
defer client.Disconnect(ctx)

coll := client.Database("app").Collection("users")
_, err = coll.InsertOne(ctx, bson.M{"name": "Ada"})
err = coll.FindOne(ctx, bson.M{"name": "Ada"}, &user)
```

## Установка

```bash
go get github.com/SaveliiYam/mongostore@latest
```

Пока репозиторий локальный — в `go.mod` потребителя:

```
replace github.com/SaveliiYam/docstore => ../docstore
replace github.com/SaveliiYam/mongostore => ../mongostore
```

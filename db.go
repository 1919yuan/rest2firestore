package rest2firestore

import (
	"context"
	"fmt"
	"path"

	"cloud.google.com/go/firestore"
)

type Object interface {
	DeserializeList(docs []*firestore.DocumentSnapshot) ([]Object, error)
	SerializeList() ([]map[string]interface{}, error)
	PostprocessList(objs []Object) ([]Object, error)
	Deserialize(doc *firestore.DocumentSnapshot) (Object, error)
	Serialize() (map[string]interface{}, error)
	Search(client *firestore.Client) (document []string, err error)
	Subcollections() []Subcollection
}

type Subcollection struct {
	Name string
	Obj  Object
}

type Db interface {
	List(obj Object, collection []string) ([]Object, error)
	Clear(dummy Object, collection []string) error
	Post(obj Object, collection []string) (Object, error)
	Put(obj Object, collection []string) (Object, error)
	Patch(obj Object) (Object, error)
	Get(dummy Object, document []string) (Object, error)
	Delete(dummy Object, document []string) error
}

type FirestoreDb struct {
	client *firestore.Client
}

var _ Db = &FirestoreDb{}

func getCollectionPath(collection []string) (string, error) {
	collection_path := path.Join(collection...)
	if len(collection) == 0 || len(collection)%2 != 1 {
		return "", fmt.Errorf(
			"%s: collection path levels should be odd.", collection_path)
	}
	return collection_path, nil
}

func getDocumentPath(document []string) (
	collection_path string, document_id string, err error) {
	if len(document) <= 1 {
		collection_path = ""
		document_id = ""
		err = fmt.Errorf(
			"%s: document path levels should be greater than 1.",
			path.Join(document...))
		return
	}
	collection_path = path.Join(document[:len(document)-1]...)
	document_id = document[len(document)-1]
	if len(document)%2 != 0 {
		err = fmt.Errorf(
			"%s: collection path levels should be odd.", collection_path)
		return
	}
	return collection_path, document_id, nil
}

func (db *FirestoreDb) List(obj Object, collection []string) ([]Object, error) {
	ctx := context.Background()
	collection_path, err := getCollectionPath(collection)
	if err != nil {
		return nil, err
	}
	docs, err := db.client.Collection(collection_path).Documents(ctx).GetAll()
	if err != nil {
		return nil, fmt.Errorf(
			"%s:List - could not list objects: %v", collection_path, err)
	}
	if len(docs) == 0 {
		return nil, nil
	}
	objs, err := obj.DeserializeList(docs)
	if err != nil {
		return nil, fmt.Errorf(
			"%s:List - could not deserialize list: %v", collection_path, err)
	}
	return obj.PostprocessList(objs)
}

func (db *FirestoreDb) Clear(dummy Object, collection []string) error {
	ctx := context.Background()
	collection_path, err := getCollectionPath(collection)
	if err != nil {
		return err
	}
	docs, err := db.client.Collection(collection_path).Documents(ctx).GetAll()
	if err != nil {
		return err
	}
	for _, doc := range docs {
		obj, err := db.Get(dummy, append(collection, doc.Ref.ID))
		if err != nil {
			return err
		}
		err = db.Delete(obj, append(collection, doc.Ref.ID))
		if err != nil {
			return err
		}
	}
	return nil
}

func (db *FirestoreDb) Post(obj Object, collection []string) (Object, error) {
	ctx := context.Background()
	existing_document, err := obj.Search(db.client)
	if err != nil {
		return nil, err
	}
	if len(existing_document) > 0 {
		return db.Get(obj, existing_document)
	}
	collection_path, err := getCollectionPath(collection)
	if err != nil {
		return nil, err
	}
	doc, _, err := db.client.Collection(collection_path).Add(ctx, obj)
	if err != nil {
		return nil, fmt.Errorf(
			"%s:Post - could not create object: %v", collection_path, err)
	}
	return db.Get(obj, append(collection, doc.ID))
}

func (db *FirestoreDb) Patch(obj Object) (Object, error) {
	ctx := context.Background()
	existing_document, err := obj.Search(db.client)
	if err != nil {
		return nil, err
	}
	if len(existing_document) == 0 {
		return nil, fmt.Errorf(
			"%s:Patch - could not find object: %v", obj)
	}
	collection_path, document_id, err := getDocumentPath(existing_document)
	if err != nil {
		return nil, err
	}
	doc := db.client.Doc(path.Join(collection_path, document_id))
	if _, err := doc.Get(ctx); err != nil {
		return nil, fmt.Errorf("%s:Patch - no object found: %v", err)
	}
	if _, err := doc.Set(ctx, obj); err != nil {
		return nil, fmt.Errorf("%s:Patch - could not update object: %v", err)
	}
	return db.Get(obj, existing_document)
}

func (db *FirestoreDb) Put(obj Object, collection []string) (Object, error) {
	existing_document, err := obj.Search(db.client)
	if err != nil {
		return nil, err
	}
	if len(existing_document) == 0 {
		return db.Post(obj, collection)
	} else {
		return db.Patch(obj)
	}
}

func (db *FirestoreDb) Get(obj Object, document []string) (Object, error) {
	ctx := context.Background()
	collection_path, document_id, err := getDocumentPath(document)
	if err != nil {
		return nil, err
	}
	doc, err := db.client.Collection(collection_path).Doc(document_id).Get(ctx)
	if err != nil {
		return nil, fmt.Errorf(
			"%s/%s:Get - could not get object: %v", collection_path, document_id, err)
	}
	return obj.Deserialize(doc)
}

func (db *FirestoreDb) Delete(dummy Object, document []string) error {
	ctx := context.Background()
	collection_path, document_id, err := getDocumentPath(document)
	if err != nil {
		return nil
	}
	document_path := path.Join(collection_path, document_id)
	doc := db.client.Doc(document_path)
	subcollections := dummy.Subcollections()
	for _, subcollection := range subcollections {
		err = db.Clear(subcollection.Obj, append(document, subcollection.Name))
		if err != nil {
			return err
		}
	}
	if _, err := doc.Delete(ctx); err != nil {
		return fmt.Errorf("%s:Delete - could not delete object: %v", document_path, err)
	}
	return nil
}

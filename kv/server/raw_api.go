package server

import (
	"context"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// The functions below are Server's Raw API. (implements TinyKvServer).
// Some helper methods can be found in sever.go in the current directory

// RawGet return the corresponding Get response based on RawGetRequest's CF and Key fields
func (server *Server) RawGet(_ context.Context, req *kvrpcpb.RawGetRequest) (*kvrpcpb.RawGetResponse, error) {
	reader, err := server.storage.Reader(nil)
	// cautious you need to close the Txn
	if err != nil {
		return nil, err
	}
	defer reader.Close()

	val, err := reader.GetCF(req.GetCf(), req.GetKey())
	if err != nil {
		return nil, err
	}
	resp := &kvrpcpb.RawGetResponse{
		Value:    val,
		NotFound: false,
	}
	// this design is different hhh
	if val == nil {
		resp.NotFound = true
	}
	return resp, nil
}

// RawPut puts the target data into storage and returns the corresponding response
func (server *Server) RawPut(_ context.Context, req *kvrpcpb.RawPutRequest) (*kvrpcpb.RawPutResponse, error) {
	put := storage.Put{
		Key:   req.GetKey(),
		Value: req.GetValue(),
		Cf:    req.GetCf(),
	}
	batch := []storage.Modify{
		{Data: put},
	}

	err := server.storage.Write(req.Context, batch)
	if err != nil {
		return nil, err
	}
	return &kvrpcpb.RawPutResponse{}, nil
}

// RawDelete delete the target data from storage and returns the corresponding response
func (server *Server) RawDelete(_ context.Context, req *kvrpcpb.RawDeleteRequest) (*kvrpcpb.RawDeleteResponse, error) {
	deleteOps := storage.Delete{
		Key: req.GetKey(),
		Cf:  req.GetCf(),
	}
	batch := []storage.Modify{
		{Data: deleteOps},
	}

	err := server.storage.Write(req.Context, batch)
	if err != nil {
		return nil, err
	}
	return &kvrpcpb.RawDeleteResponse{}, nil
}

// RawScan scan the data starting from the start key up to limit. and return the corresponding result
func (server *Server) RawScan(_ context.Context, req *kvrpcpb.RawScanRequest) (*kvrpcpb.RawScanResponse, error) {
	reader, err := server.storage.Reader(req.Context)

	if err != nil {
		return nil, err
	}
	defer reader.Close()

	// using iter to scan the key scope
	iter := reader.IterCF(req.GetCf())
	defer iter.Close()

	var kvs []*kvrpcpb.KvPair
	limit := req.GetLimit() // limit the scope of the iterator
	startKey := req.GetStartKey()

	for iter.Seek(startKey); iter.Valid(); iter.Next() {
		item := iter.Item()
		key := item.Key()

		val, err := item.ValueCopy(nil)
		if err != nil {
			return nil, err
		}

		kvs = append(kvs, &kvrpcpb.KvPair{
			Key:   key,
			Value: val,
		})

		if uint32(len(kvs)) >= limit {
			break
		}
	}
	return &kvrpcpb.RawScanResponse{
		Kvs: kvs,
	}, nil
}

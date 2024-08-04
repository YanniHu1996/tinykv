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
	// Your Code Here (1).
	sr, err := server.storage.Reader(req.Context)
	if err != nil {
		return nil, err
	}
	data, err := sr.GetCF(req.Cf, req.Key)
	if err != nil {
		return nil, err
	}
	return &kvrpcpb.RawGetResponse{Value: data, NotFound: data == nil}, nil
}

// RawPut puts the target data into storage and returns the corresponding response
func (server *Server) RawPut(_ context.Context, req *kvrpcpb.RawPutRequest) (*kvrpcpb.RawPutResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be modified
	server.storage.Write(req.Context, []storage.Modify{
		{
			Data: storage.Put{
				Cf:    req.Cf,
				Key:   req.Key,
				Value: req.Value,
			},
		},
	})
	return nil, nil
}

// RawDelete delete the target data from storage and returns the corresponding response
func (server *Server) RawDelete(_ context.Context, req *kvrpcpb.RawDeleteRequest) (*kvrpcpb.RawDeleteResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be deleted
	server.storage.Write(req.Context, []storage.Modify{
		{
			Data: storage.Delete{
				Cf:  req.Cf,
				Key: req.Key,
			},
		},
	})
	return nil, nil
}

// RawScan scan the data starting from the start key up to limit. and return the corresponding result
func (server *Server) RawScan(_ context.Context, req *kvrpcpb.RawScanRequest) (*kvrpcpb.RawScanResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using reader.IterCF
	sr, err := server.storage.Reader(req.Context)
	if err != nil {
		return nil, err
	}
	iter := sr.IterCF(req.Cf)
	defer iter.Close()

	resp := &kvrpcpb.RawScanResponse{}

	for iter.Seek(req.StartKey); iter.Valid(); iter.Next() {
		if req.Limit == 0 {
			break
		}
		req.Limit--

		item := iter.Item()
		val, err := item.Value()
		if err != nil {
			return nil, err
		}

		resp.Kvs = append(resp.Kvs, &kvrpcpb.KvPair{
			Key:   item.Key(),
			Value: val,
		})
	}
	return resp, nil
}

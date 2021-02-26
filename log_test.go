package main

import (
	"encoding/json"
	"testing"
)

func TestLogParsing(t *testing.T) {
	input := []byte(`{"t":{"$date":"2021-02-27T01:09:52.910+03:00"},"s":"I",  
"c":"NETWORK",  "id":51800,   "ctx":"conn3","msg":"client metadata","attr":
{"remote":"127.0.0.1:50410","client":"conn3",
"doc":{"driver":{"name":"mongo-go-driver","version":"v1.4.6"},
"os":{"type":"linux","architecture":"amd64"},"platform":"go1.16"}}}`)
	var e Entry
	if err := json.Unmarshal(input, &e); err != nil {
		t.Fatal(err)
	}
	t.Logf("%+v", e)
}

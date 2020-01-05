package persistence

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"github.com/elastic/go-elasticsearch/v7"
	"github.com/elastic/go-elasticsearch/v7/esapi"
	"github.com/pkg/errors"
	"log"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

const (
	esIndexName string = "magnetico"
)

var docsPool = make(chan torrentDocTypeWithId, 300)
var esBulkTrigger = make(chan bool)

type esDatabase struct {
	conn *elasticsearch.Client
}

type torrentDocType struct {
	Name string `json:"name"`
	Files []File `json:"files"`
	Timestamp int64 `json:"timestamp"`
}

type torrentDocTypeWithId struct {
	DocId string
	Doc  *torrentDocType
}

type hitType struct {
	DocId string `json:"_id"`
	Doc   torrentDocType `json:"_source"`
}

type hitsType struct {
	Hits struct {
		Hits[]hitType `json:"hits"`
	} `json:"hits"`
}

type bucketType struct {
	Key string `json:"key"`
	DocCount int `json:"doc_count"`
	Child struct {
		Buckets []bucketType `json:"buckets"`
	}`json:"child"`
}

type aggreationType struct {
	Aggregations struct {
		DocInfo struct {
			Buckets []bucketType `json:"buckets"`
		}`json:"doc_count"`
	} `json:"aggregations"`
}

func (e *esDatabase) QueryTorrentById(id uint) (infoHash []byte, name string, files []File, discorveredTime int64, err error) {
	panic("implement me")
}

func (e *esDatabase) addBulk(docId string, doc *torrentDocType) {
	docsPool <- torrentDocTypeWithId{docId, doc}
}


func (e *esDatabase) bulkDaemon() {
	bulkedDocCount := 0
	body := ""
	for true {
		select {
		case fulldoc := <- docsPool:
			bulkedDocCount ++
			docStr, err := json.Marshal(fulldoc.Doc)
			if err != nil {
				log.Fatalf("Failed to marshal docId %s while bulking. err: %v\n",
					fulldoc.DocId, err)
			}
			// create, not index, to avoid any updating(overwriting)
			body += fmt.Sprintf(`{"create": {"_index": "%s", "_id": "%s"}}` + "\n", esIndexName, fulldoc.DocId)
			body += string(docStr) + "\n"
			if bulkedDocCount >= 100 {
				esBulkTrigger <- true
			}

		case <-esBulkTrigger:
			if len(body) > 0 {
				res, err := e.conn.Bulk(strings.NewReader(body))
				if err != nil {
					statusCode := 0
					if res!=nil {
						statusCode = res.StatusCode
					}
					log.Fatalf("Failed to bulk. err: %v, status code: %d\nContent:\n%s", err, statusCode, body)
				}
			}
			body = ""
			bulkedDocCount = 0
		}

	}
}

func (e *esDatabase) AddNewTorrent(infoHash []byte, name string, files []File, discorveredTime int64) error {
	docId := hex.EncodeToString(infoHash)
	// do not check exists for better performance
	/*
		res, err := e.conn.Exists(esIndexName, docId)
		if err != nil {
			return err
		}
		if res.StatusCode == 200 {
			// exit if exists
			return nil
		}
		// res.StatusCode == 404
	*/
	doc := &torrentDocType{
		Name:  name,
		Files: files,
		Timestamp: discorveredTime * 1000,
	}
	e.addBulk(docId, doc)
	return nil
}

func (e *esDatabase) Close() error {
	esBulkTrigger <- true
	return e.Close()
}

func (e *esDatabase) DoesTorrentExist(infoHash []byte) (bool, error) {
	res, err := e.conn.Exists(esIndexName, hex.EncodeToString(infoHash))
	if err != nil || res.StatusCode == 200 {
		return true, nil
	} else {
		return false, nil
	}
}

func (e *esDatabase) Engine() databaseEngine {
	return Elasticsearch
}

func (e *esDatabase) GetFiles(infoHash []byte) ([]File, error) {
	res, err := e.conn.Get(esIndexName, hex.EncodeToString(infoHash))
	if err != nil || res.StatusCode != 200 {	// 404
		return nil, err
	}
	var hit hitType
	if err := json.NewDecoder(res.Body).Decode(&hit); err != nil {
		return nil, err
	} else {
		return hit.Doc.Files, nil
	}
}

func (e *esDatabase) GetNumberOfTorrents() (uint, error) {
	res, err := e.conn.Cat.Count(func(request *esapi.CatCountRequest) {
		request.Index = []string{esIndexName}
	})
	if err != nil {
		return 0, err
	}
	infos := strings.Split(res.String(), " ")
	count, err := strconv.Atoi(strings.TrimSpace(infos[len(infos) - 1]))
	if err != nil {
		return 0, err
	}
	return uint(count), nil
}

func (e *esDatabase) GetStatistics(from string, n uint) (*Statistics, error) {
	pfromTime, gran, err := ParseISO8601(from)
	if err != nil {
		return nil, errors.Wrap(err, "parsing ISO8601 error")
	}
	fromTime := *pfromTime
	var toTime time.Time
	var dateFormat = ""
	switch gran {
	case Year:
		toTime = fromTime.AddDate(int(n), 0, 0)
		dateFormat = "year"
	case Month:
		toTime = fromTime.AddDate(0, int(n), 0)
		dateFormat = "yyyy-MM"
	case Week:
		toTime = fromTime.AddDate(0, 0, int(n)*7)
		dateFormat = "yyyy-ww"
	case Day:
		toTime = fromTime.AddDate(0, 0, int(n))
		dateFormat = "date"
	case Hour:
		toTime = fromTime.Add(time.Duration(n) * time.Hour)
		dateFormat = "YYYY-MM-DD'T'HH"
	}

	var getData = func(format string, ft time.Time, tt time.Time) (*bucketType, error) {
		queryStr := fmt.Sprintf(format, ft.Unix()*1000, tt.Unix()*1000, dateFormat)
		res, err := e.conn.Search(func(request *esapi.SearchRequest) {
			request.Index = []string {esIndexName}
			request.Timeout = 0
			request.Body = strings.NewReader(queryStr)
		})
		if err != nil || res.StatusCode != 200{
			return nil, err
		}
		var agg aggreationType
		err = json.NewDecoder(res.Body).Decode(&agg)
		if err != nil {
			return nil, err
		}
		buckets := agg.Aggregations.DocInfo.Buckets
		if len(buckets) > 0 {
			return &buckets[0], nil
		} else {
			return nil, nil
		}
	}

	formatDoc := `{
    "size": 0,
	"_source": {"excludes": ["files"]},
    "aggs": {
        "doc_count": {
            "range" : {
                "field": "timestamp", 
                "ranges" : [{
                    "from" : %d,
                    "to" : %d
                }]
            },
			"aggs": {
				"child": {
					"terms" : {
						"script": "DateTimeFormatter.ofPattern('` + dateFormat + `').format(doc.timestamp.value)"
					}
				}
			}
        }
    }
}
`
	var statistics = Statistics{
		NDiscovered: map[string]uint64{},
		NFiles:      map[string]uint64{},
		TotalSize:   map[string]uint64{},
	}
	buckets, err := getData(formatDoc, fromTime, toTime)
	if err != nil || buckets == nil {
		return &statistics, err
	}
	for _, b := range buckets.Child.Buckets {
		statistics.NDiscovered[b.Key] = uint64(b.DocCount)
	}

	// TODO: total filesize and filecount

	return &statistics, nil
}

func (doc* torrentDocType) ToTorrentMetadata(docId string, id int64) (*TorrentMetadata, error) {
	hashHex, err := hex.DecodeString(docId)
	if err != nil {
		return nil, err
	}
	var filesize int64 = 0
	for _, f := range doc.Files {
		filesize += f.Size
	}
	return &TorrentMetadata{
		ID:           uint64(id),
		InfoHash:     hashHex,
		Name:         doc.Name,
		Size:         uint64(filesize),
		DiscoveredOn: doc.Timestamp,
		NFiles:       uint(len(doc.Files)),
		Relevance:    0,
	}, nil
}

func (e *esDatabase) GetTorrent(infoHash []byte) (*TorrentMetadata, error) {
	res, err := e.conn.Get(esIndexName, hex.EncodeToString(infoHash))
	if err != nil || res.StatusCode != 200 {	// 404
		return nil, err
	}
	var hit hitType
	if err := json.NewDecoder(res.Body).Decode(&hit); err != nil {
		return nil, err
	} else {
		return hit.Doc.ToTorrentMetadata(hit.DocId, 0)
	}
}

func (e *esDatabase) QueryTorrents(
	query string,
	epoch int64,
	orderBy OrderingCriteria,
	ascending bool,
	limit uint,
	lastOrderedValue *float64,
	lastID *uint64,
) ([]TorrentMetadata, error) {
	if query == "" {
		query = "*"
	}
	query = strings.ReplaceAll(query, `"`, `\"`)
	ascStr := "desc"
	if ascending {
		ascStr = "asc"
	}

	orderByStr := `"_score": {"order": "` + ascStr + `"}`
	switch orderBy {
	case ByRelevance:
		orderByStr = `"_score": {"order": "` + ascStr + `"}`
	case ByTotalSize:
		orderByStr = `"_script": {"script": {"lang": "painless", "source": """
int filesize = 0;
for(int i=0; i<doc['files'].length; i++) {
    filesize += doc['files'][i]['size'];
}
"""}, "type": "number", "order": "` + ascStr + `"}`
	case ByNFiles:
		orderByStr = `"_script": { "script": "doc['files'].value.length()", "type": "number", "order": "` + ascStr + `"}`
	case ByDiscoveredOn:
		orderByStr = `"timestamp": {"order": "` + ascStr + `"}`
	}

	var lastID_ int64 = 0
	if lastID == nil {
		lastID_ = 0
	} else {
		lastID_ = int64(*lastID)
	}

	queryStr := `{
  "from": ` + strconv.FormatInt(lastID_, 10) + `,
  "size": `+ strconv.FormatInt(int64(limit), 10) + `,
  "sort": [
    {
      ` + orderByStr + `
    }
  ],
  "_source": {
    "excludes": ["files.path"]
  },
  "stored_fields": [
    "*"
  ],
  "query": {
    "bool": {
      "must": [
        {
          "query_string": {
            "query": "` + query + `",
            "analyze_wildcard": true
          }
        }
      ]
    }
  }
}
`
	res, err := e.conn.Search(func(request *esapi.SearchRequest) {
		request.Index = []string{esIndexName}
		request.Timeout = 0
		request.Body = strings.NewReader(queryStr)
	})
	if err != nil {
		return nil, err
	}

	var hits hitsType
	err = json.NewDecoder(res.Body).Decode(&hits)
	if err != nil {
		return nil, err
	}
	var result []TorrentMetadata
	for i, hit := range hits.Hits.Hits {
		metadata, err := hit.Doc.ToTorrentMetadata(hit.DocId, lastID_ + int64(i))
		if err != nil {
			return nil, err
		}
		result = append(result, *metadata)
	}

	return result, nil
}

func makeElasticsearchDatabase(dataSource string) (Database, error) {
	url_, err := url.Parse(dataSource)
	if err != nil {
		return nil, nil
	}

	address := []string {url_.Scheme + "://" + url_.Host}
	username := url_.User.Username()
	password, _ := url_.User.Password()
	cloudid := url_.Query().Get("cloudid")
	apikey := url_.Query().Get("apikey")

	cfg := elasticsearch.Config{
		Addresses: address,
		Username:  username,
		Password:  password,
		CloudID:   cloudid,
		APIKey:    apikey,
		Transport: &http.Transport{
			DialContext:            (&net.Dialer{Timeout: 0}).DialContext,
		},
	}
	es , err := elasticsearch.NewClient(cfg)
	if err != nil {
		return nil, err
	}

	_, err = es.Ping()
	if err != nil {
		return nil, err
	}

	res, err := es.Indices.Get([]string{esIndexName})
	if err != nil {
		return nil, err
	}
	if res.IsError() {
		// index not exists
		res, err = es.Indices.Create(esIndexName, func(request *esapi.IndicesCreateRequest) {
			request.Body = strings.NewReader(`
{
	"settings" : {
		"index" : {
			"number_of_shards" : 8, 
			"number_of_replicas" : 0 
		}
	},
	"mappings": {
		"properties": {
			"name": { "type": "text" },
			"files": {
				"properties": {
					"size": { "type": "long" },
					"path": { "type": "text" }
				}
			},
			"timestamp": {"type": "date"}
		}
	}
}
`)
		})
	}

	db := esDatabase{conn:es}

	go db.bulkDaemon()

	return &db, nil
}
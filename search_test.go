package dicedb_test

import (
	"context"
	"time"

	. "github.com/bsm/ginkgo/v2"
	. "github.com/bsm/gomega"
	"github.com/dicedb/go-dice"
)

func WaitForIndexing(c *dicedb.Client, index string) {
	for {
		res, err := c.FTInfo(context.Background(), index).Result()
		Expect(err).NotTo(HaveOccurred())
		if c.Options().Protocol == 2 {
			if res.Indexing == 0 {
				return
			}
			time.Sleep(100 * time.Millisecond)
		}
	}
}

var _ = Describe("RediSearch commands", Label("search"), func() {
	ctx := context.TODO()
	var client *dicedb.Client

	BeforeEach(func() {
		client = dicedb.NewClient(&dicedb.Options{Addr: ":6379", Protocol: 2})
		Expect(client.FlushDB(ctx).Err()).NotTo(HaveOccurred())
	})

	AfterEach(func() {
		Expect(client.Close()).NotTo(HaveOccurred())
	})

	It("should FTCreate and FTSearch WithScores", Label("search", "ftcreate", "ftsearch"), func() {
		val, err := client.FTCreate(ctx, "txt", &dicedb.FTCreateOptions{}, &dicedb.FieldSchema{FieldName: "txt", FieldType: dicedb.SearchFieldTypeText}).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(val).To(BeEquivalentTo("OK"))
		WaitForIndexing(client, "txt")
		client.HSet(ctx, "doc1", "txt", "foo baz")
		client.HSet(ctx, "doc2", "txt", "foo bar")
		res, err := client.FTSearchWithArgs(ctx, "txt", "foo ~bar", &dicedb.FTSearchOptions{WithScores: true}).Result()

		Expect(err).NotTo(HaveOccurred())
		Expect(res.Total).To(BeEquivalentTo(int64(2)))
		for _, doc := range res.Docs {
			Expect(*doc.Score).To(BeNumerically(">", 0))
			Expect(doc.ID).To(Or(Equal("doc1"), Equal("doc2")))
		}
	})

	It("should FTCreate and FTSearch stopwords", Label("search", "ftcreate", "ftsearch"), func() {
		val, err := client.FTCreate(ctx, "txt", &dicedb.FTCreateOptions{StopWords: []interface{}{"foo", "bar", "baz"}}, &dicedb.FieldSchema{FieldName: "txt", FieldType: dicedb.SearchFieldTypeText}).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(val).To(BeEquivalentTo("OK"))
		WaitForIndexing(client, "txt")
		client.HSet(ctx, "doc1", "txt", "foo baz")
		client.HSet(ctx, "doc2", "txt", "hello world")
		res1, err := client.FTSearchWithArgs(ctx, "txt", "foo bar", &dicedb.FTSearchOptions{NoContent: true}).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(res1.Total).To(BeEquivalentTo(int64(0)))
		res2, err := client.FTSearchWithArgs(ctx, "txt", "foo bar hello world", &dicedb.FTSearchOptions{NoContent: true}).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(res2.Total).To(BeEquivalentTo(int64(1)))
	})

	It("should FTCreate and FTSearch filters", Label("search", "ftcreate", "ftsearch"), func() {
		val, err := client.FTCreate(ctx, "txt", &dicedb.FTCreateOptions{}, &dicedb.FieldSchema{FieldName: "txt", FieldType: dicedb.SearchFieldTypeText}, &dicedb.FieldSchema{FieldName: "num", FieldType: dicedb.SearchFieldTypeNumeric}, &dicedb.FieldSchema{FieldName: "loc", FieldType: dicedb.SearchFieldTypeGeo}).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(val).To(BeEquivalentTo("OK"))
		WaitForIndexing(client, "txt")
		client.HSet(ctx, "doc1", "txt", "foo bar", "num", 3.141, "loc", "-0.441,51.458")
		client.HSet(ctx, "doc2", "txt", "foo baz", "num", 2, "loc", "-0.1,51.2")
		res1, err := client.FTSearchWithArgs(ctx, "txt", "foo", &dicedb.FTSearchOptions{Filters: []dicedb.FTSearchFilter{{FieldName: "num", Min: 0, Max: 2}}, NoContent: true}).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(res1.Total).To(BeEquivalentTo(int64(1)))
		Expect(res1.Docs[0].ID).To(BeEquivalentTo("doc2"))
		res2, err := client.FTSearchWithArgs(ctx, "txt", "foo", &dicedb.FTSearchOptions{Filters: []dicedb.FTSearchFilter{{FieldName: "num", Min: 0, Max: "+inf"}}, NoContent: true}).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(res2.Total).To(BeEquivalentTo(int64(2)))
		Expect(res2.Docs[0].ID).To(BeEquivalentTo("doc1"))
		// Test Geo filter
		geoFilter1 := dicedb.FTSearchGeoFilter{FieldName: "loc", Longitude: -0.44, Latitude: 51.45, Radius: 10, Unit: "km"}
		geoFilter2 := dicedb.FTSearchGeoFilter{FieldName: "loc", Longitude: -0.44, Latitude: 51.45, Radius: 100, Unit: "km"}
		res3, err := client.FTSearchWithArgs(ctx, "txt", "foo", &dicedb.FTSearchOptions{GeoFilter: []dicedb.FTSearchGeoFilter{geoFilter1}, NoContent: true}).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(res3.Total).To(BeEquivalentTo(int64(1)))
		Expect(res3.Docs[0].ID).To(BeEquivalentTo("doc1"))
		res4, err := client.FTSearchWithArgs(ctx, "txt", "foo", &dicedb.FTSearchOptions{GeoFilter: []dicedb.FTSearchGeoFilter{geoFilter2}, NoContent: true}).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(res4.Total).To(BeEquivalentTo(int64(2)))
		docs := []interface{}{res4.Docs[0].ID, res4.Docs[1].ID}
		Expect(docs).To(ContainElement("doc1"))
		Expect(docs).To(ContainElement("doc2"))

	})

	It("should FTCreate and FTSearch sortby", Label("search", "ftcreate", "ftsearch"), func() {
		val, err := client.FTCreate(ctx, "num", &dicedb.FTCreateOptions{}, &dicedb.FieldSchema{FieldName: "txt", FieldType: dicedb.SearchFieldTypeText}, &dicedb.FieldSchema{FieldName: "num", FieldType: dicedb.SearchFieldTypeNumeric, Sortable: true}).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(val).To(BeEquivalentTo("OK"))
		WaitForIndexing(client, "num")
		client.HSet(ctx, "doc1", "txt", "foo bar", "num", 1)
		client.HSet(ctx, "doc2", "txt", "foo baz", "num", 2)
		client.HSet(ctx, "doc3", "txt", "foo qux", "num", 3)

		sortBy1 := dicedb.FTSearchSortBy{FieldName: "num", Asc: true}
		sortBy2 := dicedb.FTSearchSortBy{FieldName: "num", Desc: true}
		res1, err := client.FTSearchWithArgs(ctx, "num", "foo", &dicedb.FTSearchOptions{NoContent: true, SortBy: []dicedb.FTSearchSortBy{sortBy1}}).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(res1.Total).To(BeEquivalentTo(int64(3)))
		Expect(res1.Docs[0].ID).To(BeEquivalentTo("doc1"))
		Expect(res1.Docs[1].ID).To(BeEquivalentTo("doc2"))
		Expect(res1.Docs[2].ID).To(BeEquivalentTo("doc3"))

		res2, err := client.FTSearchWithArgs(ctx, "num", "foo", &dicedb.FTSearchOptions{NoContent: true, SortBy: []dicedb.FTSearchSortBy{sortBy2}}).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(res2.Total).To(BeEquivalentTo(int64(3)))
		Expect(res2.Docs[2].ID).To(BeEquivalentTo("doc1"))
		Expect(res2.Docs[1].ID).To(BeEquivalentTo("doc2"))
		Expect(res2.Docs[0].ID).To(BeEquivalentTo("doc3"))

	})

	It("should FTCreate and FTSearch example", Label("search", "ftcreate", "ftsearch"), func() {
		val, err := client.FTCreate(ctx, "txt", &dicedb.FTCreateOptions{}, &dicedb.FieldSchema{FieldName: "title", FieldType: dicedb.SearchFieldTypeText, Weight: 5}, &dicedb.FieldSchema{FieldName: "body", FieldType: dicedb.SearchFieldTypeText}).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(val).To(BeEquivalentTo("OK"))
		WaitForIndexing(client, "txt")
		client.HSet(ctx, "doc1", "title", "RediSearch", "body", "Redisearch impements a search engine on top of redis")
		res1, err := client.FTSearchWithArgs(ctx, "txt", "search engine", &dicedb.FTSearchOptions{NoContent: true, Verbatim: true, LimitOffset: 0, Limit: 5}).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(res1.Total).To(BeEquivalentTo(int64(1)))

	})

	It("should FTCreate NoIndex", Label("search", "ftcreate", "ftsearch"), func() {
		text1 := &dicedb.FieldSchema{FieldName: "field", FieldType: dicedb.SearchFieldTypeText}
		text2 := &dicedb.FieldSchema{FieldName: "text", FieldType: dicedb.SearchFieldTypeText, NoIndex: true, Sortable: true}
		num := &dicedb.FieldSchema{FieldName: "numeric", FieldType: dicedb.SearchFieldTypeNumeric, NoIndex: true, Sortable: true}
		geo := &dicedb.FieldSchema{FieldName: "geo", FieldType: dicedb.SearchFieldTypeGeo, NoIndex: true, Sortable: true}
		tag := &dicedb.FieldSchema{FieldName: "tag", FieldType: dicedb.SearchFieldTypeTag, NoIndex: true, Sortable: true}
		val, err := client.FTCreate(ctx, "idx", &dicedb.FTCreateOptions{}, text1, text2, num, geo, tag).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(val).To(BeEquivalentTo("OK"))
		WaitForIndexing(client, "idx")
		client.HSet(ctx, "doc1", "field", "aaa", "text", "1", "numeric", 1, "geo", "1,1", "tag", "1")
		client.HSet(ctx, "doc2", "field", "aab", "text", "2", "numeric", 2, "geo", "2,2", "tag", "2")
		res1, err := client.FTSearch(ctx, "idx", "@text:aa*").Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(res1.Total).To(BeEquivalentTo(int64(0)))
		res2, err := client.FTSearch(ctx, "idx", "@field:aa*").Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(res2.Total).To(BeEquivalentTo(int64(2)))
		res3, err := client.FTSearchWithArgs(ctx, "idx", "*", &dicedb.FTSearchOptions{SortBy: []dicedb.FTSearchSortBy{{FieldName: "text", Desc: true}}}).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(res3.Total).To(BeEquivalentTo(int64(2)))
		Expect(res3.Docs[0].ID).To(BeEquivalentTo("doc2"))
		res4, err := client.FTSearchWithArgs(ctx, "idx", "*", &dicedb.FTSearchOptions{SortBy: []dicedb.FTSearchSortBy{{FieldName: "text", Asc: true}}}).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(res4.Total).To(BeEquivalentTo(int64(2)))
		Expect(res4.Docs[0].ID).To(BeEquivalentTo("doc1"))
		res5, err := client.FTSearchWithArgs(ctx, "idx", "*", &dicedb.FTSearchOptions{SortBy: []dicedb.FTSearchSortBy{{FieldName: "numeric", Asc: true}}}).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(res5.Docs[0].ID).To(BeEquivalentTo("doc1"))
		res6, err := client.FTSearchWithArgs(ctx, "idx", "*", &dicedb.FTSearchOptions{SortBy: []dicedb.FTSearchSortBy{{FieldName: "geo", Asc: true}}}).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(res6.Docs[0].ID).To(BeEquivalentTo("doc1"))
		res7, err := client.FTSearchWithArgs(ctx, "idx", "*", &dicedb.FTSearchOptions{SortBy: []dicedb.FTSearchSortBy{{FieldName: "tag", Asc: true}}}).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(res7.Docs[0].ID).To(BeEquivalentTo("doc1"))

	})

	It("should FTExplain", Label("search", "ftexplain"), func() {
		text1 := &dicedb.FieldSchema{FieldName: "f1", FieldType: dicedb.SearchFieldTypeText}
		text2 := &dicedb.FieldSchema{FieldName: "f2", FieldType: dicedb.SearchFieldTypeText}
		text3 := &dicedb.FieldSchema{FieldName: "f3", FieldType: dicedb.SearchFieldTypeText}
		val, err := client.FTCreate(ctx, "txt", &dicedb.FTCreateOptions{}, text1, text2, text3).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(val).To(BeEquivalentTo("OK"))
		WaitForIndexing(client, "txt")
		res1, err := client.FTExplain(ctx, "txt", "@f3:f3_val @f2:f2_val @f1:f1_val").Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(res1).ToNot(BeEmpty())

	})

	It("should FTAlias", Label("search", "ftexplain"), func() {
		text1 := &dicedb.FieldSchema{FieldName: "name", FieldType: dicedb.SearchFieldTypeText}
		text2 := &dicedb.FieldSchema{FieldName: "name", FieldType: dicedb.SearchFieldTypeText}
		val1, err := client.FTCreate(ctx, "testAlias", &dicedb.FTCreateOptions{Prefix: []interface{}{"index1:"}}, text1).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(val1).To(BeEquivalentTo("OK"))
		WaitForIndexing(client, "testAlias")
		val2, err := client.FTCreate(ctx, "testAlias2", &dicedb.FTCreateOptions{Prefix: []interface{}{"index2:"}}, text2).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(val2).To(BeEquivalentTo("OK"))
		WaitForIndexing(client, "testAlias2")

		client.HSet(ctx, "index1:lonestar", "name", "lonestar")
		client.HSet(ctx, "index2:yogurt", "name", "yogurt")

		res1, err := client.FTSearch(ctx, "testAlias", "*").Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(res1.Docs[0].ID).To(BeEquivalentTo("index1:lonestar"))

		aliasAddRes, err := client.FTAliasAdd(ctx, "testAlias", "mj23").Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(aliasAddRes).To(BeEquivalentTo("OK"))

		res1, err = client.FTSearch(ctx, "mj23", "*").Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(res1.Docs[0].ID).To(BeEquivalentTo("index1:lonestar"))

		aliasUpdateRes, err := client.FTAliasUpdate(ctx, "testAlias2", "kb24").Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(aliasUpdateRes).To(BeEquivalentTo("OK"))

		res3, err := client.FTSearch(ctx, "kb24", "*").Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(res3.Docs[0].ID).To(BeEquivalentTo("index2:yogurt"))

		aliasDelRes, err := client.FTAliasDel(ctx, "mj23").Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(aliasDelRes).To(BeEquivalentTo("OK"))

	})

	It("should FTCreate and FTSearch textfield, sortable and nostem ", Label("search", "ftcreate", "ftsearch"), func() {
		val, err := client.FTCreate(ctx, "idx1", &dicedb.FTCreateOptions{}, &dicedb.FieldSchema{FieldName: "txt", FieldType: dicedb.SearchFieldTypeText, Sortable: true, NoStem: true}).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(val).To(BeEquivalentTo("OK"))
		WaitForIndexing(client, "idx1")

		resInfo, err := client.FTInfo(ctx, "idx1").Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(resInfo.Attributes[0].Sortable).To(BeTrue())
		Expect(resInfo.Attributes[0].NoStem).To(BeTrue())

	})

	It("should FTAlter", Label("search", "ftcreate", "ftsearch", "ftalter"), func() {
		val, err := client.FTCreate(ctx, "idx1", &dicedb.FTCreateOptions{}, &dicedb.FieldSchema{FieldName: "txt", FieldType: dicedb.SearchFieldTypeText}).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(val).To(BeEquivalentTo("OK"))
		WaitForIndexing(client, "idx1")

		resAlter, err := client.FTAlter(ctx, "idx1", false, []interface{}{"body", dicedb.SearchFieldTypeText.String()}).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(resAlter).To(BeEquivalentTo("OK"))

		client.HSet(ctx, "doc1", "title", "MyTitle", "body", "Some content only in the body")
		res1, err := client.FTSearch(ctx, "idx1", "only in the body").Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(res1.Total).To(BeEquivalentTo(int64(1)))

	})

	It("should FTSpellCheck", Label("search", "ftcreate", "ftsearch", "ftspellcheck"), func() {
		text1 := &dicedb.FieldSchema{FieldName: "f1", FieldType: dicedb.SearchFieldTypeText}
		text2 := &dicedb.FieldSchema{FieldName: "f2", FieldType: dicedb.SearchFieldTypeText}
		val, err := client.FTCreate(ctx, "idx1", &dicedb.FTCreateOptions{}, text1, text2).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(val).To(BeEquivalentTo("OK"))
		WaitForIndexing(client, "idx1")

		client.HSet(ctx, "doc1", "f1", "some valid content", "f2", "this is sample text")
		client.HSet(ctx, "doc2", "f1", "very important", "f2", "lorem ipsum")

		resSpellCheck, err := client.FTSpellCheck(ctx, "idx1", "impornant").Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(resSpellCheck[0].Suggestions[0].Suggestion).To(BeEquivalentTo("important"))

		resSpellCheck2, err := client.FTSpellCheck(ctx, "idx1", "contnt").Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(resSpellCheck2[0].Suggestions[0].Suggestion).To(BeEquivalentTo("content"))

		// test spellcheck with Levenshtein distance
		resSpellCheck3, err := client.FTSpellCheck(ctx, "idx1", "vlis").Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(resSpellCheck3[0].Term).To(BeEquivalentTo("vlis"))

		resSpellCheck4, err := client.FTSpellCheckWithArgs(ctx, "idx1", "vlis", &dicedb.FTSpellCheckOptions{Distance: 2}).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(resSpellCheck4[0].Suggestions[0].Suggestion).To(BeEquivalentTo("valid"))

		// test spellcheck include
		resDictAdd, err := client.FTDictAdd(ctx, "dict", "lore", "lorem", "lorm").Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(resDictAdd).To(BeEquivalentTo(3))
		terms := &dicedb.FTSpellCheckTerms{Inclusion: "INCLUDE", Dictionary: "dict"}
		resSpellCheck5, err := client.FTSpellCheckWithArgs(ctx, "idx1", "lorm", &dicedb.FTSpellCheckOptions{Terms: terms}).Result()
		Expect(err).NotTo(HaveOccurred())
		lorm := resSpellCheck5[0].Suggestions
		Expect(len(lorm)).To(BeEquivalentTo(3))
		Expect(lorm[0].Score).To(BeEquivalentTo(0.5))
		Expect(lorm[1].Score).To(BeEquivalentTo(0))
		Expect(lorm[2].Score).To(BeEquivalentTo(0))

		terms2 := &dicedb.FTSpellCheckTerms{Inclusion: "EXCLUDE", Dictionary: "dict"}
		resSpellCheck6, err := client.FTSpellCheckWithArgs(ctx, "idx1", "lorm", &dicedb.FTSpellCheckOptions{Terms: terms2}).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(resSpellCheck6).To(BeEmpty())
	})

	It("should FTDict opreations", Label("search", "ftdictdump", "ftdictdel", "ftdictadd"), func() {
		text1 := &dicedb.FieldSchema{FieldName: "f1", FieldType: dicedb.SearchFieldTypeText}
		text2 := &dicedb.FieldSchema{FieldName: "f2", FieldType: dicedb.SearchFieldTypeText}
		val, err := client.FTCreate(ctx, "idx1", &dicedb.FTCreateOptions{}, text1, text2).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(val).To(BeEquivalentTo("OK"))
		WaitForIndexing(client, "idx1")

		resDictAdd, err := client.FTDictAdd(ctx, "custom_dict", "item1", "item2", "item3").Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(resDictAdd).To(BeEquivalentTo(3))

		resDictDel, err := client.FTDictDel(ctx, "custom_dict", "item2").Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(resDictDel).To(BeEquivalentTo(1))

		resDictDump, err := client.FTDictDump(ctx, "custom_dict").Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(resDictDump).To(BeEquivalentTo([]string{"item1", "item3"}))

		resDictDel2, err := client.FTDictDel(ctx, "custom_dict", "item1", "item3").Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(resDictDel2).To(BeEquivalentTo(2))
	})

	It("should FTSearch phonetic matcher", Label("search", "ftsearch"), func() {
		text1 := &dicedb.FieldSchema{FieldName: "name", FieldType: dicedb.SearchFieldTypeText}
		val, err := client.FTCreate(ctx, "idx1", &dicedb.FTCreateOptions{}, text1).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(val).To(BeEquivalentTo("OK"))
		WaitForIndexing(client, "idx1")

		client.HSet(ctx, "doc1", "name", "Jon")
		client.HSet(ctx, "doc2", "name", "John")

		res1, err := client.FTSearch(ctx, "idx1", "Jon").Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(res1.Total).To(BeEquivalentTo(int64(1)))
		Expect(res1.Docs[0].Fields["name"]).To(BeEquivalentTo("Jon"))

		client.FlushDB(ctx)
		text2 := &dicedb.FieldSchema{FieldName: "name", FieldType: dicedb.SearchFieldTypeText, PhoneticMatcher: "dm:en"}
		val2, err := client.FTCreate(ctx, "idx1", &dicedb.FTCreateOptions{}, text2).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(val2).To(BeEquivalentTo("OK"))
		WaitForIndexing(client, "idx1")

		client.HSet(ctx, "doc1", "name", "Jon")
		client.HSet(ctx, "doc2", "name", "John")

		res2, err := client.FTSearch(ctx, "idx1", "Jon").Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(res2.Total).To(BeEquivalentTo(int64(2)))
		names := []interface{}{res2.Docs[0].Fields["name"], res2.Docs[1].Fields["name"]}
		Expect(names).To(ContainElement("Jon"))
		Expect(names).To(ContainElement("John"))
	})

	It("should FTSearch WithScores", Label("search", "ftsearch"), func() {
		text1 := &dicedb.FieldSchema{FieldName: "description", FieldType: dicedb.SearchFieldTypeText}
		val, err := client.FTCreate(ctx, "idx1", &dicedb.FTCreateOptions{}, text1).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(val).To(BeEquivalentTo("OK"))
		WaitForIndexing(client, "idx1")

		client.HSet(ctx, "doc1", "description", "The quick brown fox jumps over the lazy dog")
		client.HSet(ctx, "doc2", "description", "Quick alice was beginning to get very tired of sitting by her quick sister on the bank, and of having nothing to do.")

		res, err := client.FTSearchWithArgs(ctx, "idx1", "quick", &dicedb.FTSearchOptions{WithScores: true}).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(*res.Docs[0].Score).To(BeEquivalentTo(float64(1)))

		res, err = client.FTSearchWithArgs(ctx, "idx1", "quick", &dicedb.FTSearchOptions{WithScores: true, Scorer: "TFIDF"}).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(*res.Docs[0].Score).To(BeEquivalentTo(float64(1)))

		res, err = client.FTSearchWithArgs(ctx, "idx1", "quick", &dicedb.FTSearchOptions{WithScores: true, Scorer: "TFIDF.DOCNORM"}).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(*res.Docs[0].Score).To(BeEquivalentTo(0.14285714285714285))

		res, err = client.FTSearchWithArgs(ctx, "idx1", "quick", &dicedb.FTSearchOptions{WithScores: true, Scorer: "BM25"}).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(*res.Docs[0].Score).To(BeNumerically("<=", 0.22471909420069797))

		res, err = client.FTSearchWithArgs(ctx, "idx1", "quick", &dicedb.FTSearchOptions{WithScores: true, Scorer: "DISMAX"}).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(*res.Docs[0].Score).To(BeEquivalentTo(float64(2)))

		res, err = client.FTSearchWithArgs(ctx, "idx1", "quick", &dicedb.FTSearchOptions{WithScores: true, Scorer: "DOCSCORE"}).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(*res.Docs[0].Score).To(BeEquivalentTo(float64(1)))

		res, err = client.FTSearchWithArgs(ctx, "idx1", "quick", &dicedb.FTSearchOptions{WithScores: true, Scorer: "HAMMING"}).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(*res.Docs[0].Score).To(BeEquivalentTo(float64(0)))
	})

	It("should FTConfigSet and FTConfigGet ", Label("search", "ftconfigget", "ftconfigset", "NonRedisEnterprise"), func() {
		val, err := client.FTConfigSet(ctx, "TIMEOUT", "100").Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(val).To(BeEquivalentTo("OK"))

		res, err := client.FTConfigGet(ctx, "*").Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(res["TIMEOUT"]).To(BeEquivalentTo("100"))

		res, err = client.FTConfigGet(ctx, "TIMEOUT").Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(res).To(BeEquivalentTo(map[string]interface{}{"TIMEOUT": "100"}))

	})

	It("should FTAggregate GroupBy ", Label("search", "ftaggregate"), func() {
		text1 := &dicedb.FieldSchema{FieldName: "title", FieldType: dicedb.SearchFieldTypeText}
		text2 := &dicedb.FieldSchema{FieldName: "body", FieldType: dicedb.SearchFieldTypeText}
		text3 := &dicedb.FieldSchema{FieldName: "parent", FieldType: dicedb.SearchFieldTypeText}
		num := &dicedb.FieldSchema{FieldName: "random_num", FieldType: dicedb.SearchFieldTypeNumeric}
		val, err := client.FTCreate(ctx, "idx1", &dicedb.FTCreateOptions{}, text1, text2, text3, num).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(val).To(BeEquivalentTo("OK"))
		WaitForIndexing(client, "idx1")

		client.HSet(ctx, "search", "title", "RediSearch",
			"body", "Redisearch impements a search engine on top of redis",
			"parent", "redis",
			"random_num", 10)
		client.HSet(ctx, "ai", "title", "RedisAI",
			"body", "RedisAI executes Deep Learning/Machine Learning models and managing their data.",
			"parent", "redis",
			"random_num", 3)
		client.HSet(ctx, "json", "title", "RedisJson",
			"body", "RedisJSON implements ECMA-404 The JSON Data Interchange Standard as a native data type.",
			"parent", "redis",
			"random_num", 8)

		reducer := dicedb.FTAggregateReducer{Reducer: dicedb.SearchCount}
		options := &dicedb.FTAggregateOptions{GroupBy: []dicedb.FTAggregateGroupBy{{Fields: []interface{}{"@parent"}, Reduce: []dicedb.FTAggregateReducer{reducer}}}}
		res, err := client.FTAggregateWithArgs(ctx, "idx1", "redis", options).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(res.Rows[0].Fields["parent"]).To(BeEquivalentTo("redis"))
		Expect(res.Rows[0].Fields["__generated_aliascount"]).To(BeEquivalentTo("3"))

		reducer = dicedb.FTAggregateReducer{Reducer: dicedb.SearchCountDistinct, Args: []interface{}{"@title"}}
		options = &dicedb.FTAggregateOptions{GroupBy: []dicedb.FTAggregateGroupBy{{Fields: []interface{}{"@parent"}, Reduce: []dicedb.FTAggregateReducer{reducer}}}}
		res, err = client.FTAggregateWithArgs(ctx, "idx1", "redis", options).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(res.Rows[0].Fields["parent"]).To(BeEquivalentTo("redis"))
		Expect(res.Rows[0].Fields["__generated_aliascount_distincttitle"]).To(BeEquivalentTo("3"))

		reducer = dicedb.FTAggregateReducer{Reducer: dicedb.SearchSum, Args: []interface{}{"@random_num"}}
		options = &dicedb.FTAggregateOptions{GroupBy: []dicedb.FTAggregateGroupBy{{Fields: []interface{}{"@parent"}, Reduce: []dicedb.FTAggregateReducer{reducer}}}}
		res, err = client.FTAggregateWithArgs(ctx, "idx1", "redis", options).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(res.Rows[0].Fields["parent"]).To(BeEquivalentTo("redis"))
		Expect(res.Rows[0].Fields["__generated_aliassumrandom_num"]).To(BeEquivalentTo("21"))

		reducer = dicedb.FTAggregateReducer{Reducer: dicedb.SearchMin, Args: []interface{}{"@random_num"}}
		options = &dicedb.FTAggregateOptions{GroupBy: []dicedb.FTAggregateGroupBy{{Fields: []interface{}{"@parent"}, Reduce: []dicedb.FTAggregateReducer{reducer}}}}
		res, err = client.FTAggregateWithArgs(ctx, "idx1", "redis", options).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(res.Rows[0].Fields["parent"]).To(BeEquivalentTo("redis"))
		Expect(res.Rows[0].Fields["__generated_aliasminrandom_num"]).To(BeEquivalentTo("3"))

		reducer = dicedb.FTAggregateReducer{Reducer: dicedb.SearchMax, Args: []interface{}{"@random_num"}}
		options = &dicedb.FTAggregateOptions{GroupBy: []dicedb.FTAggregateGroupBy{{Fields: []interface{}{"@parent"}, Reduce: []dicedb.FTAggregateReducer{reducer}}}}
		res, err = client.FTAggregateWithArgs(ctx, "idx1", "redis", options).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(res.Rows[0].Fields["parent"]).To(BeEquivalentTo("redis"))
		Expect(res.Rows[0].Fields["__generated_aliasmaxrandom_num"]).To(BeEquivalentTo("10"))

		reducer = dicedb.FTAggregateReducer{Reducer: dicedb.SearchAvg, Args: []interface{}{"@random_num"}}
		options = &dicedb.FTAggregateOptions{GroupBy: []dicedb.FTAggregateGroupBy{{Fields: []interface{}{"@parent"}, Reduce: []dicedb.FTAggregateReducer{reducer}}}}
		res, err = client.FTAggregateWithArgs(ctx, "idx1", "redis", options).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(res.Rows[0].Fields["parent"]).To(BeEquivalentTo("redis"))
		Expect(res.Rows[0].Fields["__generated_aliasavgrandom_num"]).To(BeEquivalentTo("7"))

		reducer = dicedb.FTAggregateReducer{Reducer: dicedb.SearchStdDev, Args: []interface{}{"@random_num"}}
		options = &dicedb.FTAggregateOptions{GroupBy: []dicedb.FTAggregateGroupBy{{Fields: []interface{}{"@parent"}, Reduce: []dicedb.FTAggregateReducer{reducer}}}}
		res, err = client.FTAggregateWithArgs(ctx, "idx1", "redis", options).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(res.Rows[0].Fields["parent"]).To(BeEquivalentTo("redis"))
		Expect(res.Rows[0].Fields["__generated_aliasstddevrandom_num"]).To(BeEquivalentTo("3.60555127546"))

		reducer = dicedb.FTAggregateReducer{Reducer: dicedb.SearchQuantile, Args: []interface{}{"@random_num", 0.5}}
		options = &dicedb.FTAggregateOptions{GroupBy: []dicedb.FTAggregateGroupBy{{Fields: []interface{}{"@parent"}, Reduce: []dicedb.FTAggregateReducer{reducer}}}}
		res, err = client.FTAggregateWithArgs(ctx, "idx1", "redis", options).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(res.Rows[0].Fields["parent"]).To(BeEquivalentTo("redis"))
		Expect(res.Rows[0].Fields["__generated_aliasquantilerandom_num,0.5"]).To(BeEquivalentTo("8"))

		reducer = dicedb.FTAggregateReducer{Reducer: dicedb.SearchToList, Args: []interface{}{"@title"}}
		options = &dicedb.FTAggregateOptions{GroupBy: []dicedb.FTAggregateGroupBy{{Fields: []interface{}{"@parent"}, Reduce: []dicedb.FTAggregateReducer{reducer}}}}
		res, err = client.FTAggregateWithArgs(ctx, "idx1", "redis", options).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(res.Rows[0].Fields["parent"]).To(BeEquivalentTo("redis"))
		Expect(res.Rows[0].Fields["__generated_aliastolisttitle"]).To(ContainElements("RediSearch", "RedisAI", "RedisJson"))

		reducer = dicedb.FTAggregateReducer{Reducer: dicedb.SearchFirstValue, Args: []interface{}{"@title"}, As: "first"}
		options = &dicedb.FTAggregateOptions{GroupBy: []dicedb.FTAggregateGroupBy{{Fields: []interface{}{"@parent"}, Reduce: []dicedb.FTAggregateReducer{reducer}}}}
		res, err = client.FTAggregateWithArgs(ctx, "idx1", "redis", options).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(res.Rows[0].Fields["parent"]).To(BeEquivalentTo("redis"))
		Expect(res.Rows[0].Fields["first"]).To(Or(BeEquivalentTo("RediSearch"), BeEquivalentTo("RedisAI"), BeEquivalentTo("RedisJson")))

		reducer = dicedb.FTAggregateReducer{Reducer: dicedb.SearchRandomSample, Args: []interface{}{"@title", 2}, As: "random"}
		options = &dicedb.FTAggregateOptions{GroupBy: []dicedb.FTAggregateGroupBy{{Fields: []interface{}{"@parent"}, Reduce: []dicedb.FTAggregateReducer{reducer}}}}
		res, err = client.FTAggregateWithArgs(ctx, "idx1", "redis", options).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(res.Rows[0].Fields["parent"]).To(BeEquivalentTo("redis"))
		Expect(res.Rows[0].Fields["random"]).To(Or(
			ContainElement("RediSearch"),
			ContainElement("RedisAI"),
			ContainElement("RedisJson"),
		))

	})

	It("should FTAggregate sort and limit", Label("search", "ftaggregate"), func() {
		text1 := &dicedb.FieldSchema{FieldName: "t1", FieldType: dicedb.SearchFieldTypeText}
		text2 := &dicedb.FieldSchema{FieldName: "t2", FieldType: dicedb.SearchFieldTypeText}
		val, err := client.FTCreate(ctx, "idx1", &dicedb.FTCreateOptions{}, text1, text2).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(val).To(BeEquivalentTo("OK"))
		WaitForIndexing(client, "idx1")

		client.HSet(ctx, "doc1", "t1", "a", "t2", "b")
		client.HSet(ctx, "doc2", "t1", "b", "t2", "a")

		options := &dicedb.FTAggregateOptions{SortBy: []dicedb.FTAggregateSortBy{{FieldName: "@t2", Asc: true}, {FieldName: "@t1", Desc: true}}}
		res, err := client.FTAggregateWithArgs(ctx, "idx1", "*", options).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(res.Rows[0].Fields["t1"]).To(BeEquivalentTo("b"))
		Expect(res.Rows[1].Fields["t1"]).To(BeEquivalentTo("a"))
		Expect(res.Rows[0].Fields["t2"]).To(BeEquivalentTo("a"))
		Expect(res.Rows[1].Fields["t2"]).To(BeEquivalentTo("b"))

		options = &dicedb.FTAggregateOptions{SortBy: []dicedb.FTAggregateSortBy{{FieldName: "@t1"}}}
		res, err = client.FTAggregateWithArgs(ctx, "idx1", "*", options).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(res.Rows[0].Fields["t1"]).To(BeEquivalentTo("a"))
		Expect(res.Rows[1].Fields["t1"]).To(BeEquivalentTo("b"))

		options = &dicedb.FTAggregateOptions{SortBy: []dicedb.FTAggregateSortBy{{FieldName: "@t1"}}, SortByMax: 1}
		res, err = client.FTAggregateWithArgs(ctx, "idx1", "*", options).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(res.Rows[0].Fields["t1"]).To(BeEquivalentTo("a"))

		options = &dicedb.FTAggregateOptions{SortBy: []dicedb.FTAggregateSortBy{{FieldName: "@t1"}}, Limit: 1, LimitOffset: 1}
		res, err = client.FTAggregateWithArgs(ctx, "idx1", "*", options).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(res.Rows[0].Fields["t1"]).To(BeEquivalentTo("b"))
	})

	It("should FTAggregate load ", Label("search", "ftaggregate"), func() {
		text1 := &dicedb.FieldSchema{FieldName: "t1", FieldType: dicedb.SearchFieldTypeText}
		text2 := &dicedb.FieldSchema{FieldName: "t2", FieldType: dicedb.SearchFieldTypeText}
		val, err := client.FTCreate(ctx, "idx1", &dicedb.FTCreateOptions{}, text1, text2).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(val).To(BeEquivalentTo("OK"))
		WaitForIndexing(client, "idx1")

		client.HSet(ctx, "doc1", "t1", "hello", "t2", "world")

		options := &dicedb.FTAggregateOptions{Load: []dicedb.FTAggregateLoad{{Field: "t1"}}}
		res, err := client.FTAggregateWithArgs(ctx, "idx1", "*", options).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(res.Rows[0].Fields["t1"]).To(BeEquivalentTo("hello"))

		options = &dicedb.FTAggregateOptions{Load: []dicedb.FTAggregateLoad{{Field: "t2"}}}
		res, err = client.FTAggregateWithArgs(ctx, "idx1", "*", options).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(res.Rows[0].Fields["t2"]).To(BeEquivalentTo("world"))

		options = &dicedb.FTAggregateOptions{LoadAll: true}
		res, err = client.FTAggregateWithArgs(ctx, "idx1", "*", options).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(res.Rows[0].Fields["t1"]).To(BeEquivalentTo("hello"))
		Expect(res.Rows[0].Fields["t2"]).To(BeEquivalentTo("world"))
	})

	It("should FTAggregate apply", Label("search", "ftaggregate"), func() {
		text1 := &dicedb.FieldSchema{FieldName: "PrimaryKey", FieldType: dicedb.SearchFieldTypeText, Sortable: true}
		num1 := &dicedb.FieldSchema{FieldName: "CreatedDateTimeUTC", FieldType: dicedb.SearchFieldTypeNumeric, Sortable: true}
		val, err := client.FTCreate(ctx, "idx1", &dicedb.FTCreateOptions{}, text1, num1).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(val).To(BeEquivalentTo("OK"))
		WaitForIndexing(client, "idx1")

		client.HSet(ctx, "doc1", "PrimaryKey", "9::362330", "CreatedDateTimeUTC", "637387878524969984")
		client.HSet(ctx, "doc2", "PrimaryKey", "9::362329", "CreatedDateTimeUTC", "637387875859270016")

		options := &dicedb.FTAggregateOptions{Apply: []dicedb.FTAggregateApply{{Field: "@CreatedDateTimeUTC * 10", As: "CreatedDateTimeUTC"}}}
		res, err := client.FTAggregateWithArgs(ctx, "idx1", "*", options).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(res.Rows[0].Fields["CreatedDateTimeUTC"]).To(Or(BeEquivalentTo("6373878785249699840"), BeEquivalentTo("6373878758592700416")))
		Expect(res.Rows[1].Fields["CreatedDateTimeUTC"]).To(Or(BeEquivalentTo("6373878785249699840"), BeEquivalentTo("6373878758592700416")))

	})

	It("should FTAggregate filter", Label("search", "ftaggregate"), func() {
		text1 := &dicedb.FieldSchema{FieldName: "name", FieldType: dicedb.SearchFieldTypeText, Sortable: true}
		num1 := &dicedb.FieldSchema{FieldName: "age", FieldType: dicedb.SearchFieldTypeNumeric, Sortable: true}
		val, err := client.FTCreate(ctx, "idx1", &dicedb.FTCreateOptions{}, text1, num1).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(val).To(BeEquivalentTo("OK"))
		WaitForIndexing(client, "idx1")

		client.HSet(ctx, "doc1", "name", "bar", "age", "25")
		client.HSet(ctx, "doc2", "name", "foo", "age", "19")

		for _, dlc := range []int{1, 2} {
			options := &dicedb.FTAggregateOptions{Filter: "@name=='foo' && @age < 20", DialectVersion: dlc}
			res, err := client.FTAggregateWithArgs(ctx, "idx1", "*", options).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(res.Total).To(Or(BeEquivalentTo(2), BeEquivalentTo(1)))
			Expect(res.Rows[0].Fields["name"]).To(BeEquivalentTo("foo"))

			options = &dicedb.FTAggregateOptions{Filter: "@age > 15", DialectVersion: dlc, SortBy: []dicedb.FTAggregateSortBy{{FieldName: "@age"}}}
			res, err = client.FTAggregateWithArgs(ctx, "idx1", "*", options).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(res.Total).To(BeEquivalentTo(2))
			Expect(res.Rows[0].Fields["age"]).To(BeEquivalentTo("19"))
			Expect(res.Rows[1].Fields["age"]).To(BeEquivalentTo("25"))
		}

	})

	It("should FTSearch SkipInitalScan", Label("search", "ftsearch"), func() {
		client.HSet(ctx, "doc1", "foo", "bar")

		text1 := &dicedb.FieldSchema{FieldName: "foo", FieldType: dicedb.SearchFieldTypeText}
		val, err := client.FTCreate(ctx, "idx1", &dicedb.FTCreateOptions{SkipInitalScan: true}, text1).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(val).To(BeEquivalentTo("OK"))
		WaitForIndexing(client, "idx1")

		res, err := client.FTSearch(ctx, "idx1", "@foo:bar").Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(res.Total).To(BeEquivalentTo(int64(0)))
	})

	It("should FTCreate json", Label("search", "ftcreate"), func() {

		text1 := &dicedb.FieldSchema{FieldName: "$.name", FieldType: dicedb.SearchFieldTypeText}
		val, err := client.FTCreate(ctx, "idx1", &dicedb.FTCreateOptions{OnJSON: true, Prefix: []interface{}{"king:"}}, text1).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(val).To(BeEquivalentTo("OK"))
		WaitForIndexing(client, "idx1")

		client.JSONSet(ctx, "king:1", "$", `{"name": "henry"}`)
		client.JSONSet(ctx, "king:2", "$", `{"name": "james"}`)

		res, err := client.FTSearch(ctx, "idx1", "henry").Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(res.Total).To(BeEquivalentTo(1))
		Expect(res.Docs[0].ID).To(BeEquivalentTo("king:1"))
		Expect(res.Docs[0].Fields["$"]).To(BeEquivalentTo(`{"name":"henry"}`))
	})

	It("should FTCreate json fields as names", Label("search", "ftcreate"), func() {

		text1 := &dicedb.FieldSchema{FieldName: "$.name", FieldType: dicedb.SearchFieldTypeText, As: "name"}
		num1 := &dicedb.FieldSchema{FieldName: "$.age", FieldType: dicedb.SearchFieldTypeNumeric, As: "just_a_number"}
		val, err := client.FTCreate(ctx, "idx1", &dicedb.FTCreateOptions{OnJSON: true}, text1, num1).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(val).To(BeEquivalentTo("OK"))
		WaitForIndexing(client, "idx1")

		client.JSONSet(ctx, "doc:1", "$", `{"name": "Jon", "age": 25}`)

		res, err := client.FTSearchWithArgs(ctx, "idx1", "Jon", &dicedb.FTSearchOptions{Return: []dicedb.FTSearchReturn{{FieldName: "name"}, {FieldName: "just_a_number"}}}).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(res.Total).To(BeEquivalentTo(1))
		Expect(res.Docs[0].ID).To(BeEquivalentTo("doc:1"))
		Expect(res.Docs[0].Fields["name"]).To(BeEquivalentTo("Jon"))
		Expect(res.Docs[0].Fields["just_a_number"]).To(BeEquivalentTo("25"))
	})

	It("should FTCreate CaseSensitive", Label("search", "ftcreate"), func() {

		tag1 := &dicedb.FieldSchema{FieldName: "t", FieldType: dicedb.SearchFieldTypeTag, CaseSensitive: false}
		val, err := client.FTCreate(ctx, "idx1", &dicedb.FTCreateOptions{}, tag1).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(val).To(BeEquivalentTo("OK"))
		WaitForIndexing(client, "idx1")

		client.HSet(ctx, "1", "t", "HELLO")
		client.HSet(ctx, "2", "t", "hello")

		res, err := client.FTSearch(ctx, "idx1", "@t:{HELLO}").Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(res.Total).To(BeEquivalentTo(2))
		Expect(res.Docs[0].ID).To(BeEquivalentTo("1"))
		Expect(res.Docs[1].ID).To(BeEquivalentTo("2"))

		resDrop, err := client.FTDropIndex(ctx, "idx1").Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(resDrop).To(BeEquivalentTo("OK"))

		tag2 := &dicedb.FieldSchema{FieldName: "t", FieldType: dicedb.SearchFieldTypeTag, CaseSensitive: true}
		val, err = client.FTCreate(ctx, "idx1", &dicedb.FTCreateOptions{}, tag2).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(val).To(BeEquivalentTo("OK"))
		WaitForIndexing(client, "idx1")

		res, err = client.FTSearch(ctx, "idx1", "@t:{HELLO}").Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(res.Total).To(BeEquivalentTo(1))
		Expect(res.Docs[0].ID).To(BeEquivalentTo("1"))

	})

	It("should FTSearch ReturnFields", Label("search", "ftsearch"), func() {
		resJson, err := client.JSONSet(ctx, "doc:1", "$", `{"t": "riceratops","t2": "telmatosaurus", "n": 9072, "flt": 97.2}`).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(resJson).To(BeEquivalentTo("OK"))

		text1 := &dicedb.FieldSchema{FieldName: "$.t", FieldType: dicedb.SearchFieldTypeText}
		num1 := &dicedb.FieldSchema{FieldName: "$.flt", FieldType: dicedb.SearchFieldTypeNumeric}
		val, err := client.FTCreate(ctx, "idx1", &dicedb.FTCreateOptions{OnJSON: true}, text1, num1).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(val).To(BeEquivalentTo("OK"))
		WaitForIndexing(client, "idx1")

		res, err := client.FTSearchWithArgs(ctx, "idx1", "*", &dicedb.FTSearchOptions{Return: []dicedb.FTSearchReturn{{FieldName: "$.t", As: "txt"}}}).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(res.Total).To(BeEquivalentTo(1))
		Expect(res.Docs[0].ID).To(BeEquivalentTo("doc:1"))
		Expect(res.Docs[0].Fields["txt"]).To(BeEquivalentTo("riceratops"))

		res, err = client.FTSearchWithArgs(ctx, "idx1", "*", &dicedb.FTSearchOptions{Return: []dicedb.FTSearchReturn{{FieldName: "$.t2", As: "txt"}}}).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(res.Total).To(BeEquivalentTo(1))
		Expect(res.Docs[0].ID).To(BeEquivalentTo("doc:1"))
		Expect(res.Docs[0].Fields["txt"]).To(BeEquivalentTo("telmatosaurus"))
	})

	It("should FTSynUpdate", Label("search", "ftsynupdate"), func() {

		text1 := &dicedb.FieldSchema{FieldName: "title", FieldType: dicedb.SearchFieldTypeText}
		text2 := &dicedb.FieldSchema{FieldName: "body", FieldType: dicedb.SearchFieldTypeText}
		val, err := client.FTCreate(ctx, "idx1", &dicedb.FTCreateOptions{OnHash: true}, text1, text2).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(val).To(BeEquivalentTo("OK"))
		WaitForIndexing(client, "idx1")

		resSynUpdate, err := client.FTSynUpdateWithArgs(ctx, "idx1", "id1", &dicedb.FTSynUpdateOptions{SkipInitialScan: true}, []interface{}{"boy", "child", "offspring"}).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(resSynUpdate).To(BeEquivalentTo("OK"))
		client.HSet(ctx, "doc1", "title", "he is a baby", "body", "this is a test")

		resSynUpdate, err = client.FTSynUpdateWithArgs(ctx, "idx1", "id1", &dicedb.FTSynUpdateOptions{SkipInitialScan: true}, []interface{}{"baby"}).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(resSynUpdate).To(BeEquivalentTo("OK"))
		client.HSet(ctx, "doc2", "title", "he is another baby", "body", "another test")

		res, err := client.FTSearchWithArgs(ctx, "idx1", "child", &dicedb.FTSearchOptions{Expander: "SYNONYM"}).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(res.Docs[0].ID).To(BeEquivalentTo("doc2"))
		Expect(res.Docs[0].Fields["title"]).To(BeEquivalentTo("he is another baby"))
		Expect(res.Docs[0].Fields["body"]).To(BeEquivalentTo("another test"))
	})

	It("should FTSynDump", Label("search", "ftsyndump"), func() {

		text1 := &dicedb.FieldSchema{FieldName: "title", FieldType: dicedb.SearchFieldTypeText}
		text2 := &dicedb.FieldSchema{FieldName: "body", FieldType: dicedb.SearchFieldTypeText}
		val, err := client.FTCreate(ctx, "idx1", &dicedb.FTCreateOptions{OnHash: true}, text1, text2).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(val).To(BeEquivalentTo("OK"))
		WaitForIndexing(client, "idx1")

		resSynUpdate, err := client.FTSynUpdate(ctx, "idx1", "id1", []interface{}{"boy", "child", "offspring"}).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(resSynUpdate).To(BeEquivalentTo("OK"))

		resSynUpdate, err = client.FTSynUpdate(ctx, "idx1", "id1", []interface{}{"baby", "child"}).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(resSynUpdate).To(BeEquivalentTo("OK"))

		resSynUpdate, err = client.FTSynUpdate(ctx, "idx1", "id1", []interface{}{"tree", "wood"}).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(resSynUpdate).To(BeEquivalentTo("OK"))

		resSynDump, err := client.FTSynDump(ctx, "idx1").Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(resSynDump[0].Term).To(BeEquivalentTo("baby"))
		Expect(resSynDump[0].Synonyms).To(BeEquivalentTo([]string{"id1"}))
		Expect(resSynDump[1].Term).To(BeEquivalentTo("wood"))
		Expect(resSynDump[1].Synonyms).To(BeEquivalentTo([]string{"id1"}))
		Expect(resSynDump[2].Term).To(BeEquivalentTo("boy"))
		Expect(resSynDump[2].Synonyms).To(BeEquivalentTo([]string{"id1"}))
		Expect(resSynDump[3].Term).To(BeEquivalentTo("tree"))
		Expect(resSynDump[3].Synonyms).To(BeEquivalentTo([]string{"id1"}))
		Expect(resSynDump[4].Term).To(BeEquivalentTo("child"))
		Expect(resSynDump[4].Synonyms).To(Or(BeEquivalentTo([]string{"id1"}), BeEquivalentTo([]string{"id1", "id1"})))
		Expect(resSynDump[5].Term).To(BeEquivalentTo("offspring"))
		Expect(resSynDump[5].Synonyms).To(BeEquivalentTo([]string{"id1"}))

	})

	It("should FTCreate json with alias", Label("search", "ftcreate"), func() {

		text1 := &dicedb.FieldSchema{FieldName: "$.name", FieldType: dicedb.SearchFieldTypeText, As: "name"}
		num1 := &dicedb.FieldSchema{FieldName: "$.num", FieldType: dicedb.SearchFieldTypeNumeric, As: "num"}
		val, err := client.FTCreate(ctx, "idx1", &dicedb.FTCreateOptions{OnJSON: true, Prefix: []interface{}{"king:"}}, text1, num1).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(val).To(BeEquivalentTo("OK"))
		WaitForIndexing(client, "idx1")

		client.JSONSet(ctx, "king:1", "$", `{"name": "henry", "num": 42}`)
		client.JSONSet(ctx, "king:2", "$", `{"name": "james", "num": 3.14}`)

		res, err := client.FTSearch(ctx, "idx1", "@name:henry").Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(res.Total).To(BeEquivalentTo(1))
		Expect(res.Docs[0].ID).To(BeEquivalentTo("king:1"))
		Expect(res.Docs[0].Fields["$"]).To(BeEquivalentTo(`{"name":"henry","num":42}`))

		res, err = client.FTSearch(ctx, "idx1", "@num:[0 10]").Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(res.Total).To(BeEquivalentTo(1))
		Expect(res.Docs[0].ID).To(BeEquivalentTo("king:2"))
		Expect(res.Docs[0].Fields["$"]).To(BeEquivalentTo(`{"name":"james","num":3.14}`))
	})

	It("should FTCreate json with multipath", Label("search", "ftcreate"), func() {

		tag1 := &dicedb.FieldSchema{FieldName: "$..name", FieldType: dicedb.SearchFieldTypeTag, As: "name"}
		val, err := client.FTCreate(ctx, "idx1", &dicedb.FTCreateOptions{OnJSON: true, Prefix: []interface{}{"king:"}}, tag1).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(val).To(BeEquivalentTo("OK"))
		WaitForIndexing(client, "idx1")

		client.JSONSet(ctx, "king:1", "$", `{"name": "henry", "country": {"name": "england"}}`)

		res, err := client.FTSearch(ctx, "idx1", "@name:{england}").Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(res.Total).To(BeEquivalentTo(1))
		Expect(res.Docs[0].ID).To(BeEquivalentTo("king:1"))
		Expect(res.Docs[0].Fields["$"]).To(BeEquivalentTo(`{"name":"henry","country":{"name":"england"}}`))
	})

	It("should FTCreate json with jsonpath", Label("search", "ftcreate"), func() {

		text1 := &dicedb.FieldSchema{FieldName: `$["prod:name"]`, FieldType: dicedb.SearchFieldTypeText, As: "name"}
		text2 := &dicedb.FieldSchema{FieldName: `$.prod:name`, FieldType: dicedb.SearchFieldTypeText, As: "name_unsupported"}
		val, err := client.FTCreate(ctx, "idx1", &dicedb.FTCreateOptions{OnJSON: true}, text1, text2).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(val).To(BeEquivalentTo("OK"))
		WaitForIndexing(client, "idx1")

		client.JSONSet(ctx, "doc:1", "$", `{"prod:name": "RediSearch"}`)

		res, err := client.FTSearch(ctx, "idx1", "@name:RediSearch").Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(res.Total).To(BeEquivalentTo(1))
		Expect(res.Docs[0].ID).To(BeEquivalentTo("doc:1"))
		Expect(res.Docs[0].Fields["$"]).To(BeEquivalentTo(`{"prod:name":"RediSearch"}`))

		res, err = client.FTSearch(ctx, "idx1", "@name_unsupported:RediSearch").Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(res.Total).To(BeEquivalentTo(1))

		res, err = client.FTSearchWithArgs(ctx, "idx1", "@name:RediSearch", &dicedb.FTSearchOptions{Return: []dicedb.FTSearchReturn{{FieldName: "name"}}}).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(res.Total).To(BeEquivalentTo(1))
		Expect(res.Docs[0].ID).To(BeEquivalentTo("doc:1"))
		Expect(res.Docs[0].Fields["name"]).To(BeEquivalentTo("RediSearch"))

	})

	It("should FTCreate VECTOR", Label("search", "ftcreate"), func() {
		hnswOptions := &dicedb.FTHNSWOptions{Type: "FLOAT32", Dim: 2, DistanceMetric: "L2"}
		val, err := client.FTCreate(ctx, "idx1",
			&dicedb.FTCreateOptions{},
			&dicedb.FieldSchema{FieldName: "v", FieldType: dicedb.SearchFieldTypeVector, VectorArgs: &dicedb.FTVectorArgs{HNSWOptions: hnswOptions}}).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(val).To(BeEquivalentTo("OK"))
		WaitForIndexing(client, "idx1")

		client.HSet(ctx, "a", "v", "aaaaaaaa")
		client.HSet(ctx, "b", "v", "aaaabaaa")
		client.HSet(ctx, "c", "v", "aaaaabaa")

		searchOptions := &dicedb.FTSearchOptions{
			Return:         []dicedb.FTSearchReturn{{FieldName: "__v_score"}},
			SortBy:         []dicedb.FTSearchSortBy{{FieldName: "__v_score", Asc: true}},
			DialectVersion: 2,
			Params:         map[string]interface{}{"vec": "aaaaaaaa"},
		}
		res, err := client.FTSearchWithArgs(ctx, "idx1", "*=>[KNN 2 @v $vec]", searchOptions).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(res.Docs[0].ID).To(BeEquivalentTo("a"))
		Expect(res.Docs[0].Fields["__v_score"]).To(BeEquivalentTo("0"))
	})

	It("should FTCreate and FTSearch text params", Label("search", "ftcreate", "ftsearch"), func() {
		val, err := client.FTCreate(ctx, "idx1", &dicedb.FTCreateOptions{}, &dicedb.FieldSchema{FieldName: "name", FieldType: dicedb.SearchFieldTypeText}).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(val).To(BeEquivalentTo("OK"))
		WaitForIndexing(client, "idx1")

		client.HSet(ctx, "doc1", "name", "Alice")
		client.HSet(ctx, "doc2", "name", "Bob")
		client.HSet(ctx, "doc3", "name", "Carol")

		res1, err := client.FTSearchWithArgs(ctx, "idx1", "@name:($name1 | $name2 )", &dicedb.FTSearchOptions{Params: map[string]interface{}{"name1": "Alice", "name2": "Bob"}, DialectVersion: 2}).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(res1.Total).To(BeEquivalentTo(int64(2)))
		Expect(res1.Docs[0].ID).To(BeEquivalentTo("doc1"))
		Expect(res1.Docs[1].ID).To(BeEquivalentTo("doc2"))

	})

	It("should FTCreate and FTSearch numeric params", Label("search", "ftcreate", "ftsearch"), func() {
		val, err := client.FTCreate(ctx, "idx1", &dicedb.FTCreateOptions{}, &dicedb.FieldSchema{FieldName: "numval", FieldType: dicedb.SearchFieldTypeNumeric}).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(val).To(BeEquivalentTo("OK"))
		WaitForIndexing(client, "idx1")

		client.HSet(ctx, "doc1", "numval", 101)
		client.HSet(ctx, "doc2", "numval", 102)
		client.HSet(ctx, "doc3", "numval", 103)

		res1, err := client.FTSearchWithArgs(ctx, "idx1", "@numval:[$min $max]", &dicedb.FTSearchOptions{Params: map[string]interface{}{"min": 101, "max": 102}, DialectVersion: 2}).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(res1.Total).To(BeEquivalentTo(int64(2)))
		Expect(res1.Docs[0].ID).To(BeEquivalentTo("doc1"))
		Expect(res1.Docs[1].ID).To(BeEquivalentTo("doc2"))

	})

	It("should FTCreate and FTSearch geo params", Label("search", "ftcreate", "ftsearch"), func() {
		val, err := client.FTCreate(ctx, "idx1", &dicedb.FTCreateOptions{}, &dicedb.FieldSchema{FieldName: "g", FieldType: dicedb.SearchFieldTypeGeo}).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(val).To(BeEquivalentTo("OK"))
		WaitForIndexing(client, "idx1")

		client.HSet(ctx, "doc1", "g", "29.69465, 34.95126")
		client.HSet(ctx, "doc2", "g", "29.69350, 34.94737")
		client.HSet(ctx, "doc3", "g", "29.68746, 34.94882")

		res1, err := client.FTSearchWithArgs(ctx, "idx1", "@g:[$lon $lat $radius $units]", &dicedb.FTSearchOptions{Params: map[string]interface{}{"lat": "34.95126", "lon": "29.69465", "radius": 1000, "units": "km"}, DialectVersion: 2}).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(res1.Total).To(BeEquivalentTo(int64(3)))
		Expect(res1.Docs[0].ID).To(BeEquivalentTo("doc1"))
		Expect(res1.Docs[1].ID).To(BeEquivalentTo("doc2"))
		Expect(res1.Docs[2].ID).To(BeEquivalentTo("doc3"))

	})

	It("should FTConfigSet and FTConfigGet dialect", Label("search", "ftconfigget", "ftconfigset", "NonRedisEnterprise"), func() {
		res, err := client.FTConfigSet(ctx, "DEFAULT_DIALECT", "1").Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(res).To(BeEquivalentTo("OK"))

		defDialect, err := client.FTConfigGet(ctx, "DEFAULT_DIALECT").Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(defDialect).To(BeEquivalentTo(map[string]interface{}{"DEFAULT_DIALECT": "1"}))

		res, err = client.FTConfigSet(ctx, "DEFAULT_DIALECT", "2").Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(res).To(BeEquivalentTo("OK"))

		defDialect, err = client.FTConfigGet(ctx, "DEFAULT_DIALECT").Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(defDialect).To(BeEquivalentTo(map[string]interface{}{"DEFAULT_DIALECT": "2"}))
	})

	It("should FTCreate WithSuffixtrie", Label("search", "ftcreate", "ftinfo"), func() {
		val, err := client.FTCreate(ctx, "idx1", &dicedb.FTCreateOptions{}, &dicedb.FieldSchema{FieldName: "txt", FieldType: dicedb.SearchFieldTypeText}).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(val).To(BeEquivalentTo("OK"))
		WaitForIndexing(client, "idx1")

		res, err := client.FTInfo(ctx, "idx1").Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(res.Attributes[0].Attribute).To(BeEquivalentTo("txt"))

		resDrop, err := client.FTDropIndex(ctx, "idx1").Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(resDrop).To(BeEquivalentTo("OK"))

		// create withsuffixtrie index - text field
		val, err = client.FTCreate(ctx, "idx1", &dicedb.FTCreateOptions{}, &dicedb.FieldSchema{FieldName: "txt", FieldType: dicedb.SearchFieldTypeText, WithSuffixtrie: true}).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(val).To(BeEquivalentTo("OK"))
		WaitForIndexing(client, "idx1")

		res, err = client.FTInfo(ctx, "idx1").Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(res.Attributes[0].WithSuffixtrie).To(BeTrue())

		resDrop, err = client.FTDropIndex(ctx, "idx1").Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(resDrop).To(BeEquivalentTo("OK"))

		// create withsuffixtrie index - tag field
		val, err = client.FTCreate(ctx, "idx1", &dicedb.FTCreateOptions{}, &dicedb.FieldSchema{FieldName: "t", FieldType: dicedb.SearchFieldTypeTag, WithSuffixtrie: true}).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(val).To(BeEquivalentTo("OK"))
		WaitForIndexing(client, "idx1")

		res, err = client.FTInfo(ctx, "idx1").Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(res.Attributes[0].WithSuffixtrie).To(BeTrue())
	})

	It("should FTCreate GeoShape", Label("search", "ftcreate", "ftsearch"), func() {
		val, err := client.FTCreate(ctx, "idx1", &dicedb.FTCreateOptions{}, &dicedb.FieldSchema{FieldName: "geom", FieldType: dicedb.SearchFieldTypeGeoShape, GeoShapeFieldType: "FLAT"}).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(val).To(BeEquivalentTo("OK"))
		WaitForIndexing(client, "idx1")

		client.HSet(ctx, "small", "geom", "POLYGON((1 1, 1 100, 100 100, 100 1, 1 1))")
		client.HSet(ctx, "large", "geom", "POLYGON((1 1, 1 200, 200 200, 200 1, 1 1))")

		res1, err := client.FTSearchWithArgs(ctx, "idx1", "@geom:[WITHIN $poly]",
			&dicedb.FTSearchOptions{
				DialectVersion: 3,
				Params:         map[string]interface{}{"poly": "POLYGON((0 0, 0 150, 150 150, 150 0, 0 0))"},
			}).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(res1.Total).To(BeEquivalentTo(int64(1)))
		Expect(res1.Docs[0].ID).To(BeEquivalentTo("small"))

		res2, err := client.FTSearchWithArgs(ctx, "idx1", "@geom:[CONTAINS $poly]",
			&dicedb.FTSearchOptions{
				DialectVersion: 3,
				Params:         map[string]interface{}{"poly": "POLYGON((2 2, 2 50, 50 50, 50 2, 2 2))"},
			}).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(res2.Total).To(BeEquivalentTo(int64(2)))
	})
})

// It("should FTProfile Search and Aggregate", Label("search", "ftprofile"), func() {
// 	val, err := client.FTCreate(ctx, "idx1", &redis.FTCreateOptions{}, &redis.FieldSchema{FieldName: "t", FieldType: redis.SearchFieldTypeText}).Result()
// 	Expect(err).NotTo(HaveOccurred())
// 	Expect(val).To(BeEquivalentTo("OK"))
// 	WaitForIndexing(client, "idx1")

// 	client.HSet(ctx, "1", "t", "hello")
// 	client.HSet(ctx, "2", "t", "world")

// 	// FTProfile Search
// 	query := redis.FTSearchQuery("hello|world", &redis.FTSearchOptions{NoContent: true})
// 	res1, err := client.FTProfile(ctx, "idx1", false, query).Result()
// 	Expect(err).NotTo(HaveOccurred())
// 	panic(res1)
// Expect(len(res1["results"].([]interface{}))).To(BeEquivalentTo(3))
// resProfile := res1["profile"].(map[interface{}]interface{})
// Expect(resProfile["Parsing time"].(float64) < 0.5).To(BeTrue())
// iterProfile0 := resProfile["Iterators profile"].([]interface{})[0].(map[interface{}]interface{})
// Expect(iterProfile0["Counter"]).To(BeEquivalentTo(2.0))
// Expect(iterProfile0["Type"]).To(BeEquivalentTo("UNION"))

// // FTProfile Aggregate
// aggQuery := redis.FTAggregateQuery("*", &redis.FTAggregateOptions{
// 	Load:  []redis.FTAggregateLoad{{Field: "t"}},
// 	Apply: []redis.FTAggregateApply{{Field: "startswith(@t, 'hel')", As: "prefix"}}})
// res2, err := client.FTProfile(ctx, "idx1", false, aggQuery).Result()
// Expect(err).NotTo(HaveOccurred())
// Expect(len(res2["results"].([]interface{}))).To(BeEquivalentTo(2))
// resProfile = res2["profile"].(map[interface{}]interface{})
// iterProfile0 = resProfile["Iterators profile"].([]interface{})[0].(map[interface{}]interface{})
// Expect(iterProfile0["Counter"]).To(BeEquivalentTo(2))
// Expect(iterProfile0["Type"]).To(BeEquivalentTo("WILDCARD"))
// })

// 	It("should FTProfile Search Limited", Label("search", "ftprofile"), func() {
// 		val, err := client.FTCreate(ctx, "idx1", &redis.FTCreateOptions{}, &redis.FieldSchema{FieldName: "t", FieldType: redis.SearchFieldTypeText}).Result()
// 		Expect(err).NotTo(HaveOccurred())
// 		Expect(val).To(BeEquivalentTo("OK"))
// 		WaitForIndexing(client, "idx1")

// 		client.HSet(ctx, "1", "t", "hello")
// 		client.HSet(ctx, "2", "t", "hell")
// 		client.HSet(ctx, "3", "t", "help")
// 		client.HSet(ctx, "4", "t", "helowa")

// 		// FTProfile Search
// 		query := redis.FTSearchQuery("%hell% hel*", &redis.FTSearchOptions{})
// 		res1, err := client.FTProfile(ctx, "idx1", true, query).Result()
// 		Expect(err).NotTo(HaveOccurred())
// 		resProfile := res1["profile"].(map[interface{}]interface{})
// 		iterProfile0 := resProfile["Iterators profile"].([]interface{})[0].(map[interface{}]interface{})
// 		Expect(iterProfile0["Type"]).To(BeEquivalentTo("INTERSECT"))
// 		Expect(len(res1["results"].([]interface{}))).To(BeEquivalentTo(3))
// 		Expect(iterProfile0["Child iterators"].([]interface{})[0].(map[interface{}]interface{})["Child iterators"]).To(BeEquivalentTo("The number of iterators in the union is 3"))
// 		Expect(iterProfile0["Child iterators"].([]interface{})[1].(map[interface{}]interface{})["Child iterators"]).To(BeEquivalentTo("The number of iterators in the union is 4"))
// 	})

// 	It("should FTProfile Search query params", Label("search", "ftprofile"), func() {
// 		hnswOptions := &redis.FTHNSWOptions{Type: "FLOAT32", Dim: 2, DistanceMetric: "L2"}
// 		val, err := client.FTCreate(ctx, "idx1",
// 			&redis.FTCreateOptions{},
// 			&redis.FieldSchema{FieldName: "v", FieldType: redis.SearchFieldTypeVector, VectorArgs: &redis.FTVectorArgs{HNSWOptions: hnswOptions}}).Result()
// 		Expect(err).NotTo(HaveOccurred())
// 		Expect(val).To(BeEquivalentTo("OK"))
// 		WaitForIndexing(client, "idx1")

// 		client.HSet(ctx, "a", "v", "aaaaaaaa")
// 		client.HSet(ctx, "b", "v", "aaaabaaa")
// 		client.HSet(ctx, "c", "v", "aaaaabaa")

// 		// FTProfile Search
// 		searchOptions := &redis.FTSearchOptions{
// 			Return:         []redis.FTSearchReturn{{FieldName: "__v_score"}},
// 			SortBy:         []redis.FTSearchSortBy{{FieldName: "__v_score", Asc: true}},
// 			DialectVersion: 2,
// 			Params:         map[string]interface{}{"vec": "aaaaaaaa"},
// 		}
// 		query := redis.FTSearchQuery("*=>[KNN 2 @v $vec]", searchOptions)
// 		res1, err := client.FTProfile(ctx, "idx1", false, query).Result()
// 		Expect(err).NotTo(HaveOccurred())
// 		resProfile := res1["profile"].(map[interface{}]interface{})
// 		iterProfile0 := resProfile["Iterators profile"].([]interface{})[0].(map[interface{}]interface{})
// 		Expect(iterProfile0["Counter"]).To(BeEquivalentTo(2))
// 		Expect(iterProfile0["Type"]).To(BeEquivalentTo(redis.SearchFieldTypeVector.String()))
// 		Expect(res1["total_results"]).To(BeEquivalentTo(2))
// 		results0 := res1["results"].([]interface{})[0].(map[interface{}]interface{})
// 		Expect(results0["id"]).To(BeEquivalentTo("a"))
// 		Expect(results0["extra_attributes"].(map[interface{}]interface{})["__v_score"]).To(BeEquivalentTo("0"))
// 	})
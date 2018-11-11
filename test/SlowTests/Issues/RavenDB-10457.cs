using System;
using System.Collections.Generic;
using System.Linq;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_10457 : RavenTestBase
    {
        [Fact]
        public void CanDoMapOnObject()
        {
            using (var store = GetDocumentStore())
            {
                new TestDocumentByName().Execute(store);

                var testDoc = new TestDocument
                {
                    Name = "item1",
                    PriceConfig = new Dictionary<string, (int Price, int Quantity)>
                    {
                        {"Milk", (Price: 8, Quantity: 100)},
                        {"Coffee", (Price: 27, Quantity: 30)}
                    }
                };

                using (var session = store.OpenSession())
                {
                    session.Store(testDoc);
                    session.SaveChanges();
                }

                WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var query = from item in session.Query<TestDocument, TestDocumentByName>()
                                let prices = item.PriceConfig.Select(s => new { s.Value.Price, s.Value.Quantity })
                                select new
                                {
                                    item.Name,
                                    Prices = prices.ToList()
                                };

                    RavenTestHelper.AssertEqualRespectingNewLines(
@"declare function output(item) {
	var prices = Object.map(item.PriceConfig, function(v, k){ return {Price:v.Item1,Quantity:v.Item2};});
	return { Name : item.Name, Prices : prices };
}
from index 'TestDocumentByName' as item select output(item)", query.ToString());

                    var queryResult = query.ToList();

                    var expected = testDoc.PriceConfig.Select(s => new
                    {
                        s.Value.Price,
                        s.Value.Quantity
                    }).ToList();

                    Assert.Equal(expected, queryResult[0].Prices);

                }
            }
        }

        [Fact]
        public void CanDoMapOnObject2()
        {
            using (var store = GetDocumentStore())
            {
                var testDoc = Setup(store);

                using (var session = store.OpenSession())
                {
                    var query = from item in session.Query<TestDocument, TestDocumentByName>()
                        let total = item.MusicCollection.Select(s => s.Value.Sum(x => x.Quantity * x.Price))
                        select new
                        {
                            Total = total.ToList()
                        };

                    RavenTestHelper.AssertEqualRespectingNewLines(
@"declare function output(item) {
	var total = Object.map(item.MusicCollection, function(v, k){ return v.map(function(x){return x.Quantity*x.Price;}).reduce(function(a, b) { return a + b; }, 0);});
	return { Total : total };
}
from index 'TestDocumentByName' as item select output(item)", query.ToString());

                    var queryResult = query.ToList();

                    var expected = testDoc.MusicCollection
                        .Select(s => s.Value.Sum(x => x.Quantity * x.Price))
                        .ToList();

                    Assert.Equal(expected, queryResult[0].Total);

                }
            }
        }

        [Fact]
        public void CanDoMapOnObject3()
        {
            using (var store = GetDocumentStore())
            {
                var testDoc = Setup(store);

                using (var session = store.OpenSession())
                {
                    // here we use Object.keys() first

                    var query = from item in session.Query<TestDocument, TestDocumentByName>()
                                let georgeAlbums = item.MusicCollection
                                    .Where(x => x.Key.StartsWith("G"))
                                    .Select(s => s.Value.Select(x => new
                                    {
                                        x.Title, x.ReleaseDate
                                    }))
                                select new
                                {
                                    item.Name,
                                    GeorgeAlbums = georgeAlbums.ToList(),
                                };

                    RavenTestHelper.AssertEqualRespectingNewLines(
@"declare function output(item) {
	var georgeAlbums = Object.keys(item.MusicCollection).map(function(a){return{Key: a,Value:item.MusicCollection[a]};}).filter(function(x){return x.Key.startsWith(""G"");}).map(function(s){return s.Value.map(function(x){return {Title:x.Title,ReleaseDate:x.ReleaseDate};});});
	return { Name : item.Name, GeorgeAlbums : georgeAlbums };
}
from index 'TestDocumentByName' as item select output(item)", query.ToString());

                    var queryResult = query.ToList();

                    var expected = testDoc.MusicCollection
                        .Where(x => x.Key.StartsWith("G"))
                        .Select(s => s.Value.Select(x => new
                        {
                            x.Title, x.ReleaseDate
                        }))
                        .ToList();

                    Assert.Equal(expected, queryResult[0].GeorgeAlbums);

                }
            }
        }

        [Fact]
        public void CanDoMapOnObject4()
        {
            using (var store = GetDocumentStore())
            {
                var testDoc = Setup(store);

                using (var session = store.OpenSession())
                {
                    // here we use Object.map()

                    var query = from item in session.Query<TestDocument, TestDocumentByName>()
                        let artists = item.MusicCollection.Select(s => s.Value.Select(x => new { x.Title, x.ReleaseDate }))
                        select new
                        {
                            item.Name,
                            AlbumsByArtists = artists.ToList()
                        };

                    RavenTestHelper.AssertEqualRespectingNewLines(
@"declare function output(item) {
	var artists = Object.map(item.MusicCollection, function(v, k){ return v.map(function(x){return {Title:x.Title,ReleaseDate:x.ReleaseDate};});});
	return { Name : item.Name, AlbumsByArtists : artists };
}
from index 'TestDocumentByName' as item select output(item)", query.ToString());

                    var queryResult = query.ToList();

                    var expected = testDoc.MusicCollection
                        .Select(s => s.Value.Select(x => new
                        {
                            x.Title, x.ReleaseDate
                        }).ToList())
                        .ToList();

                    Assert.Equal(expected, queryResult[0].AlbumsByArtists);

                }
            }
        }

        private static TestDocument Setup(DocumentStore store)
        {
            new TestDocumentByName().Execute(store);
            var testDoc = new TestDocument
            {
                MusicCollection = new Dictionary<string, List<Album>>
                {
                    {
                        "George Harrison", new List<Album>
                        {
                            new Album
                            {
                                Title = "All things must pass",
                                ReleaseDate = new DateTime(1970, 11, 27),
                                Quantity = 25,
                                Price = 49
                            },
                            new Album
                            {
                                Title = "Dark Horse",
                                ReleaseDate = new DateTime(1974, 12, 9),
                                Quantity = 12,
                                Price = 39
                            }
                        }
                    },
                    {
                        "John Lennon", new List<Album>
                        {
                            new Album
                            {
                                Title = "Imagine",
                                ReleaseDate = new DateTime(1971, 9, 9),
                                Quantity = 40,
                                Price = 29
                            },
                            new Album
                            {
                                Title = "Mind Games",
                                ReleaseDate = new DateTime(1973, 11, 16),
                                Quantity = 18,
                                Price = 25
                            }
                        }
                    }
                }
            };
            using (var session = store.OpenSession())
            {
                session.Store(testDoc);
                session.SaveChanges();
            }

            WaitForIndexing(store);
            return testDoc;
        }

        private class TestDocumentByName : AbstractIndexCreationTask<TestDocument>
        {
            public TestDocumentByName()
            {
                Map = docs => from doc in docs select new { doc.Name, doc.PriceConfig, doc.MusicCollection };
            }
        }

        private class TestDocument
        {
            public string Id { get; set; }
            public string Name { get; set; }
            public Dictionary<string, (int Price, int Quantity)> PriceConfig { get; set; }
            public Dictionary<string, List<Album>> MusicCollection { get; set; }

        }

        private class Album
        {
            public string Title { get; set; }
            public DateTime ReleaseDate { get; set; }
            public int Quantity { get; set; }
            public int Price { get; set; }
        }
    }
}

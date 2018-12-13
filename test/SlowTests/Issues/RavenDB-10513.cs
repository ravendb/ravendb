using System.Collections.Generic;
using System.Linq;
using FastTests;
using Raven.Client.Documents.Linq;
using Xunit;

namespace SlowTests.Issues
{
    public class ProjectWithDictionaryParameter : RavenTestBase
    {
        private class Document
        {
            public string Id { get; set; }
            public List<string> Targets { get; set; }
            public Dictionary<string, int> Dict { get; set; }
        }

        [Fact]
        public void ShouldUseHasOwnProperty()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    var doc = new Document
                    {
                        Targets = Enumerable.Range(1, 5).Select(x => "target_" + x).ToList()
                    };

                    session.Store(doc);
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var dictionary = new Dictionary<string, string>
                    {
                        {"target_2", null},
                        {"target_4", null},
                    };

                    var projection =
                        from d in session.Query<Document>()
                        select new Document
                        {
                            Id = d.Id,
                            Targets = d.Targets
                                .Where(x => dictionary.Count == 0 || dictionary.ContainsKey(x))
                                .ToList()
                        };

                    Assert.Contains(".hasOwnProperty(x)", projection.ToString());

                    var docs = projection.ToList();
                    Assert.Equal(1, docs.Count);
                    Assert.Equal(2, docs[0].Targets.Count);
                }
            }
        }

        [Fact]
        public void ShouldUseHasOwnProperty2()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Document
                    {
                        Dict = new Dictionary<string, int>()
                        {
                            {"target_1", 0},
                            {"target_3", 0},
                        }
                    });

                    session.Store(new Document
                    {
                        Dict = new Dictionary<string, int>()
                        {
                            {"target_2", 0},
                            {"target_4", 0},
                        }
                    });
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var key = "target_3";

                    var projection =
                        from d in session.Query<Document>()
                        select new 
                        {
                            Id = d.Id,
                            ContainsKey = d.Dict.ContainsKey(key)
                        };

                    Assert.Contains(".hasOwnProperty($p0)", projection.ToString());

                    var docs = projection.ToList();
                    Assert.Equal(2, docs.Count);
                    Assert.True(docs[0].ContainsKey);
                    Assert.False(docs[1].ContainsKey);
                }
            }
        }
    }
}

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
    }
}

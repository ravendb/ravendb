using System;
using FastTests;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_10499 : RavenTestBase
    {
        private class Document
        {
            public string Id { get; set; }
            public string Name { get; set; }
        }

        [Fact]
        public void CanLoadAgressivelyCachingAfterDbInitialized()
        {
            using (var store = GetDocumentStore())
            {
                const string id = "myDocuments/123";

                using (var session = store.OpenSession())
                {
                    var loaded = session.Load<Document>(id);
                    if (loaded == null)
                    {
                        var doc = new Document
                        {
                            Id = id,
                            Name = "document"
                        };
                        session.Store(doc);
                        session.SaveChanges();
                    }
                }

                // now that program has started it's safe to use agressive caching as database is in valid state

                using (var session = store.OpenSession())
                {
                    using (store.AggressivelyCacheFor(TimeSpan.FromMinutes(1)))
                    {
                        var loaded = session.Load<Document>(id);
                        Assert.NotNull(loaded);
                    }
                }
            }
        }
    }
}

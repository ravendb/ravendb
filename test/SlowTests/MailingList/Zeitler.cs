using System;
using System.Linq;
using System.Text;
using FastTests;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.MailingList
{
    public class Zeitler : RavenTestBase
    {
        public Zeitler(ITestOutputHelper output) : base(output)
        {
        }

        private class PersistentCacheKey
        {
            public string Id { get; set; }
            public byte[] Hash { get; set; }
            public string RoutePattern { get; set; }
            public string ETag { get; set; }
            public DateTimeOffset LastModified { get; set; }
        }

        [RavenTheory(RavenTestCategory.Querying)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All, DatabaseMode = RavenDatabaseMode.All)]
        public void AddTest(Options options)
        {
            // want a green test? comment this	
            using (var documentStore = GetDocumentStore(options))
            {
                documentStore.Initialize();

                // want a green test? uncomment this	
                //var documentStore = new DocumentStore() {
                //	Url = "http://localhost:8082/databases/entitytagstore"
                //}.Initialize();

                byte[] hash = Encoding.UTF8.GetBytes("/api/Cars");

                var persistentCacheKey = new PersistentCacheKey()
                {
                    ETag = "\"abcdef1234\"",
                    Hash = hash,
                    LastModified = DateTime.Now,
                    RoutePattern = "/api/Cars"
                };

                using (var session = documentStore.OpenSession())
                {
                    session.Store(persistentCacheKey);
                    session.SaveChanges();
                }
                PersistentCacheKey key;
                using (var session = documentStore.OpenSession())
                {
                    key = session.Query<PersistentCacheKey>()
                        .Customize(x => x.WaitForNonStaleResults())
                        .FirstOrDefault(p => p.Hash == hash);
                }

                Assert.NotNull(key);
            }
        }
    }
}

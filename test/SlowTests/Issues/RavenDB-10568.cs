using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using FastTests;
using Raven.Server.Config;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_10568 : RavenTestBase
    {
        private class Document
        {
            public string Id { get; set; }
        }

        [Fact]
        public void QueryIdStartsWithAndQueryOptimizerGeneratedIndexesDisabled_ShouldBeAbleToQuery()
        {
            Options options = new Options
            {
                ModifyDatabaseRecord = dr =>
                {
                    dr.Settings[RavenConfiguration.GetKey(x => x.Indexing.DisableQueryOptimizerGeneratedIndexes)] = "true";
                }
            };
            using (var store = GetDocumentStore(options))
            {
                using (var session = store.OpenSession())
                {

                    session.Store(new Document
                    {
                        Id = "my-id/123"
                    });

                    session.Store(new Document
                    {
                        Id = "my-id/333"
                    });
                    session.Store(new Document
                    {
                        Id = "my-id/1234"
                    });
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var docs = session.Query<Document>()
                        .Where(x => x.Id.StartsWith("my-id/1"))
                        .ToList();

                    Assert.Equal(2, docs.Count);
                }
            }
        }
    }
}

using System.Collections.Generic;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.BulkInsert;
using Raven.Client.Documents.Operations;
using Raven.Tests.Core.Utils.Entities;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_17012 : RavenTestBase
    {
        public RavenDB_17012(ITestOutputHelper output) : base(output)
        {
        }

        [Theory]
        [InlineData(10)]
        [InlineData(500)]
        [InlineData(5_000)]
        public async Task Can_SkipOverwriteIfUnchanged(int docsCount)
        {
            using (var store = GetDocumentStore())
            {
                var docs = new List<User>();

                using (var bulk = store.BulkInsert())
                {
                    for (var i = 0; i < docsCount; i++)
                    {
                        var user = new User { Age = i };
                        docs.Add(user);
                        await bulk.StoreAsync(user, i.ToString());
                    }
                }

                var stats = store.Maintenance.Send(new GetStatisticsOperation());
                var lastEtag = stats.LastDocEtag;

                using (var bulk = store.BulkInsert(new BulkInsertOptions
                {
                    SkipOverwriteIfUnchanged = true
                }))
                {
                    for (var i = 0; i < docsCount; i++)
                    {
                        var doc = docs[i];
                        await bulk.StoreAsync(doc, i.ToString());
                    }
                }

                stats = store.Maintenance.Send(new GetStatisticsOperation());
                Assert.Equal(lastEtag, stats.LastDocEtag);
            }
        }

        [Theory]
        [InlineData(10)]
        [InlineData(500)]
        [InlineData(5_000)]
        public async Task Can_SkipOverwriteIfUnchanged_SomeDocuments(int docsCount)
        {
            using (var store = GetDocumentStore())
            {
                var docs = new List<User>();

                using (var bulk = store.BulkInsert())
                {
                    for (var i = 0; i < docsCount; i++)
                    {
                        var user = new User { Age = i };
                        docs.Add(user);
                        await bulk.StoreAsync(user, i.ToString());
                    }
                }

                var stats = store.Maintenance.Send(new GetStatisticsOperation());
                var lastEtag = stats.LastDocEtag;

                using (var bulk = store.BulkInsert(new BulkInsertOptions
                {
                    SkipOverwriteIfUnchanged = true
                }))
                {
                    for (var i = 0; i < docsCount; i++)
                    {
                        var doc = docs[i];
                        if (i % 2 == 0)
                            doc.Age = (i + 1) * 2;

                        await bulk.StoreAsync(doc, i.ToString());
                    }
                }

                stats = store.Maintenance.Send(new GetStatisticsOperation());
                Assert.Equal(lastEtag + docsCount / 2, stats.LastDocEtag);
            }
        }
    }
}

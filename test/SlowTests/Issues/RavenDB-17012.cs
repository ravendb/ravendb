using System.Collections.Generic;
using System.Threading.Tasks;
using FastTests;
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

                using (var bulk = store.BulkInsert())
                {
                    bulk.SkipOverwriteIfUnchanged = true;

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
    }
}

using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.Revisions;
using Raven.Tests.Core.Utils.Entities;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_21501 : ClusterTestBase
    {
        public RavenDB_21501(ITestOutputHelper output) : base(output)
        {
        }

        [RavenFact(RavenTestCategory.Revisions)]
        public async Task RevisionsBinRepeatingSameDocs()
        {
            using (var store = GetDocumentStore())
            {
                await store.Maintenance.SendAsync(new ConfigureRevisionsOperation(new RevisionsConfiguration {Default = new RevisionsCollectionConfiguration()}));

                using (var session = store.OpenSession())
                {
                    for (int i = 0; i < 200; i++)
                    {
                        session.Store(new User(), $"users/{i}");
                    }
                    session.SaveChanges();

                    var stats = await store.Maintenance.SendAsync(new GetStatisticsOperation("test"));
                    Assert.Equal(200, stats.CountOfDocuments);

                    for (int i = 0; i < 200; i++)
                    {
                        session.Delete($"users/{i}");
                    }
                    session.SaveChanges();
                }
                
                var revsBin = await store.Commands().GetRevisionsBinEntriesAsync(0, pageSize: 100);
                Assert.Equal(100, revsBin.Count());

                revsBin = await store.Commands().GetRevisionsBinEntriesAsync(100, pageSize: 100);
                Assert.Equal(100, revsBin.Count());

                revsBin = await store.Commands().GetRevisionsBinEntriesAsync(200, pageSize: 100);
                Assert.Equal(0, revsBin.Count());
            }
        }
    }
}

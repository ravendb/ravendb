using System;
using System.Threading.Tasks;
using Raven.Client.Documents.Smuggler;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_24002 : ReplicationTestBase
    {
        public RavenDB_24002(ITestOutputHelper output) : base(output)
        {
        }

        [RavenFact(RavenTestCategory.Revisions | RavenTestCategory.TimeSeries)]
        public async Task CanQueryTimeSeriesUsingDocumentQuery()
        {
            using (var stream = GetType().Assembly.GetManifestResourceStream("SlowTests.Data.RavenDB_24002.TimeseriesWithConflictedRevisions_new.ravendbdump"))
            using (var stream2 = GetType().Assembly.GetManifestResourceStream("SlowTests.Data.RavenDB_24002.TimeseriesWithConflictedRevisions_new.ravendbdump"))
            using (var store1 = GetDocumentStore())
            using (var store2 = GetDocumentStore())
            {
                Assert.NotNull(stream);
                Assert.NotNull(stream2);

                var operation = await store1.Smuggler.ImportAsync(new DatabaseSmugglerImportOptions(), stream);
                operation.WaitForCompletion<SmugglerResult>(TimeSpan.FromSeconds(30));

                var operation2 = await store2.Smuggler.ImportAsync(new DatabaseSmugglerImportOptions(), stream2);
                operation2.WaitForCompletion<SmugglerResult>(TimeSpan.FromSeconds(30));

                using (var session = store2.OpenAsyncSession())
                {
                    var o = await session.LoadAsync<dynamic>("TestId-3-B");
                    o["Name"] += "-B";
                    await session.SaveChangesAsync();
                }

                using (var session = store1.OpenAsyncSession())
                {
                    var o = await session.LoadAsync<dynamic>("TestId-3-B");
                    o["Name"] += "-A";
                    await session.SaveChangesAsync();
                }

                await SetupReplicationAsync(store2, store1);
                await EnsureReplicatingAsync(store2, store1);

                using (var session = store1.OpenAsyncSession())
                {
                    var o = await session.LoadAsync<dynamic>("TestId-3-B");
                    var nonRawEntries = await session.TimeSeriesFor("TestId-3-B", "GpsAndSPN").GetAsync();
                    Assert.NotEmpty(nonRawEntries);
                }

                using (var session = store2.OpenAsyncSession())
                {
                    var o = await session.LoadAsync<dynamic>("TestId-3-B");
                    var nonRawEntries = await session.TimeSeriesFor("TestId-3-B", "GpsAndSPN").GetAsync();
                    Assert.NotEmpty(nonRawEntries);
                }
            }
        }
    }
}

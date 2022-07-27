using System;
using System.Threading.Tasks;
using Raven.Client;
using Raven.Client.Documents.Operations.TimeSeries;
using Raven.Tests.Core.Utils.Entities;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace InterversionTests
{
    public class RavenDB_17556 : InterversionTestBase
    {
        public RavenDB_17556(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public async Task IncomingReplicationShouldRejectIncrementalTimeSeriesFromOldServer()
        {
            const string version = "5.2.3";
            const string incrementalTsName = Constants.Headers.IncrementalTimeSeriesPrefix + "HeartRate";
            const string docId = "users/1";

            using (var oldStore = await GetDocumentStoreAsync(version))
            using (var store = GetDocumentStore())
            {
                using (var session = oldStore.OpenAsyncSession())
                {
                    await session.StoreAsync(new User(), docId);
                    await session.SaveChangesAsync();
                }

                var tsOp = new TimeSeriesOperation
                {
                    Name = incrementalTsName
                };

                tsOp.Append(new TimeSeriesOperation.AppendOperation
                {
                    Timestamp = DateTime.UtcNow,
                    Values = new[] { 1d }
                });

                await oldStore.Operations.SendAsync(new TimeSeriesBatchOperation(docId, tsOp));

                await ReplicationTests.SetupReplication(oldStore, store);
                Assert.False(WaitForDocument(store, docId, timeout: 3000));

                var notificationCenter = (await Databases.GetDocumentDatabaseInstanceFor(store)).NotificationCenter;
                var msg = notificationCenter.GetStoredMessage("AlertRaised/Replication");
                Assert.NotNull(msg);
                Assert.Contains($"Detected an item of type Incremental-TimeSeries : '{incrementalTsName}' on document '{docId}", msg);
            }
        }
    }
}

using System;
using System.Linq;
using System.Threading.Tasks;
using Raven.Client;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Client.Documents.Operations.ETL;
using Raven.Client.Documents.Operations.OngoingTasks;
using Raven.Client.Documents.Operations.Replication;
using Raven.Client.Documents.Operations.TimeSeries;
using Raven.Client.Exceptions;
using Raven.Tests.Core.Utils.Entities;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace InterversionTests
{
    public class ReplicationTests : InterversionTestBase
    {
        public ReplicationTests(ITestOutputHelper output) : base(output)
        {
        }

        [Theory(Skip = "TODO: Add compatible version of v4.2 when released")]
        public async Task CannotReplicateTimeSeriesToV42()
        {
            var version = "4.2.101"; // todo:Add compatible version of v4.2 when released
            var getOldStore = GetDocumentStoreAsync(version);
            await Task.WhenAll(getOldStore);

            using var oldStore = await getOldStore;
            using var store = GetDocumentStore();
            using (var session = store.OpenAsyncSession())
            {
                await session.StoreAsync(new User {Name = "Egor"}, "user/322");
                session.TimeSeriesFor("user/322", "a").Append(DateTime.UtcNow, 1);
                await session.SaveChangesAsync();
            }

            var externalTask = new ExternalReplication(oldStore.Database.ToLowerInvariant(), "MyConnectionString")
            {
                Name = "MyExternalReplication",
                Url = oldStore.Urls.First()
            };

            await SetupReplication(store, externalTask);

            var replicationLoader = (await GetDocumentDatabaseInstanceFor(store)).ReplicationLoader;
            Assert.NotEmpty(replicationLoader.OutgoingFailureInfo);
            Assert.True(WaitForValue(() => replicationLoader.OutgoingFailureInfo.Any(ofi => ofi.Value.RetriesCount > 2), true));
            Assert.True(replicationLoader.OutgoingFailureInfo.Any(ofi => ofi.Value.Errors.Any(x => x.GetType() == typeof(LegacyReplicationViolationException))));
            Assert.True(replicationLoader.OutgoingFailureInfo.Any(ofi => ofi.Value.Errors.Select(x => x.Message).Any(x => x.Contains("TimeSeries"))));
        }

        [Theory]
        [InlineData("4.2.117")]
        [InlineData("5.2.3")]
        public async Task CanReplicateToOldServerWithLowerReplicationProtocolVersion(string version)
        {
            // https://issues.hibernatingrhinos.com/issue/RavenDB-17346

            using var oldStore = await GetDocumentStoreAsync(version);
            using var store = GetDocumentStore();
            {
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "Egor" }, "users/1");
                    await session.SaveChangesAsync();
                }
            }

            var externalTask = new ExternalReplication(oldStore.Database.ToLowerInvariant(), "MyConnectionString")
            {
                Name = "MyExternalReplication",
                Url = oldStore.Urls.First()
            };

            await SetupReplication(store, externalTask);

            Assert.True(WaitForDocument<User>(oldStore, "users/1", u => u.Name == "Egor"));
        }

        [Fact]
        public async Task IncrementalTimeSeriesWithDuplicatesShouldNotBreakReplicationToOldServer()
        {
            const string version = "5.2.3";
            const string incrementalTsName = Constants.Headers.IncrementalTimeSeriesPrefix + "HeartRate";
            const string docId = "users/1";
            var baseline = DateTime.UtcNow;

            using (var oldStore = await GetDocumentStoreAsync(version))
            using (var storeA = GetDocumentStore())
            using (var storeB = GetDocumentStore())
            {
                await SetupReplication(storeA, storeB);
                await SetupReplication(storeB, storeA);

                using (var session = storeA.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "ayende" }, docId);
                    await session.SaveChangesAsync();
                }
                Assert.True(WaitForDocument<User>(storeB, docId, u => u.Name == "ayende"));

                // increment on storeA
                using (var session = storeA.OpenSession())
                {
                    session.IncrementalTimeSeriesFor(docId, incrementalTsName)
                        .Increment(baseline, 1);

                    session.SaveChanges();
                }
                await EnsureReplicatingAsync(storeA, storeB);

                // increment on storeB
                using (var session = storeB.OpenSession())
                {
                    session.IncrementalTimeSeriesFor(docId, incrementalTsName)
                        .Increment(baseline, 1);

                    session.SaveChanges();
                }
                await EnsureReplicatingAsync(storeB, storeA);

                foreach (var store in new[] { storeA, storeB })
                {
                    var values = store.Operations
                        .Send(new GetTimeSeriesOperation(docId, incrementalTsName, returnFullResults: true));

                    Assert.NotNull(values.TotalResults);
                    Assert.NotNull(values.SkippedResults);

                    Assert.Equal(2, values.TotalResults);
                    Assert.Equal(1, values.SkippedResults);

                    foreach (var entry in values.Entries)
                    {
                        Assert.NotEmpty(entry.NodeValues);
                        Assert.Equal(2, entry.NodeValues.Count);

                        foreach (var nodeValue in entry.NodeValues)
                        {
                            Assert.Equal(1, nodeValue.Value.Length);
                            Assert.Equal(1, nodeValue.Value[0]);
                        }
                    }
                }

                // replicate to old server
                // this replication batch should work, because the destination will append the entire ts-segment
                await SetupReplication(storeA, oldStore);

                await EnsureReplicatingAsync(storeA, oldStore);
                await EnsureNoReplicationLoop(Server, storeA.Database);

                // increment on storeA
                using (var session = storeA.OpenSession())
                {
                    session.IncrementalTimeSeriesFor(docId, incrementalTsName)
                        .Increment(baseline, 1);

                    session.SaveChanges();
                }

                // now the destination can't append the entire segment and will enumerate all values
                await EnsureReplicatingAsync(storeA, oldStore);
                await EnsureNoReplicationLoop(Server, storeA.Database);
            }
        }

        private static async Task<ModifyOngoingTaskResult> SetupReplication(IDocumentStore src, IDocumentStore dst)
        {
            var csName = $"cs-to-{dst.Database}";
            var result = await src.Maintenance.SendAsync(new PutConnectionStringOperation<RavenConnectionString>(new RavenConnectionString
            {
                Name = csName,
                Database = dst.Database,
                TopologyDiscoveryUrls = new[]
                {
                    dst.Urls.First()
                }
            }));
            Assert.NotNull(result.RaftCommandIndex);

            var op = new UpdateExternalReplicationOperation(new ExternalReplication(dst.Database.ToLowerInvariant(), csName)
            {
                Name = $"ExternalReplicationTo{dst.Database}",
                Url = dst.Urls.First()
            });

            return await src.Maintenance.SendAsync(op);
        }


        private static async Task<ModifyOngoingTaskResult> SetupReplication(IDocumentStore store, ExternalReplicationBase watcher)
        {
            var result = await store.Maintenance.SendAsync(new PutConnectionStringOperation<RavenConnectionString>(new RavenConnectionString
            {
                Name = watcher.ConnectionStringName,
                Database = watcher.Database,
                TopologyDiscoveryUrls = new[]
                {
                    watcher.Url
                }
            }));
            Assert.NotNull(result.RaftCommandIndex);

            IMaintenanceOperation<ModifyOngoingTaskResult> op;
            switch (watcher)
            {
                case PullReplicationAsSink pull:
                    op = new UpdatePullReplicationAsSinkOperation(pull);
                    break;
                case ExternalReplication ex:
                    op = new UpdateExternalReplicationOperation(ex);
                    break;
                default:
                    throw new ArgumentException($"Unrecognized type: {watcher.GetType().FullName}");
            }

            return await store.Maintenance.SendAsync(op);
        }
    }
}

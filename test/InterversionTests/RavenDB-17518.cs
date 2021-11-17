using System.Linq;
using System.Threading.Tasks;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Client.Documents.Operations.ETL;
using Raven.Client.Documents.Operations.OngoingTasks;
using Raven.Client.Documents.Operations.Replication;
using Raven.Client.Exceptions;
using Raven.Tests.Core.Utils.Entities;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace InterversionTests
{
    public class RavenDB_17518 : InterversionTestBase
    {
        public RavenDB_17518(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public async Task ShouldNotReplicateCountersToOldServer()
        {
            const string docId = "users/1";
            using (var oldStore = await GetDocumentStoreAsync("4.0.7"))
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "ayende" }, docId);
                    session.CountersFor(docId)
                        .Increment("likes");
                    await session.SaveChangesAsync();
                }

                using (var session = store.OpenAsyncSession())
                {
                    var u = await session.LoadAsync<User>(docId);
                    session.CountersFor(u)
                        .Increment("likes");

                    u.Name = "oren";

                    await session.SaveChangesAsync();
                }

                await SetupReplication(store, oldStore);

                var replicationLoader = (await GetDocumentDatabaseInstanceFor(store)).ReplicationLoader;
                Assert.NotEmpty(replicationLoader.OutgoingFailureInfo);
                Assert.True(WaitForValue(() => replicationLoader.OutgoingFailureInfo.Any(ofi => ofi.Value.RetriesCount > 2), true));
                Assert.True(replicationLoader.OutgoingFailureInfo.Any(ofi => ofi.Value.Errors.Any(x => x.GetType() == typeof(LegacyReplicationViolationException))));
                Assert.True(replicationLoader.OutgoingFailureInfo.Any(ofi => ofi.Value.Errors.Select(x => x.Message).Any(x => x.Contains("CounterGroup"))));
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
    }
}

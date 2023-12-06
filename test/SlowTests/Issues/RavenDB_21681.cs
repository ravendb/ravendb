using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using FastTests.Server.Replication;
using FastTests.Utils;
using Raven.Client;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.Expiration;
using SlowTests.Core.Utils.Entities;
using Sparrow;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_21681 : ReplicationTestBase
    {
        public RavenDB_21681(ITestOutputHelper output) : base(output)
        {
        }

        [RavenTheory(RavenTestCategory.Cluster | RavenTestCategory.Replication | RavenTestCategory.TimeSeries | RavenTestCategory.ExpirationRefresh)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public async Task DeleteExpiredDocumentWithBigTimeSeriesShouldNotCauseReplicationToBreak(Options options)
        {
            var (nodes, leader) = await CreateRaftCluster(2, watcherCluster: true);

            options.Server = leader;
            options.ReplicationFactor = 2;
            using (var store = GetDocumentStore(options))
            {
                var user = new User { Name = "Shiran" };

                var expiry = DateTime.Now.AddYears(-1).ToUniversalTime();
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(user);
                    var metadata = session.Advanced.GetMetadataFor(user);
                    metadata[Constants.Documents.Metadata.Expires] = expiry.ToString(DefaultFormat.DateTimeFormatsToRead[0]);
                    await session.SaveChangesAsync();
                }

                using (var session = store.OpenAsyncSession())
                {
                    var ts = session.TimeSeriesFor(user.Id, "heartbeat");
                    for (int i = 0; i < 500_000; i++)
                        ts.Append(expiry.AddMilliseconds(i), new List<double> { i, i * 100, i * 200, i * int.MaxValue });

                    await session.SaveChangesAsync();
                }

                Assert.True(await WaitForChangeVectorInClusterAsync(nodes, store.Database, 30_000));

                await ExpirationHelper.SetupExpiration(store, leader.ServerStore, new ExpirationConfiguration { Disabled = false, DeleteFrequencyInSec = 5 });

                await WaitAndAssertForValueAsync(async () =>
                {
                    using (var session = store.OpenAsyncSession())
                    {
                        var u = await session.LoadAsync<User>(user.Id);
                        return u == null;
                    }

                }, true);

                var user2 = new User { Name = "Shiran2" };
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(user2);
                    await session.SaveChangesAsync();
                }

                Assert.True(await WaitForDocumentInClusterAsync<User>(nodes, store.Database, user2.Id, u => u.Name == "Shiran2", TimeSpan.FromSeconds(30)));

                foreach (var server in nodes)
                {
                    var stats = await store.Maintenance.SendAsync(new GetStatisticsOperation(server.DebugTag, server.ServerStore.NodeTag));
                    Assert.Equal(0, stats.CountOfTimeSeriesSegments);
                }
            }
        }
    }
}

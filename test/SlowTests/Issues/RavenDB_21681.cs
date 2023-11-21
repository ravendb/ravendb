using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using FastTests.Utils;
using Raven.Client;
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
            var databaseName = GetDatabaseName();
            var (nodes, leader) = await CreateRaftCluster(2, watcherCluster: true);
            var (_, servers) = await CreateDatabaseInClusterForMode(databaseName, 2, (nodes, leader), options.DatabaseMode);

            options.CreateDatabase = false;
            options.ModifyDatabaseName = _ => databaseName;
            options.Server = leader;

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

                Assert.True(await WaitForChangeVectorInClusterForModeAsync(nodes, databaseName, options.DatabaseMode, 2, 30_000));

                await ExpirationHelper.SetupExpirationAsync(store, new ExpirationConfiguration { Disabled = false, DeleteFrequencyInSec = 5 });

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

                Assert.True(await WaitForDocumentInClusterAsync<User>(servers, databaseName, user2.Id, u => u.Name == "Shiran2", TimeSpan.FromSeconds(30)));
            }
        }
    }
}

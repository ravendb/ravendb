using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using FastTests.Server.Replication;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Client.Documents.Operations.ETL;
using Raven.Client.Documents.Operations.OngoingTasks;
using Raven.Client.Documents.Operations.Replication;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;
using Raven.Server.Config;
using Raven.Server.ServerWide.Commands;
using Raven.Tests.Core.Utils.Entities;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Server.Replication
{
    public class ExternalReplicationTests : ReplicationTestBase
    {
        public ExternalReplicationTests(ITestOutputHelper output) : base(output)
        {
        }

        [Theory]
        [InlineData(3000)]
        public async Task ExternalReplicationShouldWorkWithSmallTimeoutStress(int timeout)
        {
            using (var store1 = GetDocumentStore(new Options
            {
                ModifyDatabaseName = s => $"{s}_FooBar-1"
            }))
            using (var store2 = GetDocumentStore(new Options
            {
                ModifyDatabaseName = s => $"{s}_FooBar-2"
            }))
            {
                await SetupReplicationAsync(store1, store2);

                using (var s1 = store1.OpenSession())
                {
                    s1.Store(new User(), "foo/bar");
                    s1.SaveChanges();
                }

                WaitForUserToContinueTheTest(store1);

                // SlowTests uses the regular old value of 3000mSec. But if called from StressTests - needs more timeout
                Assert.True(WaitForDocument(store2, "foo/bar", timeout), store2.Identifier);
            }
        }

        [Fact]
        public async Task DelayedExternalReplication()
        {
            using (var store1 = GetDocumentStore())
            using (var store2 = GetDocumentStore())
            {
                var delay = TimeSpan.FromSeconds(5);
                var externalTask = new ExternalReplication(store2.Database, "DelayedExternalReplication")
                {
                    DelayReplicationFor = delay
                };
                await AddWatcherToReplicationTopology(store1, externalTask);
                DateTime date;

                using (var s1 = store1.OpenSession())
                {
                    s1.Store(new User(), "foo/bar");
                    date = DateTime.UtcNow;
                    s1.SaveChanges();
                }

                Assert.True(WaitForDocument(store2, "foo/bar"));
                var elapsed = DateTime.UtcNow - date;
                Assert.True(elapsed >= delay, $" only {elapsed}/{delay} ticks elapsed");

            }
        }

        [Fact]
        public async Task EditExternalReplication()
        {
            using (var store1 = GetDocumentStore())
            using (var store2 = GetDocumentStore())
            {
                var delay = TimeSpan.FromSeconds(5);
                var externalTask = new ExternalReplication(store2.Database, "DelayedExternalReplication")
                {
                    DelayReplicationFor = delay
                };
                await AddWatcherToReplicationTopology(store1, externalTask);
                DateTime date;

                using (var s1 = store1.OpenSession())
                {
                    s1.Store(new User(), "foo/bar");
                    date = DateTime.UtcNow;
                    s1.SaveChanges();
                }

                Assert.True(WaitForDocument(store2, "foo/bar"));
                var elapsed = DateTime.UtcNow - date;
                Assert.True(elapsed >= delay, $" only {elapsed}/{delay} ticks elapsed");

                delay = TimeSpan.Zero;
                externalTask.DelayReplicationFor = delay;
                var op = new UpdateExternalReplicationOperation(externalTask);
                await store1.Maintenance.SendAsync(op);

                using (var s1 = store1.OpenSession())
                {
                    s1.Store(new User(), "foo/bar");
                    date = DateTime.UtcNow;
                    s1.SaveChanges();
                }

                Assert.True(WaitForDocument(store2, "foo/bar"));
                var elapsedTime = DateTime.UtcNow - date;
                Assert.True(elapsedTime >= delay && elapsedTime < elapsed, $" only {elapsed}/{delay} ticks elapsed");
            }
        }

        [Fact]
        public async Task CanChangeConnectionString()
        {
            using (var store1 = GetDocumentStore())
            using (var store2 = GetDocumentStore())
            {
                var externalReplication = new ExternalReplication
                {
                    ConnectionStringName = "ExReplication"
                };
                await store1.Maintenance.SendAsync(new PutConnectionStringOperation<RavenConnectionString>(new RavenConnectionString
                {
                    Name = externalReplication.ConnectionStringName,
                    Database = "NotExist",
                    TopologyDiscoveryUrls = store1.Urls
                }));
                await store1.Maintenance.SendAsync(new UpdateExternalReplicationOperation(externalReplication));

                using (var s1 = store1.OpenSession())
                {
                    s1.Store(new User(), "foo/bar");
                    s1.SaveChanges();
                }

                Assert.False(WaitForDocument(store2, "foo/bar", timeout: 5_000));

                await store1.Maintenance.SendAsync(new PutConnectionStringOperation<RavenConnectionString>(new RavenConnectionString
                {
                    Name = externalReplication.ConnectionStringName,
                    Database = store2.Database,
                    TopologyDiscoveryUrls = store1.Urls
                }));

                Assert.True(WaitForDocument(store2, "foo/bar", timeout: 5_000));

                await store1.Maintenance.SendAsync(new PutConnectionStringOperation<RavenConnectionString>(new RavenConnectionString
                {
                    Name = externalReplication.ConnectionStringName,
                    Database = "NotExist",
                    TopologyDiscoveryUrls = store1.Urls
                }));

                using (var s1 = store1.OpenSession())
                {
                    s1.Store(new User(), "foo/bar/2");
                    s1.SaveChanges();
                }
                Assert.False(WaitForDocument(store2, "foo/bar/2", timeout: 5_000));
            }
        }


        [Fact]
        public async Task ExternalReplicationToNonExistingDatabase()
        {
            using (var store1 = GetDocumentStore())
            using (var store2 = GetDocumentStore())
            {
                var externalTask = new ExternalReplication(store2.Database + "test", $"Connection to {store2.Database} test");
                await AddWatcherToReplicationTopology(store1, externalTask);
            }
        }

        [Fact]
        public async Task ExternalReplicationToNonExistingNode()
        {
            using (var store1 = GetDocumentStore())
            {
                var externalTask = new ExternalReplication(store1.Database + "test", $"Connection to {store1.Database} test");
                await AddWatcherToReplicationTopology(store1, externalTask, new []{"http://1.2.3.4:8080"});
            }
        }
    }
}

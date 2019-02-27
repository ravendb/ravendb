using System;
using System.Threading.Tasks;
using FastTests.Server.Replication;
using Raven.Client.Documents.Operations.Replication;
using Raven.Tests.Core.Utils.Entities;
using Xunit;

namespace SlowTests.Server.Replication
{
    public class ExternalReplicationTests : ReplicationTestBase
    {
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
            using (var store3 = GetDocumentStore(new Options
            {
                ModifyDatabaseName = s => $"{s}_FooBar-3"
            }))
            {
                await SetupReplicationAsync(store1, store2, store3);

                using (var s1 = store1.OpenSession())
                {
                    s1.Store(new User(), "foo/bar");
                    s1.SaveChanges();
                }
                // SlowTests uses the regular old value of 3000mSec. But if called from StressTests - needs more timeout
                Assert.True(WaitForDocument(store2, "foo/bar", timeout), store2.Identifier);
                Assert.True(WaitForDocument(store3, "foo/bar", timeout), store3.Identifier);
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
                Assert.True(elapsed >= delay,$" only {elapsed}/{delay} ticks elapsed");

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
    }
}

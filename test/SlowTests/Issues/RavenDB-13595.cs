using System.Threading.Tasks;
using FastTests.Server.Replication;
using Raven.Tests.Core.Utils.Entities;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_13595 : ReplicationTestBase
    {
        public RavenDB_13595(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public async Task ShouldCloneTheConflictedHiLoDocumentOnReplication()
        {
            using (var store1 = GetDocumentStore(new Options { ModifyDatabaseName = s => s + "_foo1" }))
            using (var store2 = GetDocumentStore(new Options { ModifyDatabaseName = s => s + "_foo2" }))
            {
                using (var s1 = store1.OpenSession())
                {
                    s1.Store(new User(), "marker/doc");
                    s1.SaveChanges();

                    s1.Store(new User { Name = "EGOR" });
                    s1.SaveChanges();
                }

                using (var s2 = store2.OpenSession())
                {
                    for (var i = 0; i < 33; i++)
                    {
                        s2.Store(new User
                        {
                            Name = $"user_{i}"
                        });
                    }
                    s2.SaveChanges();
                }

                await SetupReplicationAsync(store1, store2);
                var marker = WaitForDocumentToReplicate<User>(store2,"marker/doc", 15 * 1000);
                Assert.NotNull(marker);
            }
        }
    }
}

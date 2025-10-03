using System.Threading.Tasks;
using Raven.Client.ServerWide;
using Raven.Tests.Core.Utils.Entities;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_10955 : ReplicationTestBase
    {
        public RavenDB_10955(ITestOutputHelper output) : base(output)
        {
        }

        [RavenTheory(RavenTestCategory.Replication)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public async Task ConflictResolutionShouldPreserveDocumentIdCasing(Options options)
        {
            using (var store1 = GetDocumentStore(new Options(options)
            {
                ModifyDatabaseName = s => $"{s}_foo1"
            }))
            using (var store2 = GetDocumentStore(new Options
            {
                ModifyDatabaseName = s => $"{s}_foo2",
                ModifyDatabaseRecord = record =>
                {
                    record.ConflictSolverConfig = new ConflictSolver
                    {
                        ResolveToLatest = true
                    };
                    options.ModifyDatabaseRecord?.Invoke(record);
                }
            }))
            {
                using (var s1 = store1.OpenSession())
                {
                    s1.Store(new User { Name = "test" }, "Foo/Bar");
                    s1.SaveChanges();
                }

                using (var s2 = store2.OpenSession())
                {
                    s2.Store(new User { Name = "test2" }, "Foo/Bar");
                    s2.SaveChanges();
                }

                using (var s2 = store2.OpenSession())
                {
                    var user = s2.Load<User>("foo/bar");
                    Assert.Equal("Foo/Bar", user.Id);
                }

                await SetupReplicationAsync(store1, store2);
                await EnsureReplicatingAsync(store1, store2);

                using (var s2 = store2.OpenSession())
                {
                    var user = s2.Load<User>("foo/bar");
                    Assert.Equal("Foo/Bar", user.Id); // Id should not be lowercased
                }
            }
        }
    }
}

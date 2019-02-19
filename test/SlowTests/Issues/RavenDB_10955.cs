using System;
using System.Threading.Tasks;
using FastTests.Server.Replication;
using Orders;
using Raven.Client.Documents;
using Raven.Client.ServerWide;
using Raven.Tests.Core.Utils.Entities;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_10955 : ReplicationTestBase
    {
        [Fact]
        public async Task ConflictResolutionShouldPreserveDocumentIdCasing()
        {
            using (var store1 = GetDocumentStore(new Options
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

                WaitForMarker(store1, store2);

                using (var s2 = store2.OpenSession())
                {
                    var user = s2.Load<User>("foo/bar");
                    Assert.Equal("Foo/Bar", user.Id); // Id should not be lowercased
                }
            }
        }

        private void WaitForMarker(DocumentStore store1, DocumentStore store2)
        {
            var id = "marker - " + Guid.NewGuid();
            using (var session = store1.OpenSession())
            {
                session.Store(new Product { Name = "Marker" }, id);
                session.SaveChanges();
            }
            Assert.True(WaitForDocument(store2, id));
        }
    }
}

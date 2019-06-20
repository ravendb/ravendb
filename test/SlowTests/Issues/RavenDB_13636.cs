using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Operations.Replication;
using Raven.Client.Exceptions;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_13636 : RavenTestBase
    {
        [Fact]
        public async Task EnsureConnectionStringNameCantBeNull()
        {
            using (var store = GetDocumentStore())
            {
                var pull = new PullReplicationAsSink(store.Database, "test", "dummy");
                pull.ConnectionStringName = null;
                var op = new UpdatePullReplicationAsSinkOperation(pull);
                var ex = await Assert.ThrowsAsync<RavenException>(async () => await store.Maintenance.SendAsync(op));
                Assert.StartsWith("Raven.Server.Rachis.RachisApplyException: Failed to update database record. ---> System.ArgumentNullException: Value cannot be null.", ex.Message);
            }
        }
    }
}

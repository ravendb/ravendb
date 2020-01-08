using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Operations.Replication;
using Raven.Client.Exceptions;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_13636 : RavenTestBase
    {
        public RavenDB_13636(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public async Task EnsureConnectionStringNameCantBeNull()
        {
            using (var store = GetDocumentStore())
            {
                var pull = new PullReplicationAsSink(store.Database, "test", "dummy");
                pull.ConnectionStringName = null;
                var op = new UpdatePullReplicationAsSinkOperation(pull);
                var ex = await Assert.ThrowsAsync<RavenException>(async () => await store.Maintenance.SendAsync(op));
                RavenTestHelper.AssertStartsWithRespectingNewLines("Raven.Server.Rachis.RachisApplyException: Failed to update database record.\r\n ---> System.ArgumentNullException: Value cannot be null.", ex.Message);
            }
        }
    }
}

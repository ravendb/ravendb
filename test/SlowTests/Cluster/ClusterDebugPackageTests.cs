using System.Threading.Tasks;
using Raven.Client.Documents;
using Tests.Infrastructure;
using Xunit;
using Tests.Infrastructure.Operations;
using Xunit.Abstractions;

namespace SlowTests.Cluster
{
    public class ClusterDebugPackageTests : ClusterTestBase
    {
        public ClusterDebugPackageTests(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public async Task GetClusterDebugPackage()
        {
            var db = "TestDatabaseNodes";
            var db2 = "TestDatabaseNodes2";
            var db3 = "TestDatabaseNodes3";
            var db4 = "TestDatabaseNodes4";
            var (_, leader) = await CreateRaftCluster(3);
            await CreateDatabaseInCluster(db, 2, leader.WebUrl);
            await CreateDatabaseInCluster(db2, 2, leader.WebUrl);
            await CreateDatabaseInCluster(db3, 2, leader.WebUrl);
            await CreateDatabaseInCluster(db4, 2, leader.WebUrl);
            using (var store = new DocumentStore
            {
                Database = db,
                Urls = new[] { leader.WebUrl }
            }.Initialize())
            {
                await store.Maintenance.Server.SendAsync(new GetClusterDebugInfoPackageOperation());
            }
        }
        
    }
}

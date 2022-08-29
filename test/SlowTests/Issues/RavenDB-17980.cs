using System.IO;
using EmbeddedTests.TestDriver;
using SlowTests.Core.Utils.Entities;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_17980 : TestDriverExampleTest
    {
        public RavenDB_17980()
        {
        }

        [Fact]
        public void disable_Creating_Topology_Files_For_TestDrivers()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User
                    {
                        Name = "Yonatan",
                    });
                    session.SaveChanges();
                }

                var numberOfDatabaseTopologyFiles = Directory.GetFiles(store.Conventions.TopologyCacheLocation, "*.raven-database-topology").Length;
                Assert.True(numberOfDatabaseTopologyFiles == 0, $"number of files: {numberOfDatabaseTopologyFiles}");

                var numberOfClusterTopologyFiles = Directory.GetFiles(store.Conventions.TopologyCacheLocation, "*.raven-database-topology").Length;
                Assert.True(numberOfClusterTopologyFiles == 0, $"number of files: {numberOfClusterTopologyFiles}");
            }
        }
    }
}

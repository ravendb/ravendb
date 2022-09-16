using System.IO;
using EmbeddedTests.TestDriver;
using Raven.Client.Documents;
using Raven.Server.Utils;
using Raven.TestDriver;
using SlowTests.Core.Utils.Entities;
using Xunit;

namespace SlowTests.Issues
{
    public abstract class Abstract_RavenDB_17980 : RavenTestDriver
    {
        static Abstract_RavenDB_17980()
        {
            ConfigureServer(new TestServerOptions
            {
                ServerDirectory = TestDriverExampleTest.GetServerPath()
            });
        }

        protected override void PreInitialize(IDocumentStore documentStore)
        {
            var dir = Path.Combine(documentStore.Conventions.TopologyCacheLocation, "TopologyCache", documentStore.Database);

            IOExtensions.DeleteDirectory(dir);
            Directory.CreateDirectory(dir);

            documentStore.Conventions.TopologyCacheLocation = dir;
        }
    }

    public class Test_Without_Overriding_PreInitialize : Abstract_RavenDB_17980
    {
        [Fact]
        public void Disable_Creating_Topology_Files_For_TestDrivers()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "Yonatan", });
                    session.SaveChanges();
                }

                var numberOfDatabaseTopologyFiles = Directory.GetFiles(store.Conventions.TopologyCacheLocation, "*.raven-database-topology").Length;
                Assert.True(numberOfDatabaseTopologyFiles == 0, $"number of files: {numberOfDatabaseTopologyFiles}");

                var numberOfClusterTopologyFiles = Directory.GetFiles(store.Conventions.TopologyCacheLocation, "*.raven-cluster-topology").Length;
                Assert.True(numberOfClusterTopologyFiles == 0, $"number of files: {numberOfClusterTopologyFiles}");

                IOExtensions.DeleteDirectory(store.Conventions.TopologyCacheLocation);
            }
        }
    }

    public class Test_With_PreInitialize_DisableTopologyCache_Equals_False : Abstract_RavenDB_17980
    {
        protected override void PreInitialize(IDocumentStore documentStore)
        {
            base.PreInitialize(documentStore);
            documentStore.Conventions.DisableTopologyCache = false;
        }

        [Fact]
        public void Creating_Topology_Files_For_TestDrivers()
        {
            using (var store = GetDocumentStore())
            {
                Assert.False(store.Conventions.DisableTopologyCache);

                using (var session = store.OpenSession())
                {
                    session.Store(new User
                    {
                        Name = "Yonatan",
                    });
                    session.SaveChanges();
                }

                var numberOfDatabaseTopologyFiles = Directory.GetFiles(store.Conventions.TopologyCacheLocation, "*.raven-database-topology").Length;
                Assert.False(numberOfDatabaseTopologyFiles == 0, $"number of files: {numberOfDatabaseTopologyFiles}");

                IOExtensions.DeleteDirectory(store.Conventions.TopologyCacheLocation);
            }
        }
    }
}

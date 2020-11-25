using System;
using System.IO;
using System.Linq;
using FastTests;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Operations;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_11825 : RavenTestBase
    {
        public RavenDB_11825(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void TopologyCacheLocationIsSetToAppContextBaseDirectoryByDefault()
        {
            Assert.Equal(AppContext.BaseDirectory, DocumentConventions.Default.TopologyCacheLocation);

            var conventions = new DocumentConventions();
            Assert.Equal(AppContext.BaseDirectory, conventions.TopologyCacheLocation);
        }

        [Fact]
        public void CanChangeTopologyCacheLocation()
        {
            var cacheLocation = NewDataPath(forceCreateDir: true);
            var cacheLocationDirectory = new DirectoryInfo(cacheLocation);

            Assert.True(cacheLocationDirectory.Exists);
            Assert.Equal(0, cacheLocationDirectory.GetFiles().Length);

            using (var store = GetDocumentStore(new Options
            {
                ModifyDocumentStore = s =>
                {
                    s.Conventions.DisableTopologyCache = false;
                    s.Conventions.TopologyCacheLocation = cacheLocation;
                }
            }))
            {
                store.Maintenance.Send(new GetStatisticsOperation()); // do something

                Assert.True(cacheLocationDirectory.Exists);

                var files = cacheLocationDirectory.GetFiles();
                Assert.True(files.Length > 0);
                Assert.Contains(".raven-database-topology", files.Select(x => x.Extension));
                Assert.Contains(".raven-cluster-topology", files.Select(x => x.Extension));
            }
        }
    }
}

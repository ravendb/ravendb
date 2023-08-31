using System.IO;
using System.IO.Compression;
using System.Threading;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Exceptions.Database;
using Raven.Server.Config.Settings;
using Raven.Tests.Core.Utils.Entities;
using Xunit;
using Xunit.Abstractions;


namespace SlowTests.Issues
{
    public class RavenDB_19519 : RavenTestBase
    {
        public RavenDB_19519(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void SchemaUpgradeAddedToInitLog()
        {
            var folder = NewDataPath(forceCreateDir: true);
            DoNotReuseServer();

            var zipPath = new PathSetting("SchemaUpgrade/Issues/SystemVersion/Identities_CompareExchange_RavenData_from12.zip");
            Assert.True(File.Exists(zipPath.FullPath));

            ZipFile.ExtractToDirectory(zipPath.FullPath, folder);
            using (var server = GetNewServer(new ServerCreationOptions
                   {
                       DeletePrevious = false,
                       RunInMemory = false,
                       DataDirectory = folder,
                       RegisterForDisposal = false,
                   }))
            {
                server.Configuration.Server.MaxTimeForTaskToWaitForDatabaseToLoad = new TimeSetting(5, TimeUnit.Seconds);

                var sm = new SemaphoreSlim(initialCount: 0);

                server.ServerStore.DatabasesLandlord.ForTestingPurposesOnly().AfterDatabaseInitialize = () =>
                {
                    sm.Wait();
                };

                try
                {
                    using (var store = new DocumentStore
                           {
                               Urls = new[] { server.WebUrl },
                               Database = "a"
                           })
                    {
                        store.Initialize();

                        using (var session = store.OpenSession())
                        {
                            var exception = Assert.Throws<DatabaseLoadTimeoutException>(() => session.Store(new User { Name = "Foo" }));
                            Assert.True(exception.Message.Contains("schema upgrade"));
                        }
                    }
                }
                finally
                {
                    sm.Release(2); // we have 2 databases
                }
            }
        }
    }
}

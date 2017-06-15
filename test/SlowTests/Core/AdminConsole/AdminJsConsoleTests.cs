using System.Threading.Tasks;
using FastTests;
using Raven.Server.Documents.Patch;
using Xunit;

namespace SlowTests.Core.AdminConsole
{
    public class AdminJsConsoleTests : RavenTestBase
    {
        [Fact]
        public async Task CanGetSettings()
        {
            using (var store = GetDocumentStore())
            {
                var database = await Server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(store.Database);
                
                var result = new AdminJsConsole(database).ApplyScript(new AdminJsScript
                {
                    Script = @"
                                return { 
                                    DatabaseName: database.Name,
                                    RunInMemory: database.Configuration.Core.RunInMemory,
                                    MaxConcurrentFlushes: database.Configuration.Storage.MaxConcurrentFlushes
                                };
                             "
                });

                Assert.NotNull(result);
                Assert.Equal(database.Name ,result["DatabaseName"]);
                Assert.Equal(true, result["RunInMemory"]);
                Assert.Equal(10L, result["MaxConcurrentFlushes"]);
            }
        }

        [Fact]
        public async Task CanModifyConfigurationOnTheFly()
        {
            using (var store = GetDocumentStore())
            {
                var database = await Server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(store.Database);
                var configuration = database.Configuration;

                Assert.True(configuration.Core.ThrowIfAnyIndexOrTransformerCouldNotBeOpened);
                Assert.False(configuration.Patching.AllowScriptsToAdjustNumberOfSteps);
                Assert.Null(configuration.Queries.MaxClauseCount);
                Assert.Equal(10, configuration.Storage.MaxConcurrentFlushes);

                new AdminJsConsole(database).ApplyScript(new AdminJsScript
                {
                    Script = @"
                                database.Configuration.Core.ThrowIfAnyIndexOrTransformerCouldNotBeOpened = false;
                                database.Configuration.Patching.AllowScriptsToAdjustNumberOfSteps = true;
                                database.Configuration.Queries.MaxClauseCount = 2048;
                                database.Configuration.Storage.MaxConcurrentFlushes = 40;
                             "
                });

                Assert.False(configuration.Core.ThrowIfAnyIndexOrTransformerCouldNotBeOpened);
                Assert.True(configuration.Patching.AllowScriptsToAdjustNumberOfSteps);
                Assert.Equal(2048, database.Configuration.Queries.MaxClauseCount);
                Assert.Equal(40, database.Configuration.Storage.MaxConcurrentFlushes);
            }
        }

        [Fact]
        public void CanGetServerSettings()
        {
            var result = new AdminJsConsole(Server).ApplyServerScript(new AdminJsScript
            {
                Script = @"
                            return { 
                                AllowScriptsToAdjustNumberOfSteps: server.Configuration.Patching.AllowScriptsToAdjustNumberOfSteps,
                                MaxConcurrentFlushes: server.Configuration.Storage.MaxConcurrentFlushes
                            };"
            });

            Assert.NotNull(result);
            Assert.Equal(false, result["AllowScriptsToAdjustNumberOfSteps"]);
            Assert.Equal(10L, result["MaxConcurrentFlushes"]);           
        }

        [Fact]
        public void CanModifyServerConfigurationOnTheFly()
        {
            var configuration = Server.Configuration;

            Assert.False(configuration.Core.ThrowIfAnyIndexOrTransformerCouldNotBeOpened);
            Assert.False(configuration.Patching.AllowScriptsToAdjustNumberOfSteps);
            Assert.Null(configuration.Queries.MaxClauseCount);
            Assert.Equal(10, configuration.Storage.MaxConcurrentFlushes);

            new AdminJsConsole(Server).ApplyServerScript(new AdminJsScript
            {
                Script = @"
                            server.Configuration.Core.ThrowIfAnyIndexOrTransformerCouldNotBeOpened = true;
                            server.Configuration.Patching.AllowScriptsToAdjustNumberOfSteps = true;
                            server.Configuration.Queries.MaxClauseCount = 2048;
                            server.Configuration.Storage.MaxConcurrentFlushes = 40;
                            "
            });

            Assert.True(configuration.Core.ThrowIfAnyIndexOrTransformerCouldNotBeOpened);
            Assert.True(configuration.Patching.AllowScriptsToAdjustNumberOfSteps);
            Assert.Equal(2048, configuration.Queries.MaxClauseCount);
            Assert.Equal(40, configuration.Storage.MaxConcurrentFlushes);            
        }
    }
}

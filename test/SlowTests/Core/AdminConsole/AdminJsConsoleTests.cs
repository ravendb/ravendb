using System;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Extensions;
using Raven.Server.Config.Settings;
using Raven.Server.Documents.Patch;
using Raven.Server.Utils.Metrics;
using Sparrow.Json.Parsing;
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
                var database = await GetDocumentDatabaseInstanceFor(store);
                
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
                var djv = result as DynamicJsonValue;
                Assert.NotNull(djv);
                Assert.Equal(database.Name , djv["DatabaseName"]);
                Assert.Equal(true, djv["RunInMemory"]);
                Assert.Equal(10L, djv["MaxConcurrentFlushes"]);
            }
        }

        [Fact]
        public async Task CanGetResultAsDateObject()
        {
            using (var store = GetDocumentStore())
            {
                var database = await GetDocumentDatabaseInstanceFor(store);
                var startTime = database.StartTime;

                var result = new AdminJsConsole(database).ApplyScript(new AdminJsScript
                {
                    Script = @"
                                return database.StartTime;
                             "
                });

                Assert.NotNull(result);
                Assert.IsType<DateTime>(result);
                Assert.Equal(startTime.ToInvariantString(), result.ToInvariantString());
            }
        }

        [Fact]
        public async Task CanGetResultAsPrimitiveObject()
        {
            using (var store = GetDocumentStore())
            {
                var database = await GetDocumentDatabaseInstanceFor(store);
                var maxConcurrentFlushes = (long)database.Configuration.Storage.MaxConcurrentFlushes;

                var result = new AdminJsConsole(database).ApplyScript(new AdminJsScript
                {
                    Script = @"
                                return database.Configuration.Storage.MaxConcurrentFlushes
                             "
                });

                Assert.NotNull(result);
                Assert.IsType<long>(result);
                Assert.Equal(maxConcurrentFlushes, result);

                var allowScriptsToAdjustNumberOfSteps = database.Configuration.Indexing.MapTimeout;

                var result2 = new AdminJsConsole(database).ApplyScript(new AdminJsScript
                {
                    Script = @"
                                return database.Configuration.Indexing.MapTimeout
                             "
                });

                Assert.NotNull(result2);
                Assert.IsType<TimeSetting>(result2);
                Assert.Same(allowScriptsToAdjustNumberOfSteps, result2);

                var serverUrl = database.Configuration.Core.ServerUrl;

                var result3 = new AdminJsConsole(database).ApplyScript(new AdminJsScript
                {
                    Script = @"
                                return database.Configuration.Core.ServerUrl
                             "
                });

                Assert.NotNull(result3);
                Assert.IsType<string>(result3);
                Assert.Equal(serverUrl, result3);
            }
        }

        [Fact]
        public async Task CanGetResultAsComplexObject()
        {
            using (var store = GetDocumentStore())
            {
                var database = await GetDocumentDatabaseInstanceFor(store);
                var requestsMeter = database.Metrics.RequestsMeter;

                using (var session = store.OpenSession())
                {                    
                    for (var i = 0; i < 10; i++)
                    {
                        session.Store(new object());
                    }
                    session.SaveChanges();
                }

                var result = new AdminJsConsole(database).ApplyScript(new AdminJsScript
                {
                    Script = @"
                                return database.Metrics.RequestsMeter
                             "
                });

                var resultAsMeterMetric = result as MeterMetric;
                Assert.NotNull(resultAsMeterMetric);
                Assert.Equal(requestsMeter.Count, resultAsMeterMetric.Count);
                Assert.Equal(requestsMeter.FifteenMinuteRate, resultAsMeterMetric.FifteenMinuteRate);
                Assert.Equal(requestsMeter.FiveMinuteRate, resultAsMeterMetric.FiveMinuteRate);
                Assert.Equal(requestsMeter.FiveSecondRate, resultAsMeterMetric.FiveSecondRate);
                Assert.Equal(requestsMeter.OneMinuteRate, resultAsMeterMetric.OneMinuteRate);
                Assert.Equal(requestsMeter.OneSecondRate, resultAsMeterMetric.OneSecondRate);
            }
        }

        [Fact]
        public async Task CanModifyConfigurationOnTheFly()
        {
            using (var store = GetDocumentStore())
            {
                var database = await GetDocumentDatabaseInstanceFor(store);
                var configuration = database.Configuration;

                Assert.True(configuration.Core.ThrowIfAnyIndexOrTransformerCouldNotBeOpened);
                Assert.Null(configuration.Queries.MaxClauseCount);
                Assert.Equal(10, configuration.Storage.MaxConcurrentFlushes);

                new AdminJsConsole(database).ApplyScript(new AdminJsScript
                {
                    Script = @"
                                database.Configuration.Core.ThrowIfAnyIndexOrTransformerCouldNotBeOpened = false;
                                database.Configuration.Queries.MaxClauseCount = 2048;
                                database.Configuration.Storage.MaxConcurrentFlushes = 40;
                             "
                });

                Assert.False(configuration.Core.ThrowIfAnyIndexOrTransformerCouldNotBeOpened);
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
            var djv = result as DynamicJsonValue;

            Assert.NotNull(djv);
            Assert.Equal(false, djv["AllowScriptsToAdjustNumberOfSteps"]);
            Assert.Equal(10L, djv["MaxConcurrentFlushes"]);           
        }

        [Fact]
        public void CanModifyServerConfigurationOnTheFly()
        {
            var configuration = Server.Configuration;

            Assert.False(configuration.Core.ThrowIfAnyIndexOrTransformerCouldNotBeOpened);
            Assert.Null(configuration.Queries.MaxClauseCount);
            Assert.Equal(10, configuration.Storage.MaxConcurrentFlushes);

            new AdminJsConsole(Server).ApplyServerScript(new AdminJsScript
            {
                Script = @"
                            server.Configuration.Core.ThrowIfAnyIndexOrTransformerCouldNotBeOpened = true;
                            server.Configuration.Queries.MaxClauseCount = 2048;
                            server.Configuration.Storage.MaxConcurrentFlushes = 40;
                            "
            });

            Assert.True(configuration.Core.ThrowIfAnyIndexOrTransformerCouldNotBeOpened);
            Assert.Equal(2048, configuration.Queries.MaxClauseCount);
            Assert.Equal(40, configuration.Storage.MaxConcurrentFlushes);            
        }

        [Fact]
        public void CanReturnNullResult()
        {
            var result = new AdminJsConsole(Server).ApplyServerScript(new AdminJsScript
            {
                Script = @"return null"
            });

            Assert.Null(result);
        }
    }
}

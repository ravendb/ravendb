using System;
using System.Threading.Tasks;
using FastTests;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Raven.Client.Extensions;
using Raven.Server;
using Raven.Server.Config.Settings;
using Raven.Server.Documents;
using Raven.Server.Documents.Patch;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Core.AdminConsole
{
    public class AdminJsConsoleTests : RavenTestBase
    {
        public AdminJsConsoleTests(ITestOutputHelper output) : base(output)
        {
            DoNotReuseServer();
        }

        [Fact]
        public async Task CanGetSettings()
        {
            using (var store = GetDocumentStore())
            {
                var database = await Databases.GetDocumentDatabaseInstanceFor(store);

                var result = ExecuteScript(database, @"
                                return { 
                                    DatabaseName: database.Name,
                                    RunInMemory: database.Configuration.Core.RunInMemory,
                                    MaxConcurrentFlushes: database.Configuration.Storage.MaxConcurrentFlushes
                                };
                             ");

                Assert.Equal(database.Name , result["DatabaseName"]);
                Assert.Equal(true, result["RunInMemory"]);
                Assert.Equal(10L, result["MaxConcurrentFlushes"]);
            }
        }

        private JToken ExecuteScript(DocumentDatabase database, string script)
        {
            return ExecuteScript(Server, database, script);
        }

        internal static JToken ExecuteScript(RavenServer server, DocumentDatabase database, string script)
        {
            var result = new AdminJsConsole(server, database).ApplyScript(new AdminJsScript
            (
                script
            ));

            Assert.NotNull(result);
            var token = JsonConvert.DeserializeObject<JObject>(result).GetValue("Result");
            Assert.NotNull(token);
            return token;
        }

        [Fact]
        public async Task CanGetResultAsDateObject()
        {
            using (var store = GetDocumentStore())
            {
                var database = await Databases.GetDocumentDatabaseInstanceFor(store);
                var startTime = database.StartTime;

                var result = ExecuteScript(database, @"
                                return database.StartTime;
                             ");

                Assert.Equal(startTime.ToInvariantString(), result.ToInvariantString());
            }
        }

        [Fact]
        public async Task CanGetResultAsPrimitiveObject()
        {
            using (var store = GetDocumentStore())
            {
                var database = await Databases.GetDocumentDatabaseInstanceFor(store);
                var maxConcurrentFlushes = (long)database.Configuration.Storage.MaxConcurrentFlushes;

                var result = ExecuteScript(database, @"
                                return database.Configuration.Storage.MaxConcurrentFlushes
                             ");

                Assert.Equal(maxConcurrentFlushes, result.Value<long>());

                var allowScriptsToAdjustNumberOfSteps = database.Configuration.Indexing.MapTimeout;

                var result2 = ExecuteScript(database, @"
                                return database.Configuration.Indexing.MapTimeout
                             ");

                Assert.Equal(allowScriptsToAdjustNumberOfSteps, result2.ToObject<TimeSetting>());

                var serverUrl = database.Configuration.Core.ServerUrls;

                var result3 = ExecuteScript(database, @"
                                return database.Configuration.Core.ServerUrls[0]
                             ");

                Assert.Equal(serverUrl[0], result3.Value<string>());
            }
        }

        [Fact]
        public async Task CanGetResultAsComplexObject()
        {
            using (var store = GetDocumentStore())
            {
                var database = await Databases.GetDocumentDatabaseInstanceFor(store);
                var requestsMeter = database.Metrics.Requests.RequestsPerSec;

                using (var session = store.OpenSession())
                {                    
                    for (var i = 0; i < 10; i++)
                    {
                        session.Store(new object());
                    }
                    session.SaveChanges();
                }

                var result = ExecuteScript(database, @"
                                return database.Metrics.Requests.RequestsPerSec
                             "
                );
                // we can't verify the actual values, it is possible that there is latency
                // between the request executing and the comparison being made, so we just
                // verify the structure is valid

                result["Count"].Value<long>();
                result["FifteenMinuteRate"].Value<double>();
                result["FiveMinuteRate"].Value<double>();
                result["OneMinuteRate"].Value<double>();
                result["FiveSecondRate"].Value<double>();
                result["OneSecondRate"].Value<double>();
            }
        }

        [Fact]
        public async Task CanConvertAllJsonTypesToString()
        {
            using (var store = GetDocumentStore())
            {
                var database = await Databases.GetDocumentDatabaseInstanceFor(store);

                // Load the database and use it
                using (var session = store.OpenSession())
                {                    
                    for (var i = 0; i < 10; i++)
                    {
                        session.Store(new object());
                    }
                    session.SaveChanges();
                }

                ExecuteScript(database, @"
                                return server;
                             "
                );

                ExecuteScript(database, @"
                                return database;
                             "
                );
            }
        }

        [Fact]
        public async Task CanModifyConfigurationOnTheFly()
        {
            using (var store = GetDocumentStore())
            {
                var database = await Databases.GetDocumentDatabaseInstanceFor(store);
                var configuration = database.Configuration;

                Assert.True(configuration.Core.ThrowIfAnyIndexCannotBeOpened);
                Assert.Null(configuration.Queries.MaxClauseCount);
                Assert.Equal(10, configuration.Storage.MaxConcurrentFlushes);

                ExecuteScript(database, @"
                                database.Configuration.Core.ThrowIfAnyIndexCannotBeOpened = false;
                                database.Configuration.Queries.MaxClauseCount = 2048;
                                database.Configuration.Storage.MaxConcurrentFlushes = 40;
                             "
                );

                Assert.False(configuration.Core.ThrowIfAnyIndexCannotBeOpened);
                Assert.Equal(2048, database.Configuration.Queries.MaxClauseCount);
                Assert.Equal(40, database.Configuration.Storage.MaxConcurrentFlushes);
            }
        }

        [Fact]
        public void CanGetServerSettings()
        {
            var result = ExecuteScript(null, @"
                            return { 
                                AllowScriptsToAdjustNumberOfSteps: server.Configuration.Patching.AllowScriptsToAdjustNumberOfSteps,
                                MaxConcurrentFlushes: server.Configuration.Storage.MaxConcurrentFlushes
                            };"
            );
            Assert.Equal(JValue.CreateNull(), result["AllowScriptsToAdjustNumberOfSteps"]); // Test not exists property access
            Assert.Equal(10L, result["MaxConcurrentFlushes"]);
        }

        [Fact]
        public void CanGetServerStoreConfigs()
        {
            DoNotReuseServer();
            var result = ExecuteScript(null, @"
                            return { 
                                StrictMode: server.ServerStore.Configuration.Patching.StrictMode,
                                MaxConcurrentFlushes: server.ServerStore.Configuration.Storage.MaxConcurrentFlushes
                            };"
            );

            try
            {
                Assert.Equal(true, result["StrictMode"]);
                Assert.Equal(10, result["MaxConcurrentFlushes"]);
            }
            catch (Exception e)
            {
                throw new InvalidOperationException("Cannot understand " + result.ToString(Formatting.Indented), e);
            }
        }

        [Fact]
        public void CanModifyServerConfigurationOnTheFly()
        {
            var configuration = Server.Configuration;

            Assert.False(configuration.Core.ThrowIfAnyIndexCannotBeOpened);
            Assert.Null(configuration.Queries.MaxClauseCount);
            Assert.Equal(10, configuration.Storage.MaxConcurrentFlushes);

            ExecuteScript(null, @"
                            server.Configuration.Core.ThrowIfAnyIndexCannotBeOpened = true;
                            server.Configuration.Queries.MaxClauseCount = 2048;
                            server.Configuration.Storage.MaxConcurrentFlushes = 40;
                            "
            );

            Assert.True(configuration.Core.ThrowIfAnyIndexCannotBeOpened);
            Assert.Equal(2048, configuration.Queries.MaxClauseCount);
            Assert.Equal(40, configuration.Storage.MaxConcurrentFlushes);            
        }

        [Fact]
        public void CanReturnNullResult()
        {
            var result = ExecuteScript(null, "return null");
            Assert.Equal(JValue.CreateNull(), (JValue)result);
        }
    }
}

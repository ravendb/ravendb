using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using FastTests;
using Newtonsoft.Json;
using Newtonsoft.Json.Converters;
using Newtonsoft.Json.Linq;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Server.Documents.Handlers.Admin;
using SlowTests.Issues;
using Sparrow.Logging;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.SparrowTests
{
    public class LoggersTogglingTests : RavenTestBase
    {
        public LoggersTogglingTests(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public async Task LoggerToggling_WhenRequestAllLogger_ShouldGetLoggersWithCurrentState()
        {
            using var server = GetNewServer();
            server.Logger.SetLoggerMode(LogMode.Information);
            using var store = new DocumentStore {Urls = new[] {server.WebUrl}, Database = "Random"}.Initialize();
            var client = store.GetRequestExecutor().HttpClient;
            var response = await client.GetAsync(store.Urls.First() + "/admin/logging-toggling/loggers");
            Assert.Equal(HttpStatusCode.OK, response.StatusCode);

            var result = await response.Content.ReadAsStringAsync();
            var loggers = JsonConvert.DeserializeObject<JObject>(result);
            Assert.NotNull(loggers);
            Assert.Equal(nameof(LogMode.Information), loggers["Loggers"]["Server"]["LogMode"].Value<string>());
        }

        [Fact]
        public async Task LoggerToggling_WhenRequestSetServerLoggerToInfo_ShouldServerLoggerBeSetToInfo()
        {
            using var server = GetNewServer();
            using var store = GetDocumentStore(new Options {Server = server});

            var client = store.GetRequestExecutor().HttpClient;

            var configuration = new AdminLogsHandler.SwitchLoggerConfiguration
            {
                Loggers = new Dictionary<string, LogMode>
                {
                    {"Server", LogMode.Information},
                    {"Server.Databases", LogMode.None}
                }
            };
            var data = new StringContent(JsonConvert.SerializeObject(new {Configuration = configuration}, new StringEnumConverter()), Encoding.UTF8, "application/json");
            var setResponse = await client.PostAsync(store.Urls.First() + "/admin/logging-toggling/configuration", data);
            Assert.Equal(HttpStatusCode.NoContent, setResponse.StatusCode);
            Assert.Equal(LogMode.Information, server.Logger.GetLogMode());
            Assert.Equal(LogMode.None, server.DatabasesLogger.GetLogMode());
            
            var getResponse = await client.GetAsync(store.Urls.First() + "/admin/logging-toggling/configuration");
            Assert.Equal(HttpStatusCode.OK, getResponse.StatusCode);
            var result = await getResponse.Content.ReadAsStringAsync();
            configuration = JsonConvert.DeserializeObject<AdminLogsHandler.SwitchLoggerConfiguration>(result);

            Assert.Contains(new KeyValuePair<string, LogMode>("Server", LogMode.Information), configuration.Loggers);
            Assert.Contains(new KeyValuePair<string, LogMode>("Server.Databases", LogMode.None), configuration.Loggers);
        }

        [Fact]
        public async Task LoggerToggling_WhenAddAndRemoveDatabaseAndIndex_ShouldContainsEquivalentSwitches()
        {
            using var server = GetNewServer();
            Assert.False(server.Logger.IsModeOverrode);

            Assert.True(server.Logger.Loggers.TryGet("Databases", out var databasesSwitchLogger));
            Assert.False(databasesSwitchLogger.IsModeOverrode);

            string databaseName;
            using (var store = GetDocumentStore(new Options {Server = server}))
            {
                databaseName = store.Database;
                var index = new SampleIndex();
                await index.ExecuteAsync(store);

                Assert.True(databasesSwitchLogger.Loggers.TryGet(store.Database, out var databaseSwitchLogger));
                Assert.False(databaseSwitchLogger.IsModeOverrode);

                Assert.True(databaseSwitchLogger.Loggers.TryGet("Indexes", out var indexesSwitchLogger));
                Assert.False(indexesSwitchLogger.IsModeOverrode);

                Assert.True(indexesSwitchLogger.Loggers.TryGet(index.IndexName, out var indexSwitchLogger));
                Assert.False(indexSwitchLogger.IsModeOverrode);

                await store.Maintenance.SendAsync(new DeleteIndexOperation(index.IndexName));
                Assert.False(indexesSwitchLogger.Loggers.TryGet(index.IndexName, out _));
            }

            Assert.False(databasesSwitchLogger.Loggers.TryGet(databaseName, out _));
        }

        [Fact]
        public void LoggerToggling_WhenApplyConfiguration_ShouldSetAllDecentConfiguredSwitches()
        {
            var name = GetTestName();
            var path = NewDataPath(forceCreateDir: true);
            path = Path.Combine(path, Guid.NewGuid().ToString());
            Directory.CreateDirectory(path);

            var loggingSource = new LoggingSource(
                LogMode.None,
                path,
                "LoggingSource" + name,
                TimeSpan.MaxValue,
                long.MaxValue,
                false);

            try
            {
                var aLogger = new SwitchLogger(loggingSource, "A");
                var bLogger = aLogger.GetSubSwitchLogger("B");
                var cLogger = bLogger.GetSubSwitchLogger("C");

                var configuration = new AdminLogsHandler.SwitchLoggerConfiguration
                {
                    Loggers = new Dictionary<string, LogMode>
                    {
                        {"A.B", LogMode.Information}
                    }
                };

                Assert.False(aLogger.IsModeOverrode);
                Assert.False(aLogger.IsOperationsEnabled || aLogger.IsInfoEnabled);
                Assert.False(cLogger.IsModeOverrode);
                Assert.False(aLogger.IsOperationsEnabled || aLogger.IsInfoEnabled);

                Assert.False(bLogger.IsModeOverrode);
                Assert.False(aLogger.IsOperationsEnabled || aLogger.IsInfoEnabled);

                var (configurationPath, mode) = configuration.Loggers.First();
                SwitchLoggerConfigurationHelper.ApplyConfiguration(aLogger, SwitchLoggerConfigurationHelper.IterateSwitches(configurationPath).Skip(1), mode);

                Assert.False(aLogger.IsModeOverrode);
                Assert.False(aLogger.IsOperationsEnabled || aLogger.IsInfoEnabled);
                
                Assert.True(bLogger.IsModeOverrode);
                Assert.True(bLogger.IsInfoEnabled);
                Assert.True(cLogger.IsModeOverrode);
                Assert.True(cLogger.IsOperationsEnabled && cLogger.IsInfoEnabled);
            }
            finally
            {
                loggingSource.EndLogging();
            }
        }

        [Fact]
        public void LoggerToggling_WhenSetLoggerWithDotInTheName_ShouldSet()
        {
            var name = GetTestName();
            var path = NewDataPath(forceCreateDir: true);
            path = Path.Combine(path, Guid.NewGuid().ToString());
            Directory.CreateDirectory(path);

            var loggingSource = new LoggingSource(
                LogMode.None,
                path,
                "LoggingSource" + name,
                TimeSpan.MaxValue,
                long.MaxValue,
                false);

            try
            {
                var aLogger = new SwitchLogger(loggingSource, "A");
                var bLogger = aLogger.GetSubSwitchLogger("B");
                var cLogger = bLogger.GetSubSwitchLogger("C.C1");

                var configuration = new AdminLogsHandler.SwitchLoggerConfiguration
                {
                    Loggers = new Dictionary<string, LogMode>
                    {
                        {@"A.B.C\.C1", LogMode.Information}
                    }
                };

                var (configurationPath, mode) = configuration.Loggers.First();
                SwitchLoggerConfigurationHelper.ApplyConfiguration(aLogger, SwitchLoggerConfigurationHelper.IterateSwitches(configurationPath).Skip(1), mode);

                Assert.True(cLogger.IsModeOverrode);
                Assert.True(cLogger.IsOperationsEnabled && cLogger.IsInfoEnabled);
            }
            finally
            {
                loggingSource.EndLogging();
            }
        }

        [Fact]
        public void LoggerToggling_WhenCreateNewFromSetLogger_ShouldSetTheNew()
        {
            var name = GetTestName();
            var path = NewDataPath(forceCreateDir: true);
            path = Path.Combine(path, Guid.NewGuid().ToString());
            Directory.CreateDirectory(path);

            var loggingSource = new LoggingSource(
                LogMode.None,
                path,
                "LoggingSource" + name,
                TimeSpan.MaxValue,
                long.MaxValue,
                false);

            try
            {
                var aLogger = new SwitchLogger(loggingSource, "A");
                aLogger.SetLoggerMode(LogMode.Information);
                var bLogger = aLogger.GetSubSwitchLogger("B");

                Assert.True(bLogger.IsModeOverrode);
                Assert.True(bLogger.IsInfoEnabled);
            }
            finally
            {
                loggingSource.EndLogging();
            }
        }

        [Fact]
        public void LoggerToggling_WhenReset_ShouldResetSwitches()
        {
            var name = GetTestName();
            var path = NewDataPath(forceCreateDir: true);
            path = Path.Combine(path, Guid.NewGuid().ToString());
            Directory.CreateDirectory(path);

            var loggingSource = new LoggingSource(
                LogMode.None,
                path,
                "LoggingSource" + name,
                TimeSpan.MaxValue,
                long.MaxValue,
                false);

            try
            {
                var aLogger = new SwitchLogger(loggingSource, "A");
                aLogger.SetLoggerMode(LogMode.Information);
                var bLogger = aLogger.GetSubSwitchLogger("B");
                var cLogger = bLogger.GetSubSwitchLogger("C");

                Assert.True(aLogger.IsModeOverrode);
                Assert.True(aLogger.IsInfoEnabled);
                Assert.True(bLogger.IsModeOverrode);
                Assert.True(bLogger.IsInfoEnabled);
                Assert.True(cLogger.IsModeOverrode);
                Assert.True(cLogger.IsInfoEnabled);

                aLogger.ResetRecursively();

                Assert.False(aLogger.IsModeOverrode);
                Assert.Equal(LogMode.None, aLogger.GetLogMode());
                Assert.False(bLogger.IsModeOverrode);
                Assert.Equal(LogMode.None, bLogger.GetLogMode());
                Assert.False(cLogger.IsModeOverrode);
                Assert.Equal(LogMode.None, cLogger.GetLogMode());
            }
            finally
            {
                loggingSource.EndLogging();
            }
        }

        [Fact]
        public async Task LoggerToggling_WhenSwitchLoggerIsDifferentFromLoggingSource_ShouldUseSwitchLoggerConfiguration()
        {
            var name = GetTestName();
            var path = NewDataPath(suffix: "Logs", forceCreateDir: true);

            var loggingSource = new LoggingSource(
                LogMode.Information,
                path,
                "LoggingSource" + name,
                TimeSpan.MaxValue,
                long.MaxValue,
                false);

            try
            {
                var logFile = await AssertWaitForNotNullAsync(async () => Directory.GetFiles(path, "*.log").FirstOrDefault());

                var logger = new SwitchLogger(loggingSource, "A");

                var shouldContainList = new List<string>();
                var shouldNotContainList = new List<string>();

                //Without listeners
                {
                    await AddLog($"Without listeners 1 - loggingSource.LogMode:{loggingSource.LogMode} logger.GetLogMode:{logger.GetLogMode()}", true);

                    logger.SetLoggerMode(LogMode.None);
                    await AddLog($"Without listeners 2 - loggingSource.LogMode:{loggingSource.LogMode} logger.GetLogMode:{logger.GetLogMode()}", false);

                    logger.SetLoggerMode(LogMode.Operations);
                    await AddLog($"Without listeners 3 - loggingSource.LogMode:{loggingSource.LogMode} logger.GetLogMode:{logger.GetLogMode()}", false);

                    logger.SetLoggerMode(LogMode.Information);
                    await AddLog($"Without listeners 4 - loggingSource.LogMode:{loggingSource.LogMode} logger.GetLogMode:{logger.GetLogMode()}", true);
                }

                //With listeners
                using (var socket = new LogTestsHelper.DummyWebSocket())
                {
                    var context = new LoggingSource.WebSocketContext();
                    _ = loggingSource.Register(socket, context, CancellationToken.None);

                    loggingSource.SetupLogMode(LogMode.Information);
                    await AddLog($"With listeners 1 - loggingSource.LogMode:{loggingSource.LogMode} logger.GetLogMode:{logger.GetLogMode()}", true);

                    logger.SetLoggerMode(LogMode.None);
                    await AddLog($"With listeners 2 - loggingSource.LogMode:{loggingSource.LogMode} logger.GetLogMode:{logger.GetLogMode()}", false);

                    logger.SetLoggerMode(LogMode.Operations);
                    await AddLog($"With listeners 3 - loggingSource.LogMode:{loggingSource.LogMode} logger.GetLogMode:{logger.GetLogMode()}", false);

                    logger.SetLoggerMode(LogMode.Information);
                    await AddLog($"With listeners 4 - loggingSource.LogMode:{loggingSource.LogMode} logger.GetLogMode:{logger.GetLogMode()}", true);
                }

                //To be sure all logs where written to file
                var lastLineLogger = loggingSource.GetLogger("Test", "Test");
                await lastLineLogger.InfoWithWait("");

                var actualLogContent = await File.ReadAllTextAsync(logFile);
                foreach (var msg in shouldContainList)
                {
                    Assert.Contains(msg, actualLogContent);
                }

                foreach (var msg in shouldNotContainList)
                {
                    Assert.DoesNotContain(msg, actualLogContent);
                }

                async Task AddLog(string msg, bool shouldContain)
                {
                    var list = shouldContain ? shouldContainList : shouldNotContainList;

                    var subLogger = logger.GetLogger("test");
                    list.Add(msg);
                    if (logger.IsInfoEnabled)
                        logger.Info(msg);

                    var subLoggerMsg = $"subLogger {msg}";
                    list.Add(subLoggerMsg);
                    if (subLogger.IsInfoEnabled)
                        subLogger.Info(subLoggerMsg);

                    var waitLogger = loggingSource.GetLogger("", "");
                    await waitLogger.OperationsWithWait("").WaitAsync(TimeSpan.FromSeconds(5));
                }
            }
            finally
            {
                loggingSource.EndLogging();
            }
        }

        private static string GetTestName([CallerMemberName] string memberName = "") => memberName;
    }
}

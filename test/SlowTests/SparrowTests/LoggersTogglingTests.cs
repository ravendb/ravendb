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
using Newtonsoft.Json.Linq;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Server.Json;
using SlowTests.Issues;
using Sparrow.Json;
using Sparrow.Json.Parsing;
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
            var response = await client.GetAsync(store.Urls.First() + "/admin/loggers");
            Assert.Equal(HttpStatusCode.OK, response.StatusCode);

            var result = await response.Content.ReadAsStringAsync();
            var loggers = JsonConvert.DeserializeObject<JObject>(result);
            Assert.NotNull(loggers);
            Assert.True(loggers["Loggers"]["Server"][nameof(Logger.IsInfoEnabled)].Value<bool>());

            //We want the client to be able to get the result of "/admin/loggers" configure it and send it back to "/admin/loggers/set"
            //We don't want to change generic loggers while they are common to all test
            loggers["Loggers"].Value<JObject>().Property("Generic").Remove();
            var data = new StringContent(JsonConvert.SerializeObject(new {Configuration = loggers}), Encoding.UTF8, "application/json");
            var setResponse = await client.PostAsync(store.Urls.First() + "/admin/loggers/set", data);
            Assert.Equal(HttpStatusCode.OK, setResponse.StatusCode);
        }

        [Fact]
        public async Task LoggerToggling_WhenRequestSetServerLoggerToInfo_ShouldServerLoggerBeSetToInfo()
        {
            using var server = GetNewServer();
            using var store = GetDocumentStore(new Options {Server = server});

            var client = store.GetRequestExecutor().HttpClient;

            var configuration = new {Loggers = new {Server = new {LogMode = LogMode.Information}}};

            var data = new StringContent(JsonConvert.SerializeObject(new {Configuration = configuration}), Encoding.UTF8, "application/json");
            var setResponse = await client.PostAsync(store.Urls.First() + "/admin/loggers/set", data);

            Assert.Equal(HttpStatusCode.OK, setResponse.StatusCode);
            Assert.Equal(LogMode.Information, server.Logger.GetLogMode());
        }

        [Fact]
        public async Task LoggerToggling_WhenAddAndRemoveDatabaseAndIndex_ShouldContainsEquivalentSwitches()
        {
            using var server = GetNewServer();
            Assert.True(server.Logger.IsReset());

            Assert.True(server.Logger.Loggers.TryGet("Databases", out var databasesSwitchLogger));
            Assert.True(databasesSwitchLogger.IsReset());

            string databaseName;
            using (var store = GetDocumentStore(new Options {Server = server}))
            {
                databaseName = store.Database;
                var index = new SampleIndex();
                await index.ExecuteAsync(store);

                Assert.True(databasesSwitchLogger.Loggers.TryGet(store.Database, out var databaseSwitchLogger));
                Assert.True(databaseSwitchLogger.IsReset());

                Assert.True(databaseSwitchLogger.Loggers.TryGet("Indexes", out var indexesSwitchLogger));
                Assert.True(indexesSwitchLogger.IsReset());

                Assert.True(indexesSwitchLogger.Loggers.TryGet(index.IndexName, out var indexSwitchLogger));
                Assert.True(indexSwitchLogger.IsReset());

                await store.Maintenance.SendAsync(new DeleteIndexOperation(index.IndexName));
                Assert.False(indexesSwitchLogger.Loggers.TryGet(index.IndexName, out _));
            }

            Assert.False(databasesSwitchLogger.Loggers.TryGet(databaseName, out _));
        }

        [Fact]
        public void LoggerToggling_WhenApplyConfiguration_ShouldSetOnlyConfiguredSwitch()
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

                var configuration1 = new DynamicJsonValue {["Loggers"] = new DynamicJsonValue {["B"] = new DynamicJsonValue {["LogMode"] = LogMode.Information}}};
                var contest = JsonOperationContext.ShortTermSingleUse();
                var blittable = contest.ReadObject(configuration1, "JsonDeserializationServer");
                var configuration = JsonDeserializationServer.SwitchLoggerConfiguration(blittable);


                Assert.True(aLogger.IsReset());
                Assert.False(aLogger.IsOperationsEnabled || aLogger.IsInfoEnabled);
                Assert.True(cLogger.IsReset());
                Assert.False(aLogger.IsOperationsEnabled || aLogger.IsInfoEnabled);

                Assert.True(bLogger.IsReset());
                Assert.False(aLogger.IsOperationsEnabled || aLogger.IsInfoEnabled);

                configuration.Apply(aLogger);

                Assert.True(aLogger.IsReset());
                Assert.False(aLogger.IsOperationsEnabled || aLogger.IsInfoEnabled);
                Assert.True(cLogger.IsReset());
                Assert.False(aLogger.IsOperationsEnabled || aLogger.IsInfoEnabled);

                Assert.False(bLogger.IsReset());
                Assert.True(bLogger.IsInfoEnabled);
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

                Assert.False(bLogger.IsReset());
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

                Assert.False(aLogger.IsReset());
                Assert.True(aLogger.IsInfoEnabled);
                Assert.False(bLogger.IsReset());
                Assert.True(bLogger.IsInfoEnabled);
                Assert.False(cLogger.IsReset());
                Assert.True(cLogger.IsInfoEnabled);

                aLogger.Reset(true);

                Assert.True(aLogger.IsReset());
                Assert.Equal(LogMode.None, aLogger.GetLogMode());
                Assert.True(bLogger.IsReset());
                Assert.Equal(LogMode.None, bLogger.GetLogMode());
                Assert.True(cLogger.IsReset());
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

using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Net.WebSockets;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Exceptions;
using Raven.Client.Extensions;
using Raven.Server.Config;
using Sparrow;
using Sparrow.Extensions;
using Sparrow.Logging;
using Tests.Infrastructure;
using Voron.Global;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.SparrowTests
{
    public class LoggingSourceTests : RavenTestBase
    {

        public LoggingSourceTests(ITestOutputHelper output) : base(output)
        {
        }

        [Theory]
        [InlineData(true)]
        [InlineData(false)]
        public async Task LoggingSource_WhileRetentionByTimeInHours_ShouldKeepRetentionPolicy(bool compressing)
        {
            const int fileSize = Constants.Size.Kilobyte;

            var name = GetTestName();
            var path = NewDataPath(forceCreateDir: true);
            path = Path.Combine(path, Guid.NewGuid().ToString());
            Directory.CreateDirectory(path);
            var retentionTimeConfiguration = TimeSpan.FromHours(3);

            var now = DateTime.Now;
            var retentionTime = now - retentionTimeConfiguration;
            var toCheckLogFiles = new List<(string fileName, bool shouldExist)>();

            const int artificialLogsCount = 9;
            for (int i = 0; i < artificialLogsCount; i++)
            {
                var lastModified = now - TimeSpan.FromHours(i);
                var fileName = Path.Combine(path, $"{LoggingSource.LogInfo.DateToLogFormat(lastModified)}.00{artificialLogsCount - i}.log");
                toCheckLogFiles.Add((fileName, lastModified > retentionTime));
                await using (File.Create(fileName))
                { }
                File.SetLastWriteTime(fileName, lastModified);
            }

            const long retentionSize = long.MaxValue;
            var loggingSource = new LoggingSource(
                LogMode.Information,
                path,
                "LoggingSource" + name,
                retentionTimeConfiguration,
                retentionSize,
                compressing)
            { MaxFileSizeInBytes = fileSize };
            //This is just to make sure the MaxFileSizeInBytes is get action for the first file
            loggingSource.SetupLogMode(LogMode.Operations, path, retentionTimeConfiguration, retentionSize, compressing);

            var logger = new Logger(loggingSource, "Source" + name, "Logger" + name);

            for (var j = 0; j < 50; j++)
            {
                for (var i = 0; i < 5; i++)
                {
                    await logger.OperationsWithWait("Some message");
                }
                Thread.Sleep(10);
            }

            string[] afterEndFiles = null;
            await WaitForValueAsync(async () =>
            {
                for (var i = 0; i < 10; i++)
                {
                    await logger.OperationsWithWait("Some message");
                }
                afterEndFiles = Directory.GetFiles(path);
                return DoesContainFilesThatShouldNotBeFound(afterEndFiles, toCheckLogFiles, compressing);
            }, false, 10_000, 1_000);

            loggingSource.EndLogging();

            AssertNoFileMissing(afterEndFiles);

            try
            {
                Assert.All(toCheckLogFiles, toCheck =>
                {
                    (string fileName, bool shouldExist) = toCheck;
                    var found = Directory.GetFiles(path, Path.GetFileName(fileName) + '*');
                    if (shouldExist)
                    {
                        Assert.True(found.Any(), $"The log file \"{fileName}\" should be exist");
                    }
                    else
                    {
                        Assert.False(found.Any(), CreateErrorMessage());
                        string CreateErrorMessage()
                        {
                            var messages = found.Select(f =>
                            {
                                var fileInfo = new FileInfo(found.First());
                                return fileInfo.Exists ? $"\"{fileInfo.Name}\" last modified {fileInfo.LastWriteTime}" : string.Empty;
                            });
                            return $"The log files {string.Join(", ", messages)} should not be exist. retentionTime({retentionTime})";
                        }
                    }
                });
            }
            catch (Exception e)
            {
                throw new InvalidOperationException($"Logs after end - {JustFileNamesAsString(afterEndFiles)}", e);
            }
        }

        private static bool DoesContainFilesThatShouldNotBeFound(string[] exitFiles, List<(string fileName, bool shouldExist)> toCheckLogFiles, bool compressing)
        {
            return exitFiles.Any(f => toCheckLogFiles
                .Any(tc => tc.shouldExist == false && (tc.fileName.Equals(f) || compressing && (tc.fileName + ".gz").Equals(f))));
        }

        [Theory]
        [InlineData("log")]
        [InlineData("log.gz")]
        public async Task LoggingSource_WhenExistFileFromYesterdayAndCreateNewFileForToday_ShouldResetNumberToZero(string extension)
        {
            var testName = GetTestName();
            var path = NewDataPath(forceCreateDir: true);
            path = Path.Combine(path, Guid.NewGuid().ToString("N"));
            Directory.CreateDirectory(path);

            const long retentionSize = long.MaxValue;
            var retentionTimeConfiguration = TimeSpan.FromDays(3);

            var yesterday = DateTime.Now - TimeSpan.FromDays(1);
            var yesterdayLog = Path.Combine(path, $"{LoggingSource.LogInfo.DateToLogFormat(yesterday)}.010.{extension}");
            await File.Create(yesterdayLog).DisposeAsync();
            File.SetCreationTime(yesterdayLog, yesterday);

            var loggingSource = new LoggingSource(
                LogMode.Information,
                path,
                "LoggingSource" + testName,
                retentionTimeConfiguration,
                retentionSize);

            var logger = new Logger(loggingSource, "Source" + testName, "Logger" + testName);
            await logger.OperationsWithWait("Some message");

            var todayLog = string.Empty;
            WaitForValue(() =>
            {
                var afterEndFiles = Directory.GetFiles(path);
                todayLog = afterEndFiles.FirstOrDefault(f =>
                    LoggingSource.LogInfo.TryGetCreationTimeLocal(f, out var date) && date.Date.Equals(DateTime.Today));
                return todayLog != null;
            }, true, 10_000, 1_000);

            Assert.True(LoggingSource.LogInfo.TryGetNumber(todayLog, out var n) && n == 0);

            loggingSource.EndLogging();
        }

        [Theory]
        [InlineData("log")]
        [InlineData("log.gz")]
        public async Task LoggingSource_WhenExistFileFromToday_ShouldIncrementNumberByOne(string extension)
        {
            const int fileSize = Constants.Size.Kilobyte;

            var testName = GetTestName();
            var path = NewDataPath(forceCreateDir: true);
            path = Path.Combine(path, Guid.NewGuid().ToString("N"));
            Directory.CreateDirectory(path);

            const long retentionSize = long.MaxValue;
            var retentionTimeConfiguration = TimeSpan.FromDays(3);

            var now = DateTime.Now;
            var existLog = Path.Combine(path, $"{LoggingSource.LogInfo.DateToLogFormat(now)}.010.{extension}");
            await using (var file = File.Create(existLog))
            {
                file.SetLength(fileSize);
            }
            var loggingSource = new LoggingSource(
                LogMode.Information,
                path,
                "LoggingSource" + testName,
                retentionTimeConfiguration,
                retentionSize)
            { MaxFileSizeInBytes = fileSize };
            //This is just to make sure the MaxFileSizeInBytes is get action for the first file
            loggingSource.SetupLogMode(LogMode.Operations, path, retentionTimeConfiguration, retentionSize, false);

            var logger = new Logger(loggingSource, "Source" + testName, "Logger" + testName);
            await logger.OperationsWithWait("Some message");

            var result = WaitForValue(() =>
            {
                var strings = Directory.GetFiles(path);
                return strings.Any(f =>
                {
                    if (LoggingSource.LogInfo.TryGetLastWriteTimeLocal(f, out var d) == false || d.Date.Equals(DateTime.Today) == false)
                        return false;

                    return LoggingSource.LogInfo.TryGetNumber(f, out var n) && n == 11;
                });
            }, true, 10_000, 1_000);
            Assert.True(result);

            loggingSource.EndLogging();
        }

        [Theory]
        [InlineData(true)]
        [InlineData(false)]
        public async Task LoggingSource_WhileRetentionByTimeInDays_ShouldKeepRetentionPolicy(bool compressing)
        {
            const int fileSize = Constants.Size.Kilobyte;

            var name = GetTestName();
            var path = NewDataPath(forceCreateDir: true);
            path = Path.Combine(path, Guid.NewGuid().ToString());
            Directory.CreateDirectory(path);
            var retentionTime = TimeSpan.FromDays(3);

            var retentionDate = DateTime.Now.Date - retentionTime;
            var toCheckLogFiles = new List<(string fileName, bool shouldExist)>();
            for (var date = retentionDate - TimeSpan.FromDays(2); date <= retentionDate + TimeSpan.FromDays(2); date += TimeSpan.FromDays(1))
            {
                var fileName = Path.Combine(path, $"{LoggingSource.LogInfo.DateToLogFormat(date)}.001.log");
                toCheckLogFiles.Add((fileName, date > retentionDate));
                await using (File.Create(fileName))
                { }
                File.SetLastWriteTime(fileName, date);
            }

            const long retentionSize = long.MaxValue;
            var loggingSource = new LoggingSource(
                LogMode.Information,
                path,
                "LoggingSource" + name,
                retentionTime,
                retentionSize,
                compressing)
            { MaxFileSizeInBytes = fileSize };
            //This is just to make sure the MaxFileSizeInBytes is get action for the first file
            loggingSource.SetupLogMode(LogMode.Operations, path, retentionTime, retentionSize, compressing);

            var logger = new Logger(loggingSource, "Source" + name, "Logger" + name);

            for (var j = 0; j < 50; j++)
            {
                for (var i = 0; i < 5; i++)
                {
                    await logger.OperationsWithWait("Some message");
                }
                Thread.Sleep(10);
            }

            string[] afterEndFiles = null;
            await WaitForValueAsync(async () =>
            {
                for (var i = 0; i < 10; i++)
                {
                    await logger.OperationsWithWait("Some message");
                }
                afterEndFiles = Directory.GetFiles(path);
                return DoesContainFilesThatShouldNotBeFound(afterEndFiles, toCheckLogFiles, compressing);
            }, false, 10_000, 1_000);

            loggingSource.EndLogging();

            AssertNoFileMissing(afterEndFiles);

            try
            {
                Assert.All(toCheckLogFiles, toCheck =>
                {
                    (string fileName, bool shouldExist) = toCheck;
                    fileName = $"{fileName}{(compressing ? ".gz" : string.Empty)}";
                    var fileInfo = new FileInfo(fileName);
                    if (shouldExist)
                    {
                        Assert.True(fileInfo.Exists, $"The log file \"{fileInfo.Name}\" should be exist");
                    }
                    else
                    {
                        Assert.False(fileInfo.Exists, $"The log file \"{fileInfo.Name}\" last modified {fileInfo.LastWriteTime} should not be exist. retentionTime({retentionTime})");
                    }
                });
            }
            catch (Exception e)
            {
                throw new InvalidOperationException($"Logs after end - {JustFileNamesAsString(afterEndFiles)}", e);
            }
        }

        [Theory]
        [InlineData(true)]
        [InlineData(false)]
        public async Task LoggingSource_WhileRetentionBySizeOn_ShouldKeepRetentionPolicy(bool compressing)
        {
            const int fileSize = Constants.Size.Kilobyte;
            const int retentionSize = 10 * Constants.Size.Kilobyte;

            var name = GetTestName();

            var path = NewDataPath(forceCreateDir: true);
            var retentionTime = TimeSpan.MaxValue;
            var loggingSource = new LoggingSource(
                LogMode.Information,
                path,
                "LoggingSource" + name,
                retentionTime,
                retentionSize,
                compressing);
            loggingSource.MaxFileSizeInBytes = fileSize;
            //This is just to make sure the MaxFileSizeInBytes is get action for the first file
            loggingSource.SetupLogMode(LogMode.Operations, path, retentionTime, retentionSize, compressing);

            var logger = new Logger(loggingSource, "Source" + name, "Logger" + name);

            for (var i = 0; i < 100; i++)
            {
                for (var j = 0; j < 5; j++)
                {
                    await logger.OperationsWithWait($"compressing = {compressing}");
                }
                Thread.Sleep(10);
            }

            const int threshold = 3 * fileSize;
            long size = 0;
            FileInfo[] afterEndFiles = null;
            var logDirectory = new DirectoryInfo(path);
            var isRetentionPolicyApplied = WaitForValue(() =>
            {
                logDirectory.Refresh();
                afterEndFiles = logDirectory.GetFiles();
                AssertNoFileMissing(afterEndFiles.Select(f => f.Name).ToArray());
                size = afterEndFiles.Sum(f => f.Length);

                return Math.Abs(size - retentionSize) <= threshold;
            }, true, 10_000, 1_000);

            string errorMessage = isRetentionPolicyApplied 
                ? string.Empty
                : $"{TempInfoToInvestigate(loggingSource, path)}. " +
                  $"ActualSize({size}), retentionSize({retentionSize}), threshold({threshold}), path({path})" +
                  Environment.NewLine + 
                  FileNamesWithSize(afterEndFiles);

            loggingSource.EndLogging();
            
            Assert.True(isRetentionPolicyApplied, errorMessage);
        }

        private static string FileNamesWithSize(FileInfo[] files)
        {
            var filesInfo = files.Select(f => $"{f.Name}({f.Length})");
            var logsAroundError = string.Join(',', filesInfo);
            return logsAroundError;
        }

        private static string TempInfoToInvestigate(LoggingSource loggingSource, string path)
        {
            if (loggingSource.Compressing)
            {
                var loggedInfo = new StringBuilder();
                var logDirectory = new DirectoryInfo(path);
                var logs = logDirectory.GetFiles();
                foreach (var fileInfo in logs)
                {
                    using var file = fileInfo.Open(FileMode.Open, FileAccess.Read, FileShare.ReadWrite);

                    Stream stream;
                    if (fileInfo.Name.EndsWith(".log.gz"))
                    {
                        stream = new GZipStream(file, CompressionMode.Decompress);
                    }
                    else if (fileInfo.Name.EndsWith(".log"))
                    {
                        stream = file;
                    }
                    else
                    {
                        continue;
                    }

                    using var reader = new StreamReader(stream);

                    string line;
                    while ((line = reader.ReadLine()) != null && line.Contains("Something went wrong while compressing log files") == false)
                    {

                    }
                    if(line == null)
                        continue;
                    do
                    {
                        loggedInfo.AppendLine(line);
                    } while ((line = reader.ReadLine()) != null);
                    break;
                }

                var compressLoggingThread = loggingSource.GetType().GetField("_compressLoggingThread", BindingFlags.NonPublic | BindingFlags.Instance)?.GetValue(loggingSource) as Thread;
                if(compressLoggingThread != null)
                    return $"compressLoggingThread: {{IsAlive :{compressLoggingThread.IsAlive}, ThreadState :{compressLoggingThread.ThreadState}}} \n{loggedInfo}";
            }

            return "";
        }

        [Theory]
        [InlineData(true)]
        [InlineData(false)]
        public async Task LoggingSource_WhileLogging_ShouldNotLoseLogFile(bool compressing)
        {
            var name = GetTestName();

            var path = NewDataPath(forceCreateDir: true);
            var retentionTime = TimeSpan.MaxValue;
            var retentionSize = long.MaxValue;
            var loggingSource = new LoggingSource(
                LogMode.Information,
                path,
                "LoggingSource" + name,
                retentionTime,
                retentionSize,
                compressing);
            loggingSource.MaxFileSizeInBytes = 1024;
            //This is just to make sure the MaxFileSizeInBytes is get action for the first file
            loggingSource.SetupLogMode(LogMode.Operations, path, retentionTime, retentionSize, compressing);

            var logger = new Logger(loggingSource, "Source" + name, "Logger" + name);

            for (var i = 0; i < 1000; i++)
            {
                await logger.OperationsWithWait("Some message");
            }

            var beforeEndFiles = Directory.GetFiles(path);

            loggingSource.EndLogging();

            var afterEndFiles = Directory.GetFiles(path);

            AssertNoFileMissing(beforeEndFiles);
            AssertNoFileMissing(afterEndFiles);
        }

        [Theory]
        [InlineData(true)]
        [InlineData(false)]
        public async Task LoggingSource_WhileStopAndStartAgain_ShouldNotOverrideOld(bool compressing)
        {
            const int taskTimeout = 10000;
            var name = GetTestName();

            var path = NewDataPath(forceCreateDir: true);
            var retentionTime = TimeSpan.MaxValue;
            var retentionSize = long.MaxValue;

            var firstLoggingSource = new LoggingSource(
                LogMode.Information,
                path,
                "FirstLoggingSource" + name,
                retentionTime,
                retentionSize,
                compressing);
            firstLoggingSource.MaxFileSizeInBytes = 1024;
            //This is just to make sure the MaxFileSizeInBytes is get action for the first file
            firstLoggingSource.SetupLogMode(LogMode.Operations, path, retentionTime, retentionSize, compressing);

            try
            {
                var logger = new Logger(firstLoggingSource, "Source" + name, "Logger" + name);

                for (var i = 0; i < 100; i++)
                {
                    var task = logger.OperationsWithWait("Some message");
                    await Task.WhenAny(task, Task.Delay(taskTimeout));
                    if (task.IsCompleted == false)
                        throw new TimeoutException($"The log task took more then one second");
                }
            }
            finally
            {
                firstLoggingSource.EndLogging();
            }

            var beforeRestartFiles = Directory.GetFiles(path);

            var restartDateTime = DateTime.Now;

            Exception anotherThreadException = null;
            //To start new LoggingSource the object need to be construct on another thread
            var anotherThread = new Thread(() =>
            {
                var secondLoggingSource = new LoggingSource(
                    LogMode.Information,
                    path,
                    "SecondLoggingSource" + name,
                    retentionTime,
                    retentionSize);

                try
                {
                    secondLoggingSource.MaxFileSizeInBytes = 1024;
                    var secondLogger = new Logger(secondLoggingSource, "Source" + name, "Logger" + name);

                    for (var i = 0; i < 100; i++)
                    {
                        var task = secondLogger.OperationsWithWait("Some message");
                        if (task.WaitWithTimeout(TimeSpan.FromMilliseconds(taskTimeout)).GetAwaiter().GetResult() == false)
                            throw new TimeoutException($"The log task took more then one second");
                    }
                }
                catch (Exception e)
                {
                    anotherThreadException = e;
                }
                finally
                {
                    secondLoggingSource.EndLogging();
                }
            });
            anotherThread.Start();
            anotherThread.Join();
            if (anotherThreadException != null)
                throw anotherThreadException;

            foreach (var file in beforeRestartFiles.OrderBy(f => f).SkipLast(1)) //The last is skipped because it is still written
            {
                var lastWriteTime = File.GetLastWriteTime(file);
                Assert.True(
                    restartDateTime > lastWriteTime,
                    $"{file} was changed (time:" +
                    $"{lastWriteTime.ToString("yyyy-MM-dd HH:mm:ss.fff", CultureInfo.InvariantCulture)}) after the restart (time:" +
                    $"{restartDateTime.ToString("yyyy-MM-dd HH:mm:ss.fff", CultureInfo.InvariantCulture)})" +
                    Environment.NewLine +
                    JustFileNamesAsString(beforeRestartFiles));
            }
        }

        [Theory]
        [InlineData(LogMode.None)]
        [InlineData(LogMode.Operations)]
        [InlineData(LogMode.Information)]
        public async Task Register_WhenLogModeIsOperations_ShouldWriteToLogFileJustAsLogMode(LogMode logMode)
        {
            var timeout = TimeSpan.FromSeconds(10);

            var name = GetTestName();
            var path = NewDataPath(forceCreateDir: true);
            path = Path.Combine(path, Guid.NewGuid().ToString());
            Directory.CreateDirectory(path);

            var loggingSource = new LoggingSource(
                logMode,
                path,
                "LoggingSource" + name,
                TimeSpan.MaxValue,
                long.MaxValue,
                false);

            var logger = new Logger(loggingSource, "Source" + name, "Logger" + name);
            var tcs = new TaskCompletionSource<WebSocketReceiveResult>(TaskCreationOptions.RunContinuationsAsynchronously);
            var socket = new MyDummyWebSocket();
            socket.ReceiveAsyncFunc = () => tcs.Task;
            var context = new LoggingSource.WebSocketContext();

            //Register
            var _ = loggingSource.Register(socket, context, CancellationToken.None);
            var beforeCloseOperation = Guid.NewGuid().ToString();
            var beforeCloseInformation = Guid.NewGuid().ToString();

            var logTasks = Task.WhenAll(logger.OperationsWithWait(beforeCloseOperation), logger.InfoWithWait(beforeCloseInformation));
            await Task.WhenAny(logTasks, Task.Delay(timeout));
            Assert.True(logTasks.IsCompleted, $"Waited over {timeout.TotalSeconds} seconds for log tasks to finish");

            const int socketTimeout = 5000;
            var socketContainsLogs = WaitForValue(() => socket.LogsReceived.Contains(beforeCloseInformation) && socket.LogsReceived.Contains(beforeCloseOperation),
                true, socketTimeout, 100);
            Assert.True(socketContainsLogs, $"Waited over {socketTimeout} seconds for log to be written to socket");

            //Close socket
            socket.Close();
            tcs.SetResult(new WebSocketReceiveResult(1, WebSocketMessageType.Text, true, WebSocketCloseStatus.NormalClosure, string.Empty));

            var afterCloseOperation = Guid.NewGuid().ToString();
            var afterCloseInformation = Guid.NewGuid().ToString();

            logTasks = Task.WhenAll(logger.OperationsWithWait(afterCloseOperation), logger.InfoWithWait(afterCloseInformation));
            await Task.WhenAny(logTasks, Task.Delay(timeout));
            Assert.True(logTasks.IsCompleted || logMode == LogMode.None,
                $"Waited over {timeout.TotalSeconds} seconds for log tasks to finish");

            loggingSource.EndLogging();

            string logsFileContentAfter = await ReadLogsFileContent(path);

            AssertContainsLog(LogMode.Information, logMode)(beforeCloseInformation, logsFileContentAfter);
            AssertContainsLog(LogMode.Operations, logMode)(beforeCloseOperation, logsFileContentAfter);

            AssertContainsLog(LogMode.Information, logMode)(afterCloseInformation, logsFileContentAfter);
            AssertContainsLog(LogMode.Operations, logMode)(afterCloseOperation, logsFileContentAfter);
        }

        [Theory]
        [InlineData(LogMode.None)]
        [InlineData(LogMode.Operations)]
        [InlineData(LogMode.Information)]
        public async Task AttachPipeSink_WhenLogModeIsOperations_ShouldWriteToLogFileJustOperations(LogMode logMode)
        {
            var timeout = TimeSpan.FromSeconds(10);

            var name = GetTestName();
            var path = NewDataPath(forceCreateDir: true);
            path = Path.Combine(path, Guid.NewGuid().ToString());
            Directory.CreateDirectory(path);

            var loggingSource = new LoggingSource(
                logMode,
                path,
                "LoggingSource" + name,
                TimeSpan.MaxValue,
                long.MaxValue,
                false);

            var logger = new Logger(loggingSource, "Source" + name, "Logger" + name);
            var tcs = new TaskCompletionSource<WebSocketReceiveResult>(TaskCreationOptions.RunContinuationsAsynchronously);

            await using var stream = new MemoryStream();

            //Attach Pipe
            loggingSource.AttachPipeSink(stream);
            var beforeDetachOperation = Guid.NewGuid().ToString();
            var beforeDetachInformation = Guid.NewGuid().ToString();

            var logTasks = Task.WhenAll(logger.OperationsWithWait(beforeDetachOperation), logger.InfoWithWait(beforeDetachInformation));
            await Task.WhenAny(logTasks, Task.Delay(timeout));
            Assert.True(logTasks.IsCompleted, $"Waited over {timeout.TotalSeconds} seconds for log tasks to finish");

            //Detach Pipe
            loggingSource.DetachPipeSink();
            await stream.FlushAsync();

            var afterDetachOperation = Guid.NewGuid().ToString();
            var afterDetachInformation = Guid.NewGuid().ToString();

            logTasks = Task.WhenAll(logger.OperationsWithWait(afterDetachOperation), logger.InfoWithWait(afterDetachInformation));
            await Task.WhenAny(logTasks, Task.Delay(timeout));
            Assert.True(logTasks.IsCompleted || logMode == LogMode.None,
                $"Waited over {timeout.TotalSeconds} seconds for log tasks to finish");

            tcs.SetResult(new WebSocketReceiveResult(1, WebSocketMessageType.Text, true, WebSocketCloseStatus.NormalClosure, ""));
            loggingSource.EndLogging();

            var logsFromPipe = Encodings.Utf8.GetString(stream.ToArray());
            Assert.Contains(beforeDetachInformation, logsFromPipe);
            Assert.Contains(beforeDetachOperation, logsFromPipe);
            Assert.DoesNotContain(afterDetachInformation, logsFromPipe);
            Assert.DoesNotContain(afterDetachOperation, logsFromPipe);

            string logsFileContentAfter = await ReadLogsFileContent(path);

            AssertContainsLog(LogMode.Information, logMode)(beforeDetachInformation, logsFileContentAfter);
            AssertContainsLog(LogMode.Operations, logMode)(beforeDetachOperation, logsFileContentAfter);

            AssertContainsLog(LogMode.Information, logMode)(afterDetachInformation, logsFileContentAfter);
            AssertContainsLog(LogMode.Operations, logMode)(afterDetachOperation, logsFileContentAfter);
        }

        private static async Task<string> ReadLogsFileContent(string path)
        {
            var logsFileContent = "";
            var logsFile = Directory.GetFiles(path);
            foreach (string logFile in logsFile)
            {
                logsFileContent += await File.ReadAllTextAsync(logFile);
            }

            return logsFileContent;
        }

        private Action<string, string> AssertContainsLog(LogMode logType, LogMode logMode)
        {
            if (logMode == LogMode.Information || logMode == logType)
                return Assert.Contains;

            return Assert.DoesNotContain;
        }

        [RavenFact(RavenTestCategory.Logging)]
        public async Task Register_WhenLogModeIsNone_ShouldNotWriteToLogFile()
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

            var logger = new Logger(loggingSource, "Source" + name, "Logger" + name);
            var tcs = new TaskCompletionSource<WebSocketReceiveResult>(TaskCreationOptions.RunContinuationsAsynchronously);
            var socket = new MyDummyWebSocket();
            socket.ReceiveAsyncFunc = () => tcs.Task;
            var context = new LoggingSource.WebSocketContext();

            var _ = loggingSource.Register(socket, context, CancellationToken.None);

            var uniqForOperation = Guid.NewGuid().ToString();
            var uniqForInformation = Guid.NewGuid().ToString();

            var logTasks = Task.WhenAll(logger.OperationsWithWait(uniqForOperation), logger.InfoWithWait(uniqForInformation));
            var timeout = TimeSpan.FromSeconds(10);
            await Task.WhenAny(logTasks, Task.Delay(timeout));
            Assert.True(logTasks.IsCompleted, $"Waited over {timeout.TotalSeconds} seconds for log tasks to finish");

            tcs.SetResult(new WebSocketReceiveResult(1, WebSocketMessageType.Text, true, WebSocketCloseStatus.NormalClosure, ""));
            loggingSource.EndLogging();

            Assert.Contains(uniqForOperation, socket.LogsReceived);
            Assert.Contains(uniqForInformation, socket.LogsReceived);

            var logFile = Directory.GetFiles(path).First();
            var logContent = await File.ReadAllTextAsync(logFile);
            Assert.DoesNotContain(uniqForOperation, logContent);
            Assert.DoesNotContain(uniqForInformation, logContent);
        }

        [RavenTheory(RavenTestCategory.Logging)]
        [RavenData(Data = new object[] { true })]
        [RavenData(Data = new object[] { false })]
        public async Task Downloading_Logs_Should_Include_Files_Created_And_Modified_Within_Date_Range(Options options, bool useUtc)
        {
            // Define request date range
            var startDate = new DateTime(2023, 03, 01);
            var endDate = new DateTime(2023, 03, 31);

            // List of test files with expected outcomes
            var testFiles = new List<TestFile>
            {
                new()
                {
                    CreationTime = new DateTime(2023, 03, 01), 
                    LastWriteTime = new DateTime(2023, 03, 06), 
                    ShouldBeIncluded = true
                },
                new()
                {
                    CreationTime = new DateTime(2023, 03, 07), 
                    LastWriteTime = new DateTime(2023, 03, 29), 
                    ShouldBeIncluded = true
                },
                new()
                {
                    CreationTime = new DateTime(2023, 03, 30), 
                    LastWriteTime = new DateTime(2023, 03, 31), 
                    ShouldBeIncluded = true
                },
            };

            // Run the test with the specified files and date range
            await VerifyLogDownloadByDateRange(startDate, endDate, testFiles, useUtc);
        }

        [RavenTheory(RavenTestCategory.Logging)]
        [RavenData(Data = new object[] { true })]
        [RavenData(Data = new object[] { false })]
        public async Task Downloading_Logs_Should_Not_Include_Files_Created_Before_And_Modified_After_Date_Range(Options options, bool useUtc)
        {
            // Define request date range
            var startDate = new DateTime(2023, 03, 01);
            var endDate = new DateTime(2023, 03, 31);

            // List of test files with expected outcomes
            var testFiles = new List<TestFile>
            {
                new()
                {
                    CreationTime = new DateTime(2023, 02, 01),
                    LastWriteTime = new DateTime(2023, 02, 27),
                    ShouldBeIncluded = false
                },
                new()
                {
                    CreationTime = new DateTime(2023, 02, 28),
                    LastWriteTime = new DateTime(2023, 03, 01),
                    ShouldBeIncluded = false
                },
                new()
                {
                    CreationTime = new DateTime(2023, 03, 31),
                    LastWriteTime = new DateTime(2023, 04, 01),
                    ShouldBeIncluded = false
                },
                new()
                {
                    CreationTime = new DateTime(2023, 04, 02),
                    LastWriteTime = new DateTime(2023, 04, 07),
                    ShouldBeIncluded = false
                }
            };

            // Run the test with the specified files and date range
            await VerifyLogDownloadByDateRange(startDate, endDate, testFiles, useUtc);
        }

        [RavenTheory(RavenTestCategory.Logging)]
        [RavenData(Data = new object[] { true })]
        [RavenData(Data = new object[] { false })]
        public async Task Downloading_Logs_Should_Include_Files_Created_Within_And_Modified_After_Date_Range(Options options, bool useUtc)
        {
            // Define request date range
            var startDate = new DateTime(2023, 03, 01);
            var endDate = new DateTime(2023, 03, 31);

            // List of test files with expected outcomes
            var testFiles = new List<TestFile>
            {
                new()
                {
                    CreationTime = new DateTime(2023, 03, 29),
                    LastWriteTime = new DateTime(2023, 04, 01),
                    ShouldBeIncluded = true
                },
            };

            // Run the test with the specified files and date range
            await VerifyLogDownloadByDateRange(startDate, endDate, testFiles, useUtc);
        }

        [RavenTheory(RavenTestCategory.Logging)]
        [RavenData(Data = new object[] { true })]
        [RavenData(Data = new object[] { false })]
        public async Task Downloading_Logs_Should_Include_Files_Created_Before_And_Modified_Within_Date_Range(Options options, bool useUtc)
        {
            // Define request date range
            var startDate = new DateTime(2023, 03, 01);
            var endDate = new DateTime(2023, 03, 31);

            // List of test files with expected outcomes
            var testFiles = new List<TestFile>
            {
                new()
                {
                    CreationTime = new DateTime(2023, 02, 15),
                    LastWriteTime = new DateTime(2023, 03, 15),
                    ShouldBeIncluded = true
                },
            };

            // Run the test with the specified files and date range
            await VerifyLogDownloadByDateRange(startDate, endDate, testFiles, useUtc);
        }

        [RavenTheory(RavenTestCategory.Logging)]
        [RavenData(Data = new object[] { true })]
        [RavenData(Data = new object[] { false })]
        public async Task Downloading_Logs_Should_Include_Files_Created_Before_And_Modified_After_Date_Range(Options options, bool useUtc)
        {
            // Define request date range
            var startDate = new DateTime(2023, 03, 01);
            var endDate = new DateTime(2023, 03, 31);

            // List of test files with expected outcomes
            var testFiles = new List<TestFile>
            {
                new()
                {
                    CreationTime = new DateTime(2023, 02, 27),
                    LastWriteTime = new DateTime(2023, 04, 05),
                    ShouldBeIncluded = true
                }
            };

            // Run the test with the specified files and date range
            await VerifyLogDownloadByDateRange(startDate, endDate, testFiles, useUtc);
        }

        [RavenTheory(RavenTestCategory.Logging)]
        [RavenData(Data = new object[] { true })]
        [RavenData(Data = new object[] { false })]
        public async Task Downloading_Logs_Should_Include_Files_With_Creation_And_Modification_Dates_Matching_Exact_Date_Range(Options options, bool useUtc)
        {
            // Define request date range
            var startDate = new DateTime(2023, 03, 01);
            var endDate = new DateTime(2023, 03, 31);

            // List of test files with expected outcomes
            var testFiles = new List<TestFile>
            {
                new()
                {
                    CreationTime = new DateTime(2023, 03, 01),
                    LastWriteTime = new DateTime(2023, 03, 31),
                    ShouldBeIncluded = true
                },
            };

            // Run the test with the specified files and date range
            await VerifyLogDownloadByDateRange(startDate, endDate, testFiles, useUtc);
        }

        [RavenTheory(RavenTestCategory.Logging)]
        [RavenData(Data = new object[] { true })]
        [RavenData(Data = new object[] { false })]
        public async Task Downloading_Logs_Should_Include_Files_When_No_Start_Date_Is_Specified(Options options, bool useUtc)
        {
            // No start date specified, only end date
            var endDate = new DateTime(2023, 03, 31);

            // List of test files with their expected inclusion based on the end date
            var testFiles = new List<TestFile>
            {
                // Files modified before the end date should be included
                new()
                {
                    CreationTime = new DateTime(2023, 03, 20),
                    LastWriteTime = new DateTime(2023, 03, 29),
                    ShouldBeIncluded = true
                },
                new()
                {
                    CreationTime = new DateTime(2023, 03, 30),
                    LastWriteTime = new DateTime(2023, 03, 31),
                    ShouldBeIncluded = true
                },
                // Files modified on or after the end date should not be included
                new()
                {
                    CreationTime = new DateTime(2023, 03, 31),
                    LastWriteTime = new DateTime(2023, 04, 01),
                    ShouldBeIncluded = false
                },
                new()
                {
                    CreationTime = new DateTime(2023, 04, 01),
                    LastWriteTime = new DateTime(2023, 04, 16),
                    ShouldBeIncluded = false
                },
            };

            await VerifyLogDownloadByDateRange(startDate: null, endDate, testFiles, useUtc);
        }

        [RavenTheory(RavenTestCategory.Logging)]
        [RavenData(Data = new object[] { true })]
        [RavenData(Data = new object[] { false })]
        public async Task Downloading_Logs_Should_Include_Files_When_No_End_Date_Is_Specified(Options options, bool useUtc)
        {
            // Only start date specified, no end date
            var startDate = new DateTime(2023, 03, 01);

            // List of test files with their expected inclusion based on the start date
            var testFiles = new List<TestFile>
            {
                // Files modified before the start date should not be included
                new()
                {
                    CreationTime = new DateTime(2023, 02, 21),
                    LastWriteTime = new DateTime(2023, 02, 27),
                    ShouldBeIncluded = false
                },
                new()
                {
                    CreationTime = new DateTime(2023, 02, 28),
                    LastWriteTime = new DateTime(2023, 03, 01),
                    ShouldBeIncluded = false
                },
                // Files modified after the start date should be included
                new()
                {
                    CreationTime = new DateTime(2023, 03, 01),
                    LastWriteTime = new DateTime(2023, 03, 02),
                    ShouldBeIncluded = true
                },
                new()
                {
                    CreationTime = new DateTime(2023, 03, 03),
                    LastWriteTime = new DateTime(2023, 03, 15),
                    ShouldBeIncluded = true
                },
            };

            await VerifyLogDownloadByDateRange(startDate, endDate: null, testFiles, useUtc);
        }

        [RavenTheory(RavenTestCategory.Logging)]
        [RavenData(Data = new object[] { true })]
        [RavenData(Data = new object[] { false })]
        public async Task Downloading_Logs_Should_Include_All_Files_When_No_Dates_Are_Specified(Options options, bool useUtc)
        {
            // No start and end dates specified

            // List of test files, all of which should be included as there are no date constraints
            var testFiles = new List<TestFile>
            {
                // All files should be included regardless of their dates
                new()
                {
                    CreationTime = new DateTime(2020, 03, 01),
                    LastWriteTime = new DateTime(2021, 03, 31),
                    ShouldBeIncluded = true
                },
                new()
                {
                    CreationTime = new DateTime(2022, 04, 01),
                    LastWriteTime = new DateTime(2023, 04, 17),
                    ShouldBeIncluded = true
                }
            };

            await VerifyLogDownloadByDateRange(startDate: null, endDate: null, testFiles, useUtc);
        }

        [RavenTheory(RavenTestCategory.Logging)]
        [RavenData(Data = new object[] { true })]
        [RavenData(Data = new object[] { false })]
        public async Task Downloading_Logs_Should_Throw_Exception_When_EndDate_Is_Before_StartDate(Options options, bool useUtc)
        {
            // Define request date range
            var startDate = new DateTime(2023, 03, 31);
            var endDate = new DateTime(2023, 03, 01);

            // List of test files with expected outcomes
            var testFiles = new List<TestFile>
            {
                new()
                {
                    CreationTime = new DateTime(2023, 02, 27),
                    LastWriteTime = new DateTime(2023, 04, 05),
                    ShouldBeIncluded = true
                }
            };

            // Run the test with the specified files and date range
            var exception = await Record.ExceptionAsync(() => VerifyLogDownloadByDateRange(startDate, endDate, testFiles, useUtc));
            Assert.IsType<RavenException>(exception);
            Assert.IsType<ArgumentException>(exception.InnerException);

            var expectedMessage =
                $"End Date '{endDate.ToUniversalTime():yyyy-MM-ddTHH:mm:ss.fffffff} UTC' " +
                $"must be greater than Start Date '{startDate.ToUniversalTime():yyyy-MM-ddTHH:mm:ss.fffffff} UTC'";

            Assert.True(exception.InnerException.Message.Contains(expectedMessage),
                userMessage:$"exception.InnerException.Message: {exception.InnerException.Message}{Environment.NewLine}" +
                            $"but expectedMessage: {expectedMessage}");
        }

        [RavenTheory(RavenTestCategory.Logging)]
        [RavenData(Data = new object[] { true })]
        [RavenData(Data = new object[] { false })]
        public async Task Downloading_Logs_No_Logs_Should_Be_Selected_When_Neither_Start_Nor_End_Date_Matches(Options options, bool useUtc)
        {
            // Define request date range
            var startDate = new DateTime(2023, 03, 01);
            var endDate = new DateTime(2023, 03, 31);

            // List of test files with expected outcomes (none should be included)
            var testFiles = new List<TestFile>
            {
                new()
                {
                    CreationTime = new DateTime(2023, 02, 27),
                    LastWriteTime = new DateTime(2023, 02, 28),
                    ShouldBeIncluded = false
                }
            };

            // Run the test with the specified files and date range
            await VerifyLogDownloadByDateRange(startDate, endDate, testFiles, useUtc);
        }

        [RavenTheory(RavenTestCategory.Logging)]
        [RavenData(Data = new object[] { true })]
        [RavenData(Data = new object[] { false })]
        public async Task Downloading_Logs_No_Logs_Should_Be_Selected_When_Only_Start_Date_Is_Specified(Options options, bool useUtc)
        {
            // Define request date range with only start date
            var startDate = new DateTime(2023, 03, 31);

            // List of test files with expected outcomes (none should be included)
            var testFiles = new List<TestFile>
            {
                new()
                {
                    CreationTime = new DateTime(2023, 03, 02),
                    LastWriteTime = new DateTime(2023, 03, 03),
                    ShouldBeIncluded = false
                }
            };

            // Run the test with the specified files and date range
            await VerifyLogDownloadByDateRange(startDate, endDate: null, testFiles, useUtc);
        }

        [RavenTheory(RavenTestCategory.Logging)]
        [RavenData(Data = new object[] { true })]
        [RavenData(Data = new object[] { false })]
        public async Task Downloading_Logs_No_Logs_Should_Be_Selected_When_Only_End_Date_Is_Specified(Options options, bool useUtc)
        {
            // Define request date range with only end date
            var endDate = new DateTime(2023, 03, 01);

            // List of test files with expected outcomes (none should be included)
            var testFiles = new List<TestFile>
            {
                new()
                {
                    CreationTime = new DateTime(2023, 03, 05),
                    LastWriteTime = new DateTime(2023, 03, 25),
                    ShouldBeIncluded = false
                }
            };

            // Run the test with the specified files and date range
            await VerifyLogDownloadByDateRange(startDate: null, endDate, testFiles, useUtc);
        }

        private async Task VerifyLogDownloadByDateRange(DateTime? startDate, DateTime? endDate, List<TestFile> testFiles, bool useUtc, [CallerMemberName] string caller = null)
        {
            var path = RavenTestHelper.NewDataPath(caller, 0, forceCreateDir: true);
            try
            {
                using var server = GetNewServer(new ServerCreationOptions
                {
                    CustomSettings = new Dictionary<string, string>
                    {
                        { RavenConfiguration.GetKey(x => x.Logs.Path), path },
                        { RavenConfiguration.GetKey(x => x.Logs.UseUtcTime), useUtc.ToString() },
                    }
                });

                // Create test files as specified in the testFiles list
                foreach (var testFile in testFiles)
                {
                    Assert.NotNull(testFile);
                    Assert.True(string.IsNullOrWhiteSpace(testFile.FileName));

                    testFile.FileName = $"{testFile.CreationTime:yyyy-MM-dd}.000.log";
                    CreateTestFile(path, testFile, useUtc);
                }

                // Initialize the document store and execute the download logs command
                using (var store = GetDocumentStore(new Options { Server = server }))
                using (var commands = store.Commands())
                {
                    var command = new DownloadLogsCommand(startDate, endDate);
                    await commands.RequestExecutor.ExecuteAsync(command, commands.Context);

                    // Ensure that the command returns some result
                    Assert.True(command.Result.Length > 0);
                    var zipBytes = command.Result;

                    // Read the result as a zip archive
                    using (var msZip = new MemoryStream(zipBytes))
                    using (var archive = new ZipArchive(msZip, ZipArchiveMode.Read, false))
                    {
                        var entries = archive.Entries.Select(entry => entry.Name).ToList();

                        // Check each file in testFiles to see if it's correctly included or excluded
                        foreach (var file in testFiles)
                        {
                            if (file.ShouldBeIncluded)
                                Assert.True(entries.Contains(file.FileName), $"Archive does not contain {file.FileName}{Environment.NewLine}{BuildDebugInfo()}");
                            else
                                Assert.False(entries.Contains(file.FileName), $"Archive contain {file.FileName} but it shouldn't{Environment.NewLine}{BuildDebugInfo()}");

                            continue;

                            string BuildDebugInfo()
                            {
                                var sb = new StringBuilder();
                                sb.AppendLine("Debug info:");

                                sb.Append("Request from '");
                                sb.Append(startDate.HasValue ? startDate.Value.ToUniversalTime().ToString("yyyy-MM-ddTHH:mm:ss.fffffff 'UTC'") : "not specified");
                                sb.Append("' to '");
                                sb.Append(endDate.HasValue ? endDate.Value.ToUniversalTime().ToString("yyyy-MM-ddTHH:mm:ss.fffffff 'UTC'") : "not specified");
                                sb.AppendLine("'");
                                sb.AppendLine();

                                sb.AppendLine("Files in folder: ");
                                sb.Append("Path: ");
                                sb.AppendLine(path);

                                string[] files = Directory.GetFiles(path);
                                for (int index = 0; index < files.Length; index++)
                                {
                                    string pathToFile = files[index];
                                    string fileName = Path.GetFileName(pathToFile);
                                    sb.AppendLine(
                                        $" {index + 1}) " +
                                        $"FileName: {fileName}, " +
                                        $"CreationTime: {LoggingSource.LogInfo.GetLogFileCreationTime(pathToFile, DateTimeKind.Utc)} 'UTC', " +
                                        $"LastWriteTime: {File.GetLastWriteTimeUtc(pathToFile)} 'UTC', ");
                                }
                                sb.AppendLine();

                                sb.AppendLine("Test files:");
                                for (int index = 0; index < testFiles.Count; index++)
                                {
                                    var testFile = testFiles[index];
                                    sb.AppendLine(
                                        $" {index + 1}) " +
                                        $"FileName: {testFile.FileName}, " +
                                        $"CreationTime: {testFile.CreationTime.ToUniversalTime()} 'UTC', " +
                                        $"LastWriteTime: {testFile.LastWriteTime.ToUniversalTime()} 'UTC', " +
                                        $"ShouldBeIncluded: {testFile.ShouldBeIncluded}");
                                }
                                sb.AppendLine();

                                sb.AppendLine("Archive files:");
                                for (int index = 0; index < archive.Entries.Count; index++)
                                {
                                    ZipArchiveEntry entry = archive.Entries[index];
                                    sb.AppendLine(
                                        $" {index + 1}) " +
                                        $"FullName: {entry.FullName}, " +
                                        $"LastWriteTime: {entry.LastWriteTime.ToUniversalTime()} 'UTC'");
                                }

                                return sb.ToString();
                            }
                        }

                        if (testFiles.Any(x => x.ShouldBeIncluded))
                            return;

                        Assert.Single(archive.Entries);
                        Assert.True(archive.Entries[0].Name == "No logs matched the date range.txt");

                        // Assert that the file content is as expected
                        await using (var entryStream = archive.Entries[0].Open())
                        using (var streamReader = new StreamReader(entryStream))
                        {
                            string content = await streamReader.ReadToEndAsync();
                            var formattedStartUtc = startDate.HasValue ? startDate.Value.ToUniversalTime().ToString("yyyy-MM-ddTHH:mm:ss.fffffff 'UTC'") : "not specified";
                            var formattedEndUtc = endDate.HasValue ? endDate.Value.ToUniversalTime().ToString("yyyy-MM-ddTHH:mm:ss.fffffff 'UTC'") : "not specified";

                            Assert.Equal($"No log files were found that matched the specified date range from '{formattedStartUtc}' to '{formattedEndUtc}'.", content);
                        }
                    }
                }
            }
            finally
            {
                Directory.Delete(path, recursive: true);
            }
        }

        private static void CreateTestFile(string path, TestFile testFile, bool useUtc)
        {
            string filePath = Path.Combine(path, testFile.FileName);
            var logEntryTime = useUtc
                ? testFile.CreationTime.ToUniversalTime()
                : testFile.CreationTime;

            File.WriteAllText(filePath, contents: $"""
                                                   Time, Thread, Level, Source, Logger, Message, Exception
                                                   {logEntryTime.GetDefaultRavenFormat()}, 1, Operations, Server, Raven.Server.Program
                                                   """);

            File.SetLastWriteTime(filePath, testFile.LastWriteTime);
        }

        private class TestFile
        {
            protected internal string FileName;
            protected internal DateTime CreationTime;
            protected internal DateTime LastWriteTime;
            protected internal bool ShouldBeIncluded;
        }

        private class MyDummyWebSocket : WebSocket
        {
            private bool _close;
            public string LogsReceived { get; private set; } = "";

            public void Close() => _close = true;

            public Func<Task<WebSocketReceiveResult>> ReceiveAsyncFunc { get; set; } = () => Task.FromResult(new WebSocketReceiveResult(1, WebSocketMessageType.Text, true));

            public override void Abort()
            {
            }

            public override Task CloseAsync(WebSocketCloseStatus closeStatus, string statusDescription, CancellationToken cancellationToken)
                => Task.CompletedTask;

            public override Task CloseOutputAsync(WebSocketCloseStatus closeStatus, string statusDescription, CancellationToken cancellationToken)
                => Task.CompletedTask;

            public override void Dispose()
            {
            }

            public override Task<WebSocketReceiveResult> ReceiveAsync(ArraySegment<byte> buffer, CancellationToken cancellationToken) => ReceiveAsyncFunc();

            public override Task SendAsync(ArraySegment<byte> buffer, WebSocketMessageType messageType, bool endOfMessage, CancellationToken cancellationToken)
            {
                if (_close)
                    throw new Exception("Closed");
                LogsReceived += Encodings.Utf8.GetString(buffer.ToArray());
                return Task.CompletedTask;
            }

            public override WebSocketCloseStatus? CloseStatus { get; }
            public override string CloseStatusDescription { get; }
            public override WebSocketState State { get; }
            public override string SubProtocol { get; }
        }

        private static string GetTestName([CallerMemberName] string memberName = "") => memberName;

        private void AssertNoFileMissing(string[] files)
        {
            Assert.NotEmpty(files);

            var exceptions = new List<Exception>();

            var list = GetLogMetadataOrderedByDateThenByNumber(files, exceptions);

            for (var i = 1; i < list.Length; i++)
            {
                var previous = list[i - 1];
                var current = list[i];
                if (previous.Date == current.Date && previous.Number + 1 != current.Number)
                {
                    if (previous.Number == current.Number
                        && ((Path.GetExtension(previous.FileName) == ".gz" && Path.GetExtension(current.FileName) == ".log") || (Path.GetExtension(previous.FileName) == ".log" && Path.GetExtension(current.FileName) == ".gz")))
                        continue;

                    exceptions.Add(new Exception($"Log between {previous.FileName} and {current.FileName} is missing"));
                }
            }

            if (exceptions.Any())
            {
                var allLogs = JustFileNamesAsString(files);
                throw new AggregateException($"All logs - {allLogs}", exceptions);
            }
        }

        private static string JustFileNamesAsString(string[] files)
        {
            var justFileNames = files.Select(Path.GetFileName);
            var logsAroundError = string.Join(',', justFileNames);
            return logsAroundError;
        }

        private LogMetaData[] GetLogMetadataOrderedByDateThenByNumber(string[] files, List<Exception> exceptions)
        {
            var list = files.Select(f =>
                {
                    var withoutExtension = f.Substring(0, f.IndexOf("log", StringComparison.Ordinal) - ".".Length);
                    var snum = Path.GetExtension(withoutExtension).Substring(1);
                    if (int.TryParse(snum, out var num) == false)
                    {
                        exceptions.Add(new Exception($"incremented number of {f} can't be parsed to int"));
                        return null;
                    }

                    var withoutLogNumber = Path.GetFileNameWithoutExtension(withoutExtension);
                    var strLogDateTime = withoutLogNumber.Substring(withoutLogNumber.Length - "yyyy-MM-dd".Length, "yyyy-MM-dd".Length);
                    if (DateTime.TryParse(strLogDateTime, out var logDateTime) == false)
                    {
                        exceptions.Add(new Exception($"{f} can't be parsed to date format"));
                        return null;
                    }

                    return new LogMetaData
                    {
                        FileName = f,
                        Date = logDateTime,
                        Number = num
                    };
                })
                .Where(f => f != null)
                .OrderBy(f => f.Date)
                .ThenBy(f => f.Number)
                .ToArray();

            return list;
        }

        private class LogMetaData
        {
            public string FileName { set; get; }
            public DateTime Date { set; get; }
            public int Number { set; get; }
        }
    }
}

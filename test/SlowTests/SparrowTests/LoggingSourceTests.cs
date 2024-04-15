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

        [RavenTheory(RavenTestCategory.Logging)]
        [RavenData(Data = new object[] { true })]
        [RavenData(Data = new object[] { false })]
        public async Task LoggingSource_WhileRetentionByTimeInHours_ShouldKeepRetentionPolicy(Options options, bool compressing)
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
                var fileName = Path.Combine(path, $"{LoggingSource.DateToLogFormat(lastModified)}.00{artificialLogsCount - i}.log");
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
                .Any(tc => tc.shouldExist == false && (tc.fileName.Equals(f) || compressing && (tc.fileName + LoggingSource.AdditionalCompressExtension).Equals(f))));
        }

        [RavenTheory(RavenTestCategory.Logging)]
        [RavenData(Data = new object[] { true })]
        [RavenData(Data = new object[] { false })]
        public async Task LoggingSource_WhenExistFileFromYesterdayAndCreateNewFileForToday_ShouldResetNumberToZero(Options options, bool compressing)
        {
            string extension = compressing ? LoggingSource.FullCompressExtension : LoggingSource.LogExtension;

            var testName = GetTestName();
            var path = NewDataPath(forceCreateDir: true);
            path = Path.Combine(path, Guid.NewGuid().ToString("N"));
            Directory.CreateDirectory(path);

            const long retentionSize = long.MaxValue;
            var retentionTimeConfiguration = TimeSpan.FromDays(3);

            var yesterday = DateTime.Now - TimeSpan.FromDays(1);
            var yesterdayLog = Path.Combine(path, $"{LoggingSource.DateToLogFormat(yesterday)}.010{extension}");
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
                    LoggingSource.TryGetCreationTimeLocal(f, out var date) && date.Date.Equals(DateTime.Today));
                return todayLog != null;
            }, true, 10_000, 1_000);

            Assert.True(LoggingSource.TryGetLogFileNumber(todayLog, out var n) && n == 0);

            loggingSource.EndLogging();
        }

        [RavenTheory(RavenTestCategory.Logging)]
        [RavenData(Data = new object[] { true })]
        [RavenData(Data = new object[] { false })]
        public async Task LoggingSource_WhenExistFileFromThisMinute_ShouldIncrementNumberByOne(Options options, bool compressing)
        {
            string extension = compressing ? LoggingSource.FullCompressExtension : LoggingSource.LogExtension;

            const int fileSize = Constants.Size.Kilobyte;

            var testName = GetTestName();
            var path = NewDataPath(forceCreateDir: true);
            path = Path.Combine(path, Guid.NewGuid().ToString("N"));
            Directory.CreateDirectory(path);

            const long retentionSize = long.MaxValue;
            var retentionTimeConfiguration = TimeSpan.FromDays(3);

            var now = DateTime.Now;
            var existLog = Path.Combine(path, $"{LoggingSource.DateToLogFormat(now)}.010{extension}");
            await using (var file = File.Create(existLog))
                file.SetLength(fileSize);

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
                    if (LoggingSource.TryGetLastWriteTimeLocal(f, out var d) == false || d.Date.Equals(DateTime.Today) == false)
                        return false;

                    return LoggingSource.TryGetLogFileNumber(f, out var n) && n == 11;
                });
            }, true, 10_000, 1_000);
            Assert.True(result);

            loggingSource.EndLogging();
        }

        [RavenTheory(RavenTestCategory.Logging)]
        [RavenData(Data = new object[] { true })]
        [RavenData(Data = new object[] { false })]
        public async Task LoggingSource_WhileRetentionByTimeInDays_ShouldKeepRetentionPolicy(Options options, bool compressing)
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
                var fileName = Path.Combine(path, $"{LoggingSource.DateToLogFormat(date)}.001{LoggingSource.LogExtension}");
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
                    fileName = $"{fileName}{(compressing ? LoggingSource.AdditionalCompressExtension : string.Empty)}";
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

        [RavenTheory(RavenTestCategory.Logging)]
        [RavenData(Data = new object[] { true })]
        [RavenData(Data = new object[] { false })]
        public async Task LoggingSource_WhileRetentionBySizeOn_ShouldKeepRetentionPolicy(Options options, bool compressing)
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
                compressing)
                { MaxFileSizeInBytes = fileSize };
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
                    if (fileInfo.Name.EndsWith(LoggingSource.FullCompressExtension))
                    {
                        stream = new GZipStream(file, CompressionMode.Decompress);
                    }
                    else if (fileInfo.Name.EndsWith(LoggingSource.LogExtension))
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

        [RavenTheory(RavenTestCategory.Logging)]
        [RavenData(Data = new object[] { true })]
        [RavenData(Data = new object[] { false })]
        public async Task LoggingSource_WhileLogging_ShouldNotLoseLogFile(Options options, bool compressing)
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
                compressing)
                { MaxFileSizeInBytes = 1024 };
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

        [RavenTheory(RavenTestCategory.Logging)]
        [RavenData(Data = new object[] { true })]
        [RavenData(Data = new object[] { false })]
        public async Task LoggingSource_WhileStopAndStartAgain_ShouldNotOverrideOld(Options options, bool compressing)
        {
            const int taskTimeout = 10000;
            const long retentionSize = long.MaxValue;

            var name = GetTestName();
            var path = NewDataPath(forceCreateDir: true);
            var retentionTime = TimeSpan.MaxValue;

            var firstLoggingSource = new LoggingSource(
                LogMode.Information,
                path,
                "FirstLoggingSource" + name,
                retentionTime,
                retentionSize,
                compressing)
                { MaxFileSizeInBytes = 1024 };
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
            // To start new LoggingSource the object need to be constructed on another thread
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

        [RavenTheory(RavenTestCategory.Logging)]

        [RavenData(Data = new object[] { LogMode.None })]
        [RavenData(Data = new object[] { LogMode.Operations })]
        [RavenData(Data = new object[] { LogMode.Information })]
        public async Task Register_WhenLogModeIsOperations_ShouldWriteToLogFileJustAsLogMode(Options options, LogMode logMode)
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

        [RavenTheory(RavenTestCategory.Logging)]
        [RavenData(Data = new object[] { LogMode.None })]
        [RavenData(Data = new object[] { LogMode.Operations })]
        [RavenData(Data = new object[] { LogMode.Information })]
        public async Task AttachPipeSink_WhenLogModeIsOperations_ShouldWriteToLogFileJustOperations(Options options, LogMode logMode)
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
        public async Task DownloadingLogs_ShouldIncludeFiles_CreatedAndModifiedWithin_DateRange(Options options, bool compressing)
        {
            // Define request date range
            const string startDate = "01-03-2023 00:01";
            const string endDate = "31-03-2023 23:59";

            // List of test files with expected outcomes
            var testFiles = new List<TestFile>
            {
                new()
                {
                    CreationTime = "01-03-2023 00:01",
                    LastWriteTime = "06-03-2023 07:08",
                    ShouldBeIncluded = true
                },
                new()
                {
                    CreationTime = "07-03-2023 08:09",
                    LastWriteTime = "29-03-2023 10:11",
                    ShouldBeIncluded = true
                },
                new()
                {
                    CreationTime = "30-03-2023 12:13",
                    LastWriteTime = "31-03-2023 23:59",
                    ShouldBeIncluded = true
                },
            };

            // Run the test with the specified files and date range
            await VerifyLogDownloadByDateRange(startDate, endDate, testFiles, compressing);
        }

        [RavenTheory(RavenTestCategory.Logging)]
        [RavenData(Data = new object[] { true })]
        [RavenData(Data = new object[] { false })]
        public async Task DownloadingLogs_ShouldNotIncludeFiles_CreatedBeforeAndModifiedAfter_DateRange(Options options, bool compressing)
        {
            // Define request date range
            const string startDate = "01-03-2023 02:03";
            const string endDate = "31-03-2023 11:11";

            // List of test files with expected outcomes
            var testFiles = new List<TestFile>
            {
                new()
                {
                    CreationTime = "01-02-2023 00:01",
                    LastWriteTime = "27-02-2023 23:59",
                    ShouldBeIncluded = false
                },
                new()
                {
                    CreationTime = "28-02-2023 23:59",
                    LastWriteTime = "01-03-2023 02:03",
                    ShouldBeIncluded = false
                },
                new()
                {
                    CreationTime = "31-03-2023 11:11",
                    LastWriteTime = "01-04-2023 23:59",
                    ShouldBeIncluded = false
                },
                new()
                {
                    CreationTime = "02-04-2023 00:01",
                    LastWriteTime = "07-04-2023 23:59",
                    ShouldBeIncluded = false
                }
            };

            // Run the test with the specified files and date range
            await VerifyLogDownloadByDateRange(startDate, endDate, testFiles, compressing);
        }

        [RavenTheory(RavenTestCategory.Logging)]
        [RavenData(Data = new object[] { true })]
        [RavenData(Data = new object[] { false })]
        public async Task DownloadingLogs_ShouldIncludeFiles_CreatedWithinAndModifiedAfter_DateRange(Options options, bool compressing)
        {
            // Define request date range
            const string startDate = "01-03-2023 00:01";
            const string endDate = "31-03-2023 23:59";

            // List of test files with expected outcomes
            var testFiles = new List<TestFile>
            {
                new()
                {
                    CreationTime = "29-03-2023 11:22",
                    LastWriteTime = "01-04-2023 22:33",
                    ShouldBeIncluded = true
                },
            };

            // Run the test with the specified files and date range
            await VerifyLogDownloadByDateRange(startDate, endDate, testFiles, compressing);
        }

        [RavenTheory(RavenTestCategory.Logging)]
        [RavenData(Data = new object[] { true })]
        [RavenData(Data = new object[] { false })]
        public async Task DownloadingLogs_ShouldIncludeFiles_CreatedBeforeAndModifiedWithin_DateRange(Options options, bool compressing)
        {
            // Define request date range
            const string startDate = "01-03-2023 00:01";
            const string endDate = "31-03-2023 23:59";

            // List of test files with expected outcomes
            var testFiles = new List<TestFile>
            {
                new()
                {
                    CreationTime = "15-02-2023 00:01",
                    LastWriteTime = "15-03-2023 23:59",
                    ShouldBeIncluded = true
                },
            };

            // Run the test with the specified files and date range
            await VerifyLogDownloadByDateRange(startDate, endDate, testFiles, compressing);
        }

        [RavenTheory(RavenTestCategory.Logging)]
        [RavenData(Data = new object[] { true })]
        [RavenData(Data = new object[] { false })]
        public async Task DownloadingLogs_ShouldIncludeFiles_CreatedBeforeAndModifiedAfter_DateRange(Options options, bool compressing)
        {
            // Define request date range
            const string startDate = "01-03-2023 00:01";
            const string endDate = "31-03-2023 23:59";

            // List of test files with expected outcomes
            var testFiles = new List<TestFile>
            {
                new()
                {
                    CreationTime = "27-02-2023 22:22",
                    LastWriteTime = "05-04-2023 11:05",
                    ShouldBeIncluded = true
                }
            };

            // Run the test with the specified files and date range
            await VerifyLogDownloadByDateRange(startDate, endDate, testFiles, compressing);
        }

        [RavenTheory(RavenTestCategory.Logging)]
        [RavenData(Data = new object[] { true })]
        [RavenData(Data = new object[] { false })]
        public async Task DownloadingLogs_ShouldIncludeFiles_WithCreationAndModificationDatesMatchingExact_DateRange(Options options, bool compressing)
        {
            // Define request date range
            const string startDate = "01-03-2023 00:01";
            const string endDate = "31-03-2023 23:59";

            // List of test files with expected outcomes
            var testFiles = new List<TestFile>
            {
                new()
                {
                    CreationTime = "01-03-2023 00:01",
                    LastWriteTime = "31-03-2023 23:59",
                    ShouldBeIncluded = true
                },
            };

            // Run the test with the specified files and date range
            await VerifyLogDownloadByDateRange(startDate, endDate, testFiles, compressing);
        }

        [RavenTheory(RavenTestCategory.Logging)]
        [RavenData(Data = new object[] { true })]
        [RavenData(Data = new object[] { false })]
        public async Task DownloadingLogs_ShouldIncludeFiles_WhenNoStartDateIsSpecified(Options options, bool compressing)
        {
            // No start date specified, only end date
            const string endDate = "31-03-2023 23:58";

            // List of test files with their expected inclusion based on the end date
            var testFiles = new List<TestFile>
            {
                // Files modified before the end date should be included
                new()
                {
                    CreationTime = "20-03-2023 00:01",
                    LastWriteTime = "29-03-2023 23:59",
                    ShouldBeIncluded = true
                },
                new()
                {
                    CreationTime = "30-03-2023 00:01",
                    LastWriteTime = "31-03-2023 23:59",
                    ShouldBeIncluded = true
                },
                // Files modified on or after the end date should not be included
                new()
                {
                    CreationTime = "31-03-2023 23:59",
                    LastWriteTime = "01-04-2023 23:59",
                    ShouldBeIncluded = false
                },
                new()
                {
                    CreationTime = "01-04-2023 00:01",
                    LastWriteTime = "16-04-2023 23:59",
                    ShouldBeIncluded = false
                },
            };

            await VerifyLogDownloadByDateRange(startDateStr: null, endDate, testFiles, compressing);
        }

        [RavenTheory(RavenTestCategory.Logging)]
        [RavenData(Data = new object[] { true })]
        [RavenData(Data = new object[] { false })]
        public async Task DownloadingLogs_ShouldIncludeFiles_WhenNoEndDateIsSpecified(Options options, bool compressing)
        {
            // Only start date specified, no end date
            const string startDate = "01-03-2023 00:01";

            // List of test files with their expected inclusion based on the start date
            var testFiles = new List<TestFile>
            {
                // Files modified before the start date should not be included
                new()
                {
                    CreationTime = "21-02-2023 00:01",
                    LastWriteTime = "27-02-2023 23:59",
                    ShouldBeIncluded = false
                },
                new()
                {
                    CreationTime ="28-02-2023 00:01",
                    LastWriteTime = "01-03-2023 00:00",
                    ShouldBeIncluded = false
                },
                // Files modified after the start date should be included
                new()
                {
                    CreationTime = "01-03-2023 00:01",
                    LastWriteTime = "02-03-2023 23:59",
                    ShouldBeIncluded = true
                },
                new()
                {
                    CreationTime = "03-03-2023 00:01",
                    LastWriteTime = "15-03-2023 23:59",
                    ShouldBeIncluded = true
                },
            };

            await VerifyLogDownloadByDateRange(startDate, endDateStr: null, testFiles, compressing);
        }

        [RavenTheory(RavenTestCategory.Logging)]
        [RavenData(Data = new object[] { true })]
        [RavenData(Data = new object[] { false })]
        public async Task DownloadingLogs_ShouldIncludeAllFiles_WhenNoDatesAreSpecified(Options options, bool compressing)
        {
            // No start and end dates specified

            // List of test files, all of which should be included as there are no date constraints
            var testFiles = new List<TestFile>
            {
                // All files should be included regardless of their dates
                new()
                {
                    CreationTime = "01-03-2020 12:34",
                    LastWriteTime = "31-03-2021 23:45",
                    ShouldBeIncluded = true
                },
                new()
                {
                    CreationTime = "01-04-2022 21:43",
                    LastWriteTime = "17-04-2023 23:54",
                    ShouldBeIncluded = true
                }
            };

            await VerifyLogDownloadByDateRange(startDateStr: null, endDateStr: null, testFiles, compressing);
        }

        [RavenTheory(RavenTestCategory.Logging)]
        [RavenData(Data = new object[] { true })]
        [RavenData(Data = new object[] { false })]
        public async Task DownloadingLogs_ShouldThrowException_WhenEndDateIsBeforeStartDate(Options options, bool compressing)
        {
            // Define request date range
            const string startDate = "31-03-2023 00:01";
            const string endDate = "01-03-2023 23:59";

            // List of test files with expected outcomes
            var testFiles = new List<TestFile>
            {
                new()
                {
                    CreationTime = "27-02-2023 00:01",
                    LastWriteTime = "05-04-2023 23:59",
                    ShouldBeIncluded = true
                }
            };

            // Run the test with the specified files and date range
            var exception = await Record.ExceptionAsync(() => VerifyLogDownloadByDateRange(startDate, endDate, testFiles, compressing));
            Assert.IsType<RavenException>(exception);
            Assert.IsType<ArgumentException>(exception.InnerException);

            var expectedMessage =
                $"End Date '{endDate.ToUniversalTime():yyyy-MM-ddTHH:mm:ss.fffffff} UTC' must be greater than " +
                $"Start Date '{startDate.ToUniversalTime():yyyy-MM-ddTHH:mm:ss.fffffff} UTC'";

            Assert.True(exception.InnerException.Message.Contains(expectedMessage),
                userMessage:$"exception.InnerException.Message: {exception.InnerException.Message}{Environment.NewLine}" +
                            $"but expectedMessage: {expectedMessage}");
        }

        [RavenTheory(RavenTestCategory.Logging)]
        [RavenData(Data = new object[] { true })]
        [RavenData(Data = new object[] { false })]
        public async Task DownloadingLogs_NoLogsShouldBeSelected_WhenNeitherStartNorEndDateMatches(Options options, bool compressing)
        {
            // Define request date range
            const string startDate = "01-03-2023 00:01";
            const string endDate = "31-03-2023 23:59";

            // List of test files with expected outcomes (none should be included)
            var testFiles = new List<TestFile>
            {
                new()
                {
                    CreationTime = "27-02-2023 00:01",
                    LastWriteTime = "28-02-2023 23:59",
                    ShouldBeIncluded = false
                }
            };

            // Run the test with the specified files and date range
            await VerifyLogDownloadByDateRange(startDate, endDate, testFiles, compressing);
        }

        [RavenTheory(RavenTestCategory.Logging)]
        [RavenData(Data = new object[] { true })]
        [RavenData(Data = new object[] { false })]
        public async Task DownloadingLogs_NoLogsShouldBeSelected_WhenOnlyStartDateIsSpecified(Options options, bool compressing)
        {
            // Define request date range with only start date
            const string startDate = "31-03-2023 00:01";

            // List of test files with expected outcomes (none should be included)
            var testFiles = new List<TestFile>
            {
                new()
                {
                    CreationTime = "02-03-2023 00:01",
                    LastWriteTime = "03-03-2023 23:59",
                    ShouldBeIncluded = false
                }
            };

            // Run the test with the specified files and date range
            await VerifyLogDownloadByDateRange(startDate, endDateStr: null, testFiles, compressing);
        }

        [RavenTheory(RavenTestCategory.Logging)]
        [RavenData(Data = new object[] { true })]
        [RavenData(Data = new object[] { false })]
        public async Task DownloadingLogs_NoLogsShouldBeSelected_WhenOnlyEndDateIsSpecified(Options options, bool compressing)
        {
            // Define request date range with only end date
            const string endDate = "01-03-2023 00:01";

            // List of test files with expected outcomes (none should be included)
            var testFiles = new List<TestFile>
            {
                new()
                {
                    CreationTime = "05-03-2023 00:01",
                    LastWriteTime = "25-03-2023 23:59",
                    ShouldBeIncluded = false
                }
            };

            // Run the test with the specified files and date range
            await VerifyLogDownloadByDateRange(startDateStr: null, endDate, testFiles, compressing);
        }

        private async Task VerifyLogDownloadByDateRange(string startDateStr, string endDateStr, List<TestFile> testFiles, bool compressing, [CallerMemberName] string caller = null)
        {
            var path = RavenTestHelper.NewDataPath(caller, serverPort: 0, forceCreateDir: true);
            try
            {
                using var server = GetNewServer(new ServerCreationOptions
                {
                    CustomSettings = new Dictionary<string, string>
                    {
                        { RavenConfiguration.GetKey(x => x.Logs.Path), path }
                    }
                });

                // Create test files as specified in the testFiles list
                foreach (var testFile in testFiles)
                {
                    Assert.NotNull(testFile);
                    Assert.True(string.IsNullOrWhiteSpace(testFile.FileName));

                    var extension = compressing ? LoggingSource.FullCompressExtension : LoggingSource.LogExtension;
                    testFile.FileName = $"{LoggingSource.DateToLogFormat(testFile.CreationTime.ToDateTime())}.000{extension}";

                    CreateTestFile(path, testFile);
                }

                // Initialize the document store and execute the download logs command
                using (var store = GetDocumentStore(new Options { Server = server }))
                using (var commands = store.Commands())
                {
                    DateTime? startDate = string.IsNullOrWhiteSpace(startDateStr) ? null : startDateStr.ToDateTime();
                    DateTime? endDate = string.IsNullOrWhiteSpace(endDateStr) ? null : endDateStr.ToDateTime();

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
                                sb.Append(startDateStr.ToDebugDate());
                                sb.Append("' to '");
                                sb.Append(endDateStr.ToDebugDate());
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
                                        $"CreationTime: {LoggingSource.GetLogFileCreationTime(pathToFile, DateTimeKind.Utc)} 'UTC', " +
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
                                        $"CreationTime: {testFile.CreationTime.ToDateTime().ToUniversalTime()} 'UTC', " +
                                        $"LastWriteTime: {testFile.LastWriteTime.ToDateTime().ToUniversalTime()} 'UTC', " +
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

                            Assert.Equal($"No log files were found that matched the specified date range from '{startDateStr.ToDebugDate()}' to '{endDateStr.ToDebugDate()}'.", content);
                        }
                    }
                }
            }
            finally
            {
                Directory.Delete(path, recursive: true);
            }
        }

        private static void CreateTestFile(string path, TestFile testFile)
        {
            string filePath = Path.Combine(path, testFile.FileName);
            var logEntryTime = testFile.CreationTime.ToDateTime();

            File.WriteAllText(filePath, contents: $"""
                                                   Time, Thread, Level, Source, Logger, Message, Exception
                                                   {logEntryTime.GetDefaultRavenFormat()}, 1, Operations, Server, Raven.Server.Program
                                                   """);

            File.SetLastWriteTime(filePath, testFile.LastWriteTime.ToDateTime());
        }

        private class TestFile
        {
            internal string FileName;
            internal string CreationTime;
            internal string LastWriteTime;
            internal bool ShouldBeIncluded;
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

        private void AssertNoFileMissing(string[] logFiles)
        {
            Assert.NotEmpty(logFiles);

            var exceptions = new List<Exception>();

            var logMetadataList = GetLogMetadataOrderedByDateThenByNumber(logFiles, exceptions);

            for (var i = 1; i < logMetadataList.Length; i++)
            {
                var previousLogMetadata = logMetadataList[i - 1];
                var currentLogMetadata = logMetadataList[i];

                // If the dates or numbers of the current and previous logs are not sequential, skip to the next iteration
                if (previousLogMetadata.CreationTime != currentLogMetadata.CreationTime || previousLogMetadata.Number + 1 == currentLogMetadata.Number)
                    continue;

                // If the numbers of the current and previous logs are the same
                if (previousLogMetadata.Number == currentLogMetadata.Number)
                {
                    var previousLogExtension = Path.GetExtension(previousLogMetadata.FilePath);
                    var currentLogExtension = Path.GetExtension(currentLogMetadata.FilePath);

                    // Check if one file is a log and the other is a compressed log
                    bool isOneLogAndOneCompressed =
                        (previousLogExtension == LoggingSource.AdditionalCompressExtension && currentLogExtension == LoggingSource.LogExtension) ||
                        (previousLogExtension == LoggingSource.LogExtension && currentLogExtension == LoggingSource.AdditionalCompressExtension);

                    // If the condition is met, skip the current iteration
                    if (isOneLogAndOneCompressed)
                        continue;
                }

                // If none of the above conditions are met, add an exception indicating a missing log file
                exceptions.Add(new Exception($"Log between {previousLogMetadata.FilePath} and {currentLogMetadata.FilePath} is missing"));
            }

            if (exceptions.Any())
            {
                var allLogFilesAsString = JustFileNamesAsString(logFiles);
                throw new AggregateException($"All logs:{Environment.NewLine}{allLogFilesAsString}", exceptions);
            }
        }

        private static string JustFileNamesAsString(string[] files)
        {
            var justFileNames = files.Select(Path.GetFileName);
            var logsAroundError = string.Join($", {Environment.NewLine}", justFileNames);
            return logsAroundError;
        }

        private static LogMetaData[] GetLogMetadataOrderedByDateThenByNumber(string[] filePaths, List<Exception> exceptions)
        {
            return filePaths.Select(filePath =>
                {
                    if (LoggingSource.TryGetLogFileNumber(filePath, out var number) == false)
                    {
                        exceptions.Add(new Exception($"Unable to get log number from {filePath}"));
                        return null;
                    }

                    if (LoggingSource.TryGetCreationTimeLocal(filePath, out var creationTime) ||
                        LoggingSource.TryGetCreationTimeLocal($"{filePath}{LoggingSource.AdditionalCompressExtension}", out creationTime))
                        return new LogMetaData
                        {
                            FilePath = filePath,
                            CreationTime = creationTime,
                            Number = number
                        };

                    exceptions.Add(new Exception($"Unable to get creation time from {filePath}"));
                    return null;

                })
                .Where(logMetaData => logMetaData != null)
                .OrderBy(logMetaData => logMetaData.CreationTime)
                .ThenBy(logMetaData => logMetaData.Number)
                .ToArray();
        }

        private class LogMetaData
        {
            public string FilePath { set; get; }
            public DateTime CreationTime { set; get; }
            public int Number { set; get; }
        }
    }

    internal static class LoggingSourceTestExtensions
    {
        internal static DateTime ToDateTime(this string date, string format = "dd-MM-yyyy HH:mm") =>
            DateTime.ParseExact(date, format, CultureInfo.InvariantCulture);

        internal static DateTime ToUniversalTime(this string date, string format = "dd-MM-yyyy HH:mm") =>
            date.ToDateTime(format).ToUniversalTime();

        internal static string ToDebugDate(this string date, string format = "dd-MM-yyyy HH:mm") =>
            date == null
                ? "not specified"
                : date
                    .ToUniversalTime(format)
                    .ToString("yyyy-MM-ddTHH:mm:ss.fffffff 'UTC'", CultureInfo.InvariantCulture);
    }
}

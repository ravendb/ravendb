using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Drawing;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using FastTests;
using Sparrow.Logging;
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
        public async Task LoggingSource_WhileRetentionByTimeOn_ShouldKeepRetentionPolicy(bool compressing)
        {
            const int fileSize = Constants.Size.Kilobyte;

            var name = GetTestName();
            var path = NewDataPath(forceCreateDir: true);
            path = Path.Combine(path, Guid.NewGuid().ToString());
            Directory.CreateDirectory(path);
            var retentionTime = TimeSpan.FromDays(3);

            var retentionDate = DateTime.Now.Date - retentionTime;
            var toCheckLogFiles = new List<(string, bool)>();
            for (var date = retentionDate - TimeSpan.FromDays(1); date <= retentionDate + TimeSpan.FromDays(1); date += TimeSpan.FromDays(1))
            {
                var fileName = Path.Combine(path, LoggingSource.LogInfo.GetFileName(date) + ".001.log");
                toCheckLogFiles.Add((fileName, date >= retentionDate));
                var file = File.Create(fileName);
                file.Dispose();
            }

            var retentionSize = long.MaxValue;
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

            for (var j = 0; j < 50; j++)
            {
                for (var i = 0; i < 5; i++)
                {
                    await logger.OperationsAsync("Some message");
                }
                Thread.Sleep(10);
            }

            loggingSource.EndLogging();

            var afterEndFiles = Directory.GetFiles(path);
            AssertNoFileMissing(afterEndFiles);

            try
            {
                foreach (var (fileName, shouldExist) in toCheckLogFiles)
                {
                    var compressedFileName = fileName + ".gz";
                    if (shouldExist)
                    {
                        Assert.True(afterEndFiles.Contains(fileName) || afterEndFiles.Contains(compressedFileName),
                            $"The log file \"{Path.GetFileNameWithoutExtension(fileName)}\" and all log files from and after {retentionDate} " +
                            "should be deleted due to time retention");
                    }
                    else
                    {
                        Assert.False(afterEndFiles.Contains(fileName),
                            $"The file \"{fileName}\" and all log files from before {retentionDate} should be deleted due to time retention");

                        Assert.False(afterEndFiles.Contains(compressedFileName),
                            $"The file \"{compressedFileName}\" and all log files from before {retentionDate} should be deleted due to time retention");
                    }
                }
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
            const int retentionSize = 5 * Constants.Size.Kilobyte;

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
                    await logger.OperationsAsync("Some message");
                }
                Thread.Sleep(10);
            }

            var sw = Stopwatch.StartNew();
            while (true)
            {
                await Task.Delay(TimeSpan.FromSeconds(1));
                
                var afterEndFiles = Directory.GetFiles(path);
                AssertNoFileMissing(afterEndFiles);
                var size = afterEndFiles.Select(f => new FileInfo(f)).Sum(f => f.Length);
                const int threshold = 2 * fileSize;

                if (Math.Abs(size - retentionSize) <= threshold)
                    break;
                    
                if(sw.Elapsed < TimeSpan.FromSeconds(10))
                    continue;
                
                Assert.True(false,
                    $"ActualSize({size}), retentionSize({retentionSize}), threshold({threshold})" +
                    Environment.NewLine +
                    JustFileNamesAsString(afterEndFiles));
            }
            
            loggingSource.EndLogging();
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
                await logger.OperationsAsync("Some message");
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
                    var task = logger.OperationsAsync("Some message");
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
                        var task = secondLogger.OperationsAsync("Some message");
                        Task.WhenAny(task, Task.Delay(taskTimeout)).GetAwaiter().GetResult();
                        if (task.IsCompleted == false)
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

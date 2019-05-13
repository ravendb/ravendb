using System;
using System.Collections.Generic;
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

namespace SlowTests.SparrowTests
{
    public class LoggingSourceTests : RavenTestBase
    {
        [Theory]
        [InlineData(true)]
        [InlineData(false)]
        public async Task LoggingSource_WhileRetentionByTimeOn_ShouldKeepRetentionPolicy(bool compressing)
        {
            const int fileSize = Constants.Size.Kilobyte;

            var name = GetTestName();

            var path = NewDataPath(forceCreateDir: true);
            var loggingSource = new LoggingSource(
                LogMode.Information,
                path,
                "LoggingSource" + name,
                TimeSpan.FromSeconds(30),
                long.MaxValue,
                compressing);

            loggingSource.MaxFileSizeInBytes = fileSize;
            var logger = new Logger(loggingSource, "Source" + name, "Logger" + name);

            for (int j = 0; j < 50; j++)
            {
                for (var i = 0; i < 5; i++)
                {
                    await logger.OperationsAsync("Some message");
                }
                await Task.Delay(TimeSpan.FromSeconds(1));
            }

            loggingSource.EndLogging();

            var afterEndFiles = Directory.GetFiles(path);
            AssertNoFileMissing(afterEndFiles);
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
            var loggingSource = new LoggingSource(
                LogMode.Information,
                path,
                "LoggingSource" + name,
                TimeSpan.MaxValue,
                retentionSize,
                compressing);

            loggingSource.MaxFileSizeInBytes = fileSize;
            var logger = new Logger(loggingSource, "Source" + name, "Logger" + name);

            for (var i = 0; i < 500; i++)
            {
                await logger.OperationsAsync("Some message");
            }

            loggingSource.EndLogging();

            var afterEndFiles = Directory.GetFiles(path);
            AssertNoFileMissing(afterEndFiles);
            var size = afterEndFiles.Select(f => new FileInfo(f)).Sum(f => f.Length);
            var threshold = 2 * fileSize;
            Assert.True(Math.Abs(size - retentionSize) < threshold, $"ActualSize({size}), retentionSize({retentionSize}), threshold({threshold})");
        }

        [Theory]
        [InlineData(true)]
        [InlineData(false)]
        public async Task LoggingSource_WhileLogging_ShouldNotLoseLogFile(bool compressing)
        {
            var name = GetTestName();

            var path = NewDataPath(forceCreateDir: true);
            var loggingSource = new LoggingSource(
                LogMode.Information,
                path,
                "LoggingSource" + name,
                TimeSpan.MaxValue,
                long.MaxValue,
                compressing);

            loggingSource.MaxFileSizeInBytes = 1024;
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
            var firstLoggingSource = new LoggingSource(
                LogMode.Information,
                path,
                "FirstLoggingSource" + name,
                TimeSpan.MaxValue,
                long.MaxValue,
                compressing);

            try
            {
                firstLoggingSource.MaxFileSizeInBytes = 1024;
                var logger = new Logger(firstLoggingSource, "Source" + name, "Logger" + name);

                for (var i = 0; i < 100; i++)
                {
                    var task = logger.OperationsAsync("Some message");
                    await Task.WhenAny(task, Task.Delay(taskTimeout));
                    if(task.IsCompleted == false)
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
                    TimeSpan.MaxValue,
                    long.MaxValue);

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
                catch(Exception e)
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

            foreach (var file in beforeRestartFiles.SkipLast(1)) //The last is skipped because it is still written
            {
                var lastWriteTime = File.GetLastWriteTime(file);
                Assert.True(
                    restartDateTime > lastWriteTime, 
                    $"{file} was changed (time:" +
                    $"{lastWriteTime.ToString("yyyy-MM-dd HH:mm:ss.fff", CultureInfo.InvariantCulture)}) after the restart (time:" +
                    $"{restartDateTime.ToString("yyyy-MM-dd HH:mm:ss.fff", CultureInfo.InvariantCulture)})");
            }
        }

        private static string GetTestName([CallerMemberName] string memberName = "") => memberName;

        private void AssertNoFileMissing(string[] beforeEndFiles)
        {
            Assert.NotEmpty(beforeEndFiles);

            var exceptions = new List<Exception>();

            var list = GetLogMetadata(beforeEndFiles, exceptions);

            for (var i = 1; i < list.Length; i++)
            {
                var previous = list[i - 1];
                var current = list[i];
                if (previous.Date == current.Date && previous.No + 1 != current.No)
                    exceptions.Add(new Exception($"Log between {previous} nad {current} is missing"));
            }

            if (exceptions.Any())
            {
                throw new AggregateException(exceptions);
            }
        }

        private LogMetaData[] GetLogMetadata(string[] beforeEndFiles, List<Exception> exceptions)
        {
            var list = beforeEndFiles.Select(f =>
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
                        No = num
                    };
                })
                .Where(f => f != null)
                .OrderBy(f => f.Date)
                .ThenBy(f => f.No)
                .ToArray();

            return list;
        }

        private class LogMetaData
        {
            public string FileName { set; get; }
            public DateTime Date { set; get; }
            public int No { set; get; }
        }
    }
}

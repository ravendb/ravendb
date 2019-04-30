using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using FastTests;
using Sparrow.Logging;
using Xunit;

namespace SlowTests.SparrowTests
{
    public class LoggingSourceTests : RavenTestBase
    {
        [Fact]
        public async Task LoggingSource_WhileCompressing_ShouldNotLoseLogFile()
        {
            var name = GetTestName();

            var path = NewDataPath(forceCreateDir: true);
            var loggingSource = new LoggingSource(
                LogMode.Information,
                path,
                "LoggingSource" + name,
                TimeSpan.MaxValue,
                long.MaxValue);

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

        [Fact]
        public async Task LoggingSource_WhileStopAndStartAgain_ShouldNotOverrideOld()
        {
            var name = GetTestName();

            var path = NewDataPath(forceCreateDir: true);
            var firstLoggingSource = new LoggingSource(
                LogMode.Information,
                path,
                "FirstLoggingSource" + name,
                TimeSpan.MaxValue,
                long.MaxValue);

            firstLoggingSource.MaxFileSizeInBytes = 1024;
            var logger = new Logger(firstLoggingSource, "Source" + name, "Logger" + name);

            for (var i = 0; i < 100; i++)
            {
                await logger.OperationsAsync("Some message");
            }
            firstLoggingSource.EndLogging();

            var beforeRestartFiles = Directory.GetFiles(path);

            var restartDateTime = DateTime.Now;

            //To start new LoggingSource the object need to be construct on another thread
            var anotherThread = new Thread(() =>
            {
                var secondLoggingSource = new LoggingSource(
                    LogMode.Information,
                    path,
                    "SecondLoggingSource" + name,
                    TimeSpan.MaxValue,
                    long.MaxValue);

                secondLoggingSource.MaxFileSizeInBytes = 1024;
                var secondLogger = new Logger(secondLoggingSource, "Source" + name, "Logger" + name);

                for (var i = 0; i < 100; i++)
                {
                    secondLogger.OperationsAsync("Some message").GetAwaiter().GetResult();
                }
                secondLoggingSource.EndLogging();
            });
            anotherThread.Start();
            anotherThread.Join();

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
            var exceptions = new List<Exception>();

            var list = beforeEndFiles.Select(f =>
            {
                var inputSpan = f.Substring(0, "yyyy-MM-dd".Length);
                if (DateTime.TryParse(inputSpan, out var logDateTime) == false)
                {
                    exceptions.Add(new Exception($"{f} can't be parsed to date format"));
                    return null;
                }

                var snum = Path.GetFileNameWithoutExtension(f).Substring("yyyy-MM-dd".Length);
                if (int.TryParse(snum, out var num) == false)
                {
                    exceptions.Add(new Exception($"incremented number of {f} can't be parsed to int"));
                    return null;
                }

                return new
                {
                    FileName = f,
                    Date = logDateTime,
                    No = snum
                };
            })
                .Where(f => f != null)
                .OrderBy(f => f.Date + f.No)
                .ToArray();

            for (var i = 1; i < list.Length; i++)
            {
                var previous = list[i - 1];
                var current = list[i];
                if (previous.Date != current.Date && previous.No + 1 != current.No)
                {
                    exceptions.Add(new Exception($"Log between {previous} nad {current} is missing"));
                }
            }

            if (list.Any())
            {
                throw new AggregateException(exceptions);
            }
        }
    }
}

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Client.Documents.Queries;
using Raven.Client.Documents.Session;
using Raven.Server.Utils;
using Voron.Platform.Posix;
using Sparrow.Collections;
using Sparrow.Platform;
using Sparrow.Utils;
using Xunit;
using System.Text;
using Xunit.Sdk;
using ExceptionAggregator = Raven.Server.Utils.ExceptionAggregator;

namespace FastTests
{
    public static class RavenTestHelper
    {
        public static readonly bool IsRunningOnCI;

        public static readonly ParallelOptions DefaultParallelOptions = new ParallelOptions
        {
            MaxDegreeOfParallelism = ProcessorInfo.ProcessorCount * 2
        };

        static RavenTestHelper()
        {
            bool.TryParse(Environment.GetEnvironmentVariable("RAVEN_IS_RUNNING_ON_CI"), out IsRunningOnCI);
        }

        private static int _pathCount;

        public static string NewDataPath(string testName, int serverPort, bool forceCreateDir = false, string rootDir = null)
        {
            testName = testName?.Replace("<", "").Replace(">", "");

            var prefix = rootDir == null ? "." : $".\\{rootDir}-{Interlocked.Increment(ref _pathCount)}";

            var pathString = $"{prefix}\\Databases\\{testName ?? "TestDatabase"}.{serverPort}-{Interlocked.Increment(ref _pathCount)}";

            var newDataDir = Path.GetFullPath(pathString);

            if (PlatformDetails.RunningOnPosix)
                newDataDir = PosixHelper.FixLinuxPath(newDataDir);

            if (forceCreateDir && Directory.Exists(newDataDir) == false)
                Directory.CreateDirectory(newDataDir);

            return newDataDir;
        }

        public static string NewServerDataPath(string serverName, int serverPort)
        {
            var newDataDir = Path.GetFullPath($".\\{serverName}-{serverPort}.{Interlocked.Increment(ref _pathCount)}");

            if (PlatformDetails.RunningOnPosix)
                newDataDir = PosixHelper.FixLinuxPath(newDataDir);

            if (Directory.Exists(newDataDir) == false)
                Directory.CreateDirectory(newDataDir);

            return newDataDir;
        }

        public static string NewDatabaseDataPath(string serverPath, string testName, int serverPort, bool forceCreateDir = false)
        {
            testName = testName?.Replace("<", "").Replace(">", "");

            var pathString = $"{serverPath}\\Databases\\{testName ?? "TestDatabase"}.{serverPort}-{Interlocked.Increment(ref _pathCount)}";

            var newDataDir = Path.GetFullPath(pathString);

            if (PlatformDetails.RunningOnPosix)
                newDataDir = PosixHelper.FixLinuxPath(newDataDir);

            if (forceCreateDir && Directory.Exists(newDataDir) == false)
                Directory.CreateDirectory(newDataDir);

            return newDataDir;
        }

        public static void DeletePaths(ConcurrentSet<string> pathsToDelete, ExceptionAggregator exceptionAggregator)
        {
            var localPathsToDelete = pathsToDelete.OrderByDescending(x => x).ToArray();
            foreach (var pathToDelete in localPathsToDelete)
            {
                pathsToDelete.TryRemove(pathToDelete);

                FileAttributes pathAttributes;
                try
                {
                    pathAttributes = File.GetAttributes(pathToDelete);
                }
                catch (FileNotFoundException)
                {
                    continue;
                }
                catch (DirectoryNotFoundException)
                {
                    continue;
                }

                if (pathAttributes.HasFlag(FileAttributes.Directory))
                    exceptionAggregator.Execute(() => ClearDatabaseDirectory(pathToDelete));
                else
                    exceptionAggregator.Execute(() => IOExtensions.DeleteFile(pathToDelete));
            }
        }

        private static void ClearDatabaseDirectory(string dataDir)
        {
            var isRetry = false;

            while (true)
            {
                try
                {
                    IOExtensions.DeleteDirectory(dataDir);
                    break;
                }
                catch (IOException)
                {
                    if (isRetry)
                        throw;

                    GC.Collect();
                    GC.WaitForPendingFinalizers();
                    isRetry = true;

                    Thread.Sleep(200);
                }
            }
        }

        public static IndexQuery GetIndexQuery<T>(IQueryable<T> queryable)
        {
            var inspector = (IRavenQueryInspector)queryable;
            return inspector.GetIndexQuery(isAsync: false);
        }

        public static void AssertNoIndexErrors(IDocumentStore store, string databaseName = null)
        {
            var errors = store.Maintenance.ForDatabase(databaseName).Send(new GetIndexErrorsOperation());
            StringBuilder sb = null;
            foreach (var indexErrors in errors)
            {
                if (indexErrors == null || indexErrors.Errors == null || indexErrors.Errors.Length == 0)
                    continue;

                if (sb == null)
                    sb = new StringBuilder();

                sb.AppendLine($"Index Errors for '{indexErrors.Name}' ({indexErrors.Errors.Length})");
                foreach (var indexError in indexErrors.Errors)
                {
                    sb.AppendLine($"- {indexError}");
                }
                sb.AppendLine();
            }

            if (sb == null)
                return;

            throw new InvalidOperationException(sb.ToString());
        }

        public static void AssertEqualRespectingNewLines(string expected, string actual)
        {
            var converted = ConvertRespectingNewLines(expected);

            Assert.Equal(converted, actual);
        }

        public static void AssertStartsWithRespectingNewLines(string expected, string actual)
        {
            var converted = ConvertRespectingNewLines(expected);

            Assert.StartsWith(converted, actual);
        }

        public static void AreEquivalent<T>(IEnumerable<T> expected, IEnumerable<T> actual)
        {
            var forMonitor = actual.ToList();
            Assert.All(expected, e =>
            {
                Assert.Contains(e, forMonitor);
                forMonitor.Remove(e);
            });
            Assert.Empty(forMonitor);
        }

        public static void AssertAll(Func<string> massageFactory, params Action[] asserts)
        {
            try
            {
                Assert.All(asserts, assert => assert());
            }
            catch (Exception e)
            {
                throw new XunitException(massageFactory() + Environment.NewLine + e.Message);
            }
        }

        public static void AssertAll(params Action[] asserts)
        {
            Assert.All(asserts, assert => assert());
        }
        public static async Task AssertAllAsync(Func<Task<string>> massageFactory, params Action[] asserts)
        {
            try
            {
                Assert.All(asserts, assert => assert());
            }
            catch (Exception e)
            {
                throw new XunitException(await massageFactory() + Environment.NewLine + e.Message);
            }
        }
        
        private static string ConvertRespectingNewLines(string toConvert)
        {
            if (string.IsNullOrEmpty(toConvert))
                return toConvert;

            var regex = new Regex("\r*\n");
            return regex.Replace(toConvert, Environment.NewLine);
        }

        public static DateTime UtcToday
        {
            get
            {
                var local = DateTime.Today;
                return DateTime.SpecifyKind(local, DateTimeKind.Utc);
            }
        }

        public class DateTimeComparer : IEqualityComparer<DateTime>
        {
            public static readonly DateTimeComparer Instance = new DateTimeComparer();
            public bool Equals(DateTime x, DateTime y)
            {
                if (x.Kind == DateTimeKind.Local)
                    x = x.ToUniversalTime();

                if (y.Kind == DateTimeKind.Local)
                    y = y.ToUniversalTime();

                return x == y;
            }

            public int GetHashCode(DateTime obj)
            {
                return obj.GetHashCode();
            }
        }
    }
}

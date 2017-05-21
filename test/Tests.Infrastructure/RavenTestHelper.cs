using System;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Raven.Server.Extensions;
using Raven.Server.Utils;
using Voron.Platform.Posix;

using Sparrow;
using Sparrow.Collections;
using Sparrow.Platform;
using Sparrow.Utils;

namespace FastTests
{
    public static class RavenTestHelper
    {
        public static ParallelOptions DefaultParallelOptions = new ParallelOptions
        {
            MaxDegreeOfParallelism = ProcessorInfo.ProcessorCount * 2
        };

        private static int _pathCount;

        public static string NewDataPath(string testName, int serverPort, bool forceCreateDir = false)
        {
            testName = testName?.Replace("<", "").Replace(">", "");

            var newDataDir = Path.GetFullPath($".\\Databases\\{testName ?? "TestDatabase"}_{serverPort}-{DateTime.Now.ToString("yyyy-MM-dd-HH-mm-ss-fff")}-{Interlocked.Increment(ref _pathCount)}");

            if (PlatformDetails.RunningOnPosix)
                newDataDir = PosixHelper.FixLinuxPath(newDataDir);

            if (forceCreateDir && Directory.Exists(newDataDir) == false)
                Directory.CreateDirectory(newDataDir);

            return newDataDir;
        }

        public static void DeletePaths(ConcurrentSet<string> pathsToDelete, ExceptionAggregator exceptionAggregator)
        {
            var localPathsToDelete = pathsToDelete.ToArray();
            foreach (var pathToDelete in localPathsToDelete)
            {
                pathsToDelete.TryRemove(pathToDelete);
                exceptionAggregator.Execute(() => ClearDatabaseDirectory(pathToDelete));
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
    }
}
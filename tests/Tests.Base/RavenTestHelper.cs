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

namespace NewClientTests
{
    public static class RavenTestHelper
    {
        public static ParallelOptions DefaultParallelOptions = new ParallelOptions
        {
            MaxDegreeOfParallelism = Environment.ProcessorCount * 2
        };

        private static int _pathCount;

        public static string NewDataPath(string testName, int serverPort, bool forceCreateDir = false)
        {
            testName = testName?.Replace("<", "").Replace(">", "");

            var newDataDir = Path.GetFullPath($".\\Databases\\{testName ?? "TestDatabase"}_{serverPort}-{DateTime.Now.ToString("yyyy-MM-dd-HH-mm-ss-fff")}-{Interlocked.Increment(ref _pathCount)}");

            if (Platform.RunningOnPosix)
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

                if (File.Exists(pathToDelete))
                {
                    exceptionAggregator.Execute(() =>
                    {
                        throw new IOException(string.Format("We tried to delete the '{0}' directory, but failed because it is a file.\r\n{1}", pathToDelete, WhoIsLocking.ThisFile(pathToDelete)));
                    });
                }
                else if (Directory.Exists(pathToDelete))
                {
                    exceptionAggregator.Execute(() =>
                    {
                        string filePath;
                        try
                        {
                            filePath = Directory.GetFiles(pathToDelete, "*", SearchOption.AllDirectories).FirstOrDefault() ?? pathToDelete;
                        }
                        catch (Exception)
                        {
                            filePath = pathToDelete;
                        }

                        throw new IOException(string.Format("We tried to delete the '{0}' directory.\r\n{1}", pathToDelete, WhoIsLocking.ThisFile(filePath)));
                    });
                }
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

                    Thread.Sleep(2500);
                }
            }
        }
    }
}
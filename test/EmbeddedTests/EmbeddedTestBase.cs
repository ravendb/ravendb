using System;
using System.IO;
using System.Runtime.CompilerServices;
using System.Threading;
using EmbeddedTests.Platform;
using Sparrow.Collections;

namespace EmbeddedTests
{
    public abstract class EmbeddedTestBase : IDisposable
    {
        private static int _pathCount;

        private readonly ConcurrentSet<string> _localPathsToDelete = new ConcurrentSet<string>(StringComparer.OrdinalIgnoreCase);

        protected string NewDataPath([CallerMemberName] string caller = null)
        {
            var path = $".\\Databases\\{caller ?? "TestPath"}.{Interlocked.Increment(ref _pathCount)}";
            if (PosixHelper.RunningOnPosix)
                path = PosixHelper.FixLinuxPath(path);

            path = Path.GetFullPath(path);
            _localPathsToDelete.Add(path);

            return path;
        }

        public virtual void Dispose()
        {
            foreach (var path in _localPathsToDelete)
            {
                var directoryInfo = new DirectoryInfo(path);
                if (directoryInfo.Exists == false)
                    continue;

                directoryInfo.Delete(recursive: true);
            }
        }
    }
}

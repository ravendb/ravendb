using System;
using System.IO;
using System.Runtime.CompilerServices;
using System.Threading;
using Sparrow.Collections;

namespace EmbeddedTests
{
    public abstract class EmbeddedTestBase : IDisposable
    {
        private static int _pathCount;

        private readonly ConcurrentSet<string> _localPathsToDelete = new ConcurrentSet<string>(StringComparer.OrdinalIgnoreCase);

        protected string NewDataPath([CallerMemberName] string caller = null)
        {
            var path = Path.GetFullPath($".\\Databases\\{caller ?? "TestPath"}.{Interlocked.Increment(ref _pathCount)}");
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

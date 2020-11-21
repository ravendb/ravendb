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

        protected (string ServerDirectory, string DataDirectory) CopyServer()
        {
            var baseDirectory = NewDataPath();
            var serverDirectory = Path.Combine(baseDirectory, "RavenDBServer");
            var dataDirectory = Path.Combine(baseDirectory, "RavenDB");

            if (Directory.Exists(serverDirectory) == false)
                Directory.CreateDirectory(serverDirectory);

            if (Directory.Exists(dataDirectory) == false)
                Directory.CreateDirectory(dataDirectory);

#if DEBUG
            var runtimeConfigPath = @"../../../../../src/Raven.Server/bin/x64/Debug/netcoreapp3.1/Raven.Server.runtimeconfig.json";
            if (File.Exists(runtimeConfigPath) == false) // this can happen when running directly from CLI e.g. dotnet xunit
                runtimeConfigPath = @"../../../../../src/Raven.Server/bin/Debug/netcoreapp3.1/Raven.Server.runtimeconfig.json";
#else
                var runtimeConfigPath = @"../../../../../src/Raven.Server/bin/x64/Release/netcoreapp3.1/Raven.Server.runtimeconfig.json";
                if (File.Exists(runtimeConfigPath) == false) // this can happen when running directly from CLI e.g. dotnet xunit
                    runtimeConfigPath = @"../../../../../src/Raven.Server/bin/Release/netcoreapp3.1/Raven.Server.runtimeconfig.json";
#endif

            var runtimeConfigFileInfo = new FileInfo(runtimeConfigPath);
            if (runtimeConfigFileInfo.Exists == false)
                throw new FileNotFoundException("Could not find runtime config", runtimeConfigPath);

            File.Copy(runtimeConfigPath, Path.Combine(serverDirectory, runtimeConfigFileInfo.Name), true);

            foreach (var extension in new[] { "*.dll", "*.so", "*.dylib", "*.deps.json" })
            {
                foreach (var file in Directory.GetFiles(runtimeConfigFileInfo.DirectoryName, extension))
                {
                    var fileInfo = new FileInfo(file);
                    File.Copy(file, Path.Combine(serverDirectory, fileInfo.Name), true);
                }
            }

            var runtimesSource = Path.Combine(runtimeConfigFileInfo.DirectoryName, "runtimes");
            var runtimesDestination = Path.Combine(serverDirectory, "runtimes");

            foreach (string dirPath in Directory.GetDirectories(runtimesSource, "*",
                SearchOption.AllDirectories))
                Directory.CreateDirectory(dirPath.Replace(runtimesSource, runtimesDestination));

            foreach (string newPath in Directory.GetFiles(runtimesSource, "*.*",
                SearchOption.AllDirectories))
                File.Copy(newPath, newPath.Replace(runtimesSource, runtimesDestination), true);

            return (serverDirectory, dataDirectory);
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

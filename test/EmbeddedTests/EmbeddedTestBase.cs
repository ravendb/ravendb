using System;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Threading;
using EmbeddedTests.Platform;
using Raven.Embedded;
using Sparrow.Collections;
using Sparrow.Platform;

#pragma warning disable LOCAL0003

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

            path = ToFullPath(Path.GetFullPath(path));
            _localPathsToDelete.Add(path);

            return path;
        }

        protected (string ServerDirectory, string DataDirectory) CopyServer()
        {
            var baseDirectory = NewDataPath();
            var serverDirectory = ToFullPath(Path.Combine(baseDirectory, "RavenDBServer"));
            var dataDirectory = ToFullPath(Path.Combine(baseDirectory, "RavenDB"));

            if (Directory.Exists(serverDirectory) == false)
                Directory.CreateDirectory(serverDirectory);

            if (Directory.Exists(dataDirectory) == false)
                Directory.CreateDirectory(dataDirectory);

#if DEBUG
            var runtimeConfigPath = @"../../../../../src/Raven.Server/bin/x64/Debug/net8.0/Raven.Server.runtimeconfig.json";
            if (File.Exists(runtimeConfigPath) == false) // this can happen when running directly from CLI e.g. dotnet xunit
                runtimeConfigPath = @"../../../../../src/Raven.Server/bin/Debug/net8.0/Raven.Server.runtimeconfig.json";
#else
                var runtimeConfigPath = @"../../../../../src/Raven.Server/bin/x64/Release/net8.0/Raven.Server.runtimeconfig.json";
                if (File.Exists(runtimeConfigPath) == false) // this can happen when running directly from CLI e.g. dotnet xunit
                    runtimeConfigPath = @"../../../../../src/Raven.Server/bin/Release/net8.0/Raven.Server.runtimeconfig.json";
#endif

            var runtimeConfigFileInfo = new FileInfo(runtimeConfigPath);
            if (runtimeConfigFileInfo.Exists == false)
                throw new FileNotFoundException("Could not find runtime config", runtimeConfigPath);

            File.Copy(runtimeConfigPath, ToFullPath(Path.Combine(serverDirectory, runtimeConfigFileInfo.Name)), true);

            foreach (var extension in new[] { "*.dll", "*.so", "*.dylib", "*.deps.json" })
            {
                foreach (var file in Directory.GetFiles(runtimeConfigFileInfo.DirectoryName, extension).Select(x => ToFullPath(x)))
                {
                    var fileInfo = new FileInfo(file);
                    File.Copy(file, ToFullPath(Path.Combine(serverDirectory, fileInfo.Name)), true);
                }
            }

            var runtimesSource = Path.Combine(runtimeConfigFileInfo.DirectoryName, "runtimes");
            var runtimesDestination = Path.Combine(serverDirectory, "runtimes");

            foreach (string dirPath in Directory.GetDirectories(runtimesSource, "*",
                SearchOption.AllDirectories))
                Directory.CreateDirectory(ToFullPath(dirPath.Replace(runtimesSource, runtimesDestination)));

            foreach (string newPath in Directory.GetFiles(runtimesSource, "*.*",
                SearchOption.AllDirectories).Select(x => ToFullPath(x)))
                File.Copy(newPath, ToFullPath(newPath.Replace(runtimesSource, runtimesDestination)), true);

            return (serverDirectory, dataDirectory);
        }

        protected ServerOptions CopyServerAndCreateOptions()
        {
            var (severDirectory, dataDirectory) = CopyServer();
            return new ServerOptions { ServerDirectory = severDirectory, DataDirectory = dataDirectory, LogsPath = Path.Combine(severDirectory, "Logs") };
        }

        public virtual void Dispose()
        {
            foreach (var path in _localPathsToDelete)
            {
                var directoryInfo = new DirectoryInfo(path);
                if (directoryInfo.Exists == false)
                    continue;

                DeleteAllFilesAndSubfolders(directoryInfo);
            }
        }

        private void DeleteAllFilesAndSubfolders(DirectoryInfo directoryInfo)
        {
            // Delete all files in the current directory
            foreach (var p in directoryInfo.GetFiles().Select(x => ToFullPath(x.FullName)))
            {
                File.Delete(p);
            }

            // Recursively delete all subdirectories
            foreach (var subdirectory in directoryInfo.GetDirectories())
            {
                DeleteAllFilesAndSubfolders(subdirectory);
                Directory.Delete(ToFullPath(subdirectory.FullName));
            }
        }

        private static string ToFullPath(string inputPath, string baseDataDirFullPath = null)
        {
            var path = Environment.ExpandEnvironmentVariables(inputPath);

            if (PlatformDetails.RunningOnPosix == false && path.StartsWith(@"\") == false ||
                PlatformDetails.RunningOnPosix && path.StartsWith(@"/") == false) // if relative path
                path = Path.Combine(baseDataDirFullPath ?? AppContext.BaseDirectory, path);

            var result = Path.IsPathRooted(path)
                ? path
                : Path.Combine(baseDataDirFullPath ?? AppContext.BaseDirectory, path);

            if (result.Length >= 260 &&
                RuntimeInformation.IsOSPlatform(OSPlatform.Windows) &&
                result.StartsWith(@"\\?\") == false)
                result = @"\\?\" + result;

            var resultRoot = Path.GetPathRoot(result);
            if (resultRoot != result && (result.EndsWith(@"\") || result.EndsWith("/")))
                result = result.TrimEnd('\\', '/');

            if (PlatformDetails.RunningOnPosix)
                result = PosixHelper.FixLinuxPath(result);

            return result != string.Empty || resultRoot == null
                ? Path.GetFullPath(result)
                : Path.GetFullPath(resultRoot); // it will unify directory separators and sort out parent directories
        }
    }
}

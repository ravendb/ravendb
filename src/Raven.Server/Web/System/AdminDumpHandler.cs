using System;
using System.Diagnostics;
using System.IO;
using System.IO.Compression;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading.Tasks;
using Raven.Client.Properties;
using Raven.Server.Rachis;
using Raven.Server.Routing;
using Raven.Server.Utils;
using Sparrow.Platform;
using Sparrow.Utils;

namespace Raven.Server.Web.System
{
    public class AdminDumpHandler : ServerRequestHandler
    {
        [RavenAction("/admin/debug/dump", "GET", AuthorizationStatus.ClusterAdmin)]
        public async Task Dump()
        {
            var typeAsString = GetStringQueryString("type");
            if (Enum.TryParse(typeAsString, out DumpType type) == false)
                throw new InvalidOperationException($"Could not parse '{typeAsString}' to dump type.");

            var path = GetStringQueryString("path", required: false);
            if (string.IsNullOrWhiteSpace(path))
                path = Path.GetTempPath();

            using (var process = Process.GetCurrentProcess())
            {
                var fileNames = GetFileNames(".dmp");
                path = Path.Combine(path, fileNames.FileName);
                HttpContext.Response.RegisterForDispose(new DeleteFile(path));

                Execute($"dump --type {type}", process.Id, path);

                await using (var file = File.OpenRead(path))
                await using (var gzipStream = new GZipStream(ResponseBodyStream(), CompressionMode.Compress))
                {
                    HttpContext.Response.Headers["Content-Disposition"] = "attachment; filename=" + Uri.EscapeDataString(fileNames.GzipFileName);

                    await file.CopyToAsync(gzipStream);
                }
            }
        }

        [RavenAction("/admin/debug/gcdump", "GET", AuthorizationStatus.ClusterAdmin)]
        public async Task GcDump()
        {
            var timeout = GetIntValueQueryString("timeout", required: false) ?? 30;

            var path = GetStringQueryString("path", required: false);
            if (string.IsNullOrWhiteSpace(path))
                path = Path.GetTempPath();

            using (var process = Process.GetCurrentProcess())
            {
                var fileNames = GetFileNames(".gcdump");
                path = Path.Combine(path, fileNames.FileName);
                HttpContext.Response.RegisterForDispose(new DeleteFile(path));

                Execute($"gcdump --timeout {timeout}", process.Id, path);

                await using (var file = File.OpenRead(path))
                await using (var gzipStream = new GZipStream(ResponseBodyStream(), CompressionMode.Compress))
                {
                    HttpContext.Response.Headers["Content-Disposition"] = "attachment; filename=" + Uri.EscapeDataString(fileNames.GzipFileName);

                    await file.CopyToAsync(gzipStream);
                }
            }
        }

        [MethodImpl(MethodImplOptions.Synchronized)]
        private static void Execute(string args, int processId, string output)
        {
            var ravenDebugExec = Path.Combine(AppContext.BaseDirectory,
                PlatformDetails.RunningOnPosix ? "Raven.Debug" : "Raven.Debug.exe"
            );

            if (File.Exists(ravenDebugExec) == false)
                throw new FileNotFoundException($"Could not find debugger tool at '{ravenDebugExec}'");

            var sb = new StringBuilder($"{args} --pid {processId} --output {CommandLineArgumentEscaper.EscapeSingleArg(output)}");

            var startup = new ProcessStartInfo
            {
                Arguments = sb.ToString(),
                FileName = ravenDebugExec,
                WindowStyle = ProcessWindowStyle.Normal,
                RedirectStandardError = true,
                RedirectStandardOutput = true,
                RedirectStandardInput = true,
                UseShellExecute = false
            };

            if (PlatformDetails.RunningOnPosix == false)
            {
#pragma warning disable CA1416 // Validate platform compatibility
                startup.LoadUserProfile = false;
#pragma warning restore CA1416 // Validate platform compatibility
            }

            var process = new Process
            {
                StartInfo = startup,
                EnableRaisingEvents = true
            };

            sb.Clear();
            process.ErrorDataReceived += (sender, args) => sb.Append(args.Data);

            process.Start();

            process.BeginErrorReadLine();

            process.WaitForExit();

            if (process.ExitCode != 0)
                throw new InvalidOperationException($"Could not read stack traces, exit code: {process.ExitCode}, error: {sb}");

            AssertOutputExists(output, ravenDebugExec, sb.ToString());
        }

        private static void AssertOutputExists(string filePath, string ravenDebugExecPath, string ravenDebugExecOutput)
        {
            if (File.Exists(filePath))
                return;

            string desc;
            if (PlatformDetails.RunningOnLinux == false)
                desc = string.Empty;
            else
            {
                var createDumpExecPath = Path.Combine(AppContext.BaseDirectory, "createdump");

                desc = $"Make sure to run enable-debugging.sh script as root from the main RavenDB directory.";
            }

            throw new InvalidOperationException(
                $"Raven.Debug execution failed.{Environment.NewLine}" +
                desc + Environment.NewLine +
                $"{ravenDebugExecOutput}");
        }

        private (string FileName, string GzipFileName) GetFileNames(string extension)
        {
            var nodeTag = ServerStore.NodeTag == RachisConsensus.InitialTag
                ? "Unknown"
                : ServerStore.NodeTag;

            var platform = PlatformDetails.RunningOnPosix
                ? ".linux"
                : ".win";

            if (PlatformDetails.Is32Bits)
                platform += "-x86";
            else
                platform += "-x64";

            var fileWithoutExtension = $"RavenDB_{RavenVersionAttribute.Instance.BuildVersion}_{nodeTag}_{DateTime.Now.ToString("ddMMyyyy_HHmmss")}{platform}";
            var fileName = fileWithoutExtension + extension;
            var gzipFileName = fileName + ".gz";

            return (fileName, gzipFileName);
        }

        private class DeleteFile : IDisposable
        {
            private readonly string _path;

            public DeleteFile(string path)
            {
                if (path == null)
                    throw new ArgumentNullException(nameof(path));

                _path = path;
            }

            public void Dispose()
            {
                if (File.Exists(_path))
                    IOExtensions.DeleteFile(_path);
            }
        }

        private enum DumpType
        {
            Mini,
            Heap
        }
    }
}

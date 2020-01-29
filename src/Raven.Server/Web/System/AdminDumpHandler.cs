using System;
using System.Diagnostics;
using System.IO;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading.Tasks;
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
                var extension = PlatformDetails.RunningOnPosix == false ? ".dmp" : string.Empty;
                string fileName = GetFileName(extension);
                path = Path.Combine(path, fileName);
                HttpContext.Response.RegisterForDispose(new DeleteFile(path));

                Execute($"dump --type {type}", process.Id, path);

                using (var file = File.OpenRead(path))
                {
                    HttpContext.Response.Headers["Content-Disposition"] = "attachment; filename=" + Uri.EscapeDataString(fileName);

                    using (var stream = ResponseBodyStream())
                        await file.CopyToAsync(stream);
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
                string fileName = GetFileName(".gcdump");
                path = Path.Combine(path, fileName);
                HttpContext.Response.RegisterForDispose(new DeleteFile(path));

                Execute($"gcdump --timeout {timeout}", process.Id, path);

                using (var file = File.OpenRead(path))
                {
                    HttpContext.Response.Headers["Content-Disposition"] = "attachment; filename=" + Uri.EscapeDataString(fileName);

                    using (var stream = ResponseBodyStream())
                        await file.CopyToAsync(stream);
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
                startup.LoadUserProfile = false;

            var process = new Process
            {
                StartInfo = startup,
                EnableRaisingEvents = true
            };

            sb.Clear();
            process.ErrorDataReceived += (sender, args) => sb.Append(args.Data);

            process.Start();

            process.BeginErrorReadLine();
            process.BeginOutputReadLine();

            process.WaitForExit();

            if (process.ExitCode != 0)
                throw new InvalidOperationException($"Could not read stack traces, exit code: {process.ExitCode}, error: {sb}");
        }

        private string GetFileName(string extension)
        {
            var nodeTag = ServerStore.NodeTag == RachisConsensus.InitialTag
                ? "Unknown"
                : ServerStore.NodeTag;

            return $"RavenDB_Dump_{nodeTag}_{DateTime.Now.ToString("ddMMyyyy_HHmmss")}{extension}";
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

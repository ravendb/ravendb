using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Threading.Tasks;
using Sparrow.Platform;
using Sparrow.Utils;

namespace Raven.Embedded
{
    internal static class RavenServerRunner
    {
        public static async Task<Process> RunAsync(ServerOptions options)
        {
            if (string.IsNullOrWhiteSpace(options.ServerDirectory))
                throw new ArgumentNullException(nameof(options.ServerDirectory));

            if (string.IsNullOrWhiteSpace(options.DataDirectory))
                throw new ArgumentNullException(nameof(options.DataDirectory));

            if (string.IsNullOrWhiteSpace(options.LogsPath))
                throw new ArgumentNullException(nameof(options.LogsPath));

            (string exec, string fstArg) = GetExecAndFirstArgument(options);

            var commandLineArgs = new List<string>(options.CommandLineArgs);

            using (var currentProcess = Process.GetCurrentProcess())
            {
                commandLineArgs.Add($"--Embedded.ParentProcessId={currentProcess.Id}");
            }

            commandLineArgs.Add($"--License.Eula.Accepted={options.AcceptEula}");
            commandLineArgs.Add("--Setup.Mode=None");
            commandLineArgs.Add($"--DataDir={CommandLineArgumentEscaper.EscapeSingleArg(options.DataDirectory)}");
            commandLineArgs.Add($"--Logs.Path={CommandLineArgumentEscaper.EscapeSingleArg(options.LogsPath)}");

            if (options.Security != null)
            {
                if (string.IsNullOrWhiteSpace(options.ServerUrl))
                    options.ServerUrl = "https://127.0.0.1:0";

                if (options.Security.CertificatePath != null)
                {
                    commandLineArgs.Add($"--Security.Certificate.Path={CommandLineArgumentEscaper.EscapeSingleArg(options.Security.CertificatePath)}");
                    if (options.Security.CertificatePassword != null)
                        commandLineArgs.Add($"--Security.Certificate.Password={CommandLineArgumentEscaper.EscapeSingleArg(options.Security.CertificatePassword)}");
                }
                else
                {
                    commandLineArgs.Add($"--Security.Certificate.Load.Exec={CommandLineArgumentEscaper.EscapeSingleArg(options.Security.CertificateLoadExec)}");
                    commandLineArgs.Add($"--Security.Certificate.Load.Exec.Arguments={CommandLineArgumentEscaper.EscapeSingleArg(options.Security.CertificateLoadExecArguments)}");
                }
                commandLineArgs.Add($"--Security.WellKnownCertificates.Admin={CommandLineArgumentEscaper.EscapeSingleArg(options.Security.ClientCertificate.Thumbprint)}");
            }
            else
            {
                if (string.IsNullOrWhiteSpace(options.ServerUrl))
                    options.ServerUrl = "http://127.0.0.1:0";
            }

            commandLineArgs.Add($"--ServerUrl={options.ServerUrl}");

            if (fstArg != null)
            {
                commandLineArgs.Insert(0, CommandLineArgumentEscaper.EscapeSingleArg(fstArg));

                if (string.IsNullOrWhiteSpace(options.FrameworkVersion) == false)
                {
                    var frameworkVersion = await RuntimeFrameworkVersionMatcher.MatchAsync(options).ConfigureAwait(false);
                    commandLineArgs.Insert(0, $"--fx-version {frameworkVersion}");
                }
            }

            var argumentsString = string.Join(" ", commandLineArgs);

            var processStartInfo = new ProcessStartInfo
            {
                FileName = exec,
                Arguments = argumentsString,
                CreateNoWindow = true,
                RedirectStandardOutput = true,
                RedirectStandardError = true,
                RedirectStandardInput = true,
                UseShellExecute = false
            };

            RemoveEnvironmentVariables(processStartInfo);

            Process process = null;
            try
            {
                process = Process.Start(processStartInfo);
                process.EnableRaisingEvents = true;
            }
            catch (Exception e)
            {
                process?.Kill();
                throw new InvalidOperationException("Unable to execute server." + Environment.NewLine +
                                                    "Command was: " + Environment.NewLine +
                                                    (processStartInfo.WorkingDirectory ?? Directory.GetCurrentDirectory()) + "> "
                                                    + processStartInfo.FileName + " " + processStartInfo.Arguments, e);
            }

            return process;
        }

        private static (string Exec, string FirstArgument) GetExecAndFirstArgument(ServerOptions options)
        {
            string exec = Path.Combine(options.ServerDirectory,
                PlatformDetails.RunningOnPosix ? "Raven.Server" : "Raven.Server.exe"
            );

            if (File.Exists(exec))
                return (exec, null);

            var serverDllPath = Path.Combine(options.ServerDirectory, "Raven.Server.dll");
            var serverDllFound = File.Exists(serverDllPath);

            if (serverDllFound == false)
            {
                if (string.Equals(options.ServerDirectory, ServerOptions.DefaultServerDirectory, StringComparison.OrdinalIgnoreCase))
                {
                    // ASP.NET (not Core) AppContext.BaseDirectory is not in 'bin' folder
                    // but NuSpec contentFiles are extracting there
                    var aspNetServerDllPath = Path.Combine(ServerOptions.AltServerDirectory, "Raven.Server.dll");
                    if (File.Exists(aspNetServerDllPath))
                    {
                        serverDllFound = true;
                        serverDllPath = aspNetServerDllPath;
                    }
                }

                if (serverDllFound == false)
                    throw new FileNotFoundException("Server file was not found", serverDllPath);
            }

            if (string.IsNullOrWhiteSpace(options.DotNetPath))
                throw new ArgumentNullException(nameof(options.DotNetPath));

            return (options.DotNetPath, serverDllPath);
        }

        private static void RemoveEnvironmentVariables(ProcessStartInfo processStartInfo)
        {
            if (processStartInfo.Environment == null || processStartInfo.Environment.Count == 0)
                return;

            var variablesToRemove = new List<string>();
            foreach (var key in processStartInfo.Environment.Keys)
            {
                if (key == null)
                    continue;

                if (key.StartsWith("APP_POOL_", StringComparison.OrdinalIgnoreCase)
                    || key.StartsWith("ASPNETCORE_", StringComparison.OrdinalIgnoreCase)
                    || key.StartsWith("IIS_", StringComparison.OrdinalIgnoreCase))
                    variablesToRemove.Add(key);
            }

            foreach (var key in variablesToRemove)
                processStartInfo.Environment.Remove(key);
        }
    }
}

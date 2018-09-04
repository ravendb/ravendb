using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using Sparrow.Utils;

namespace Raven.Embedded
{
    internal class RavenServerRunner
    {
        public static Process Run(ServerOptions options)
        {
            if (string.IsNullOrWhiteSpace(options.ServerDirectory))
                throw new ArgumentNullException(nameof(options.ServerDirectory));

            if (string.IsNullOrWhiteSpace(options.DataDirectory))
                throw new ArgumentNullException(nameof(options.DataDirectory));

            var serverDllPath = Path.Combine(options.ServerDirectory, "Raven.Server.dll");

            if (File.Exists(serverDllPath) == false)
                throw new FileNotFoundException("Server file was not found", serverDllPath);

            if (string.IsNullOrWhiteSpace(options.DotNetPath))
                throw new ArgumentNullException(nameof(options.DotNetPath));

            using (var currentProcess = Process.GetCurrentProcess())
            {
                options.CommandLineArgs.Add($"--Embedded.ParentProcessId={currentProcess.Id}");
            }

            options.CommandLineArgs.Add($"--License.Eula.Accepted={options.AcceptEula}");
            options.CommandLineArgs.Add("--Setup.Mode=None");
            options.CommandLineArgs.Add($"--DataDir={CommandLineArgumentEscaper.EscapeSingleArg(options.DataDirectory)}");

            if (options.Security != null)
            {
                if (string.IsNullOrWhiteSpace(options.ServerUrl))
                    options.ServerUrl = "https://127.0.0.1:0";

                if (options.Security.CertificatePath != null)
                {
                    options.CommandLineArgs.Add($"--Security.Certificate.Path={CommandLineArgumentEscaper.EscapeSingleArg(options.Security.CertificatePath)}");
                    if (options.Security.CertificatePassword != null)
                        options.CommandLineArgs.Add($"--Security.Certificate.Password={CommandLineArgumentEscaper.EscapeSingleArg(options.Security.CertificatePassword)}");
                }
                else
                {
                    options.CommandLineArgs.Add($"--Security.Certificate.Exec={CommandLineArgumentEscaper.EscapeSingleArg(options.Security.CertificateExec)}");
                    options.CommandLineArgs.Add($"--Security.Certificate.Exec.Arguments={CommandLineArgumentEscaper.EscapeSingleArg(options.Security.CertificateArguments)}");
                }
                options.CommandLineArgs.Add($"--Security.WellKnownCertificates.Admin={CommandLineArgumentEscaper.EscapeSingleArg(options.Security.ClientCertificate.Thumbprint)}");
            }
            else
            {
                if (string.IsNullOrWhiteSpace(options.ServerUrl))
                    options.ServerUrl = "http://127.0.0.1:0";
            }

            options.CommandLineArgs.Add($"--ServerUrl={options.ServerUrl}");
            options.CommandLineArgs.Insert(0, CommandLineArgumentEscaper.EscapeSingleArg(serverDllPath));

            if (string.IsNullOrWhiteSpace(options.FrameworkVersion) == false)
                options.CommandLineArgs.Insert(0, $"--fx-version {options.FrameworkVersion}");

            var argumentsString = string.Join(" ", options.CommandLineArgs);

            var processStartInfo = new ProcessStartInfo
            {
                FileName = options.DotNetPath,
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

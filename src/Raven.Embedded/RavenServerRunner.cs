using System;
using System.Diagnostics;
using System.IO;
using System.Linq;

namespace Raven.Embedded
{
    internal class RavenServerRunner
    {
        private static readonly string RavenDbServerPath = Path.Combine(AppContext.BaseDirectory, "RavenDBServer/Raven.Server.dll");

        public static Process Run(ServerOptions options)
        {
            if (File.Exists(RavenDbServerPath) == false)
            {
                throw new FileNotFoundException("Server file was not found", RavenDbServerPath);
            }

            using (var currentProcess = Process.GetCurrentProcess())
            {
                options.CommandLineArgs.Add("--Embedded.ParentProcessId=" + currentProcess.Id);
            }

            if(string.IsNullOrEmpty(options.DataDir) == false)
                options.CommandLineArgs.Add($"--DataDir={options.DataDir}");

            var argumentsString = string.Join(" ", new[] { $"--fx-version { options.FrameworkVersion} ", RavenDbServerPath }
                .Concat(options.CommandLineArgs));
            var processStartInfo = new ProcessStartInfo
            {
                FileName = "dotnet",
                Arguments = argumentsString,
                CreateNoWindow = true,
                RedirectStandardOutput = true,
                RedirectStandardError = true,
                UseShellExecute = false,
            };

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

    }
}

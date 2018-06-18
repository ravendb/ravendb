using System;
using System.Diagnostics;
using System.IO;

namespace Raven.Embedded
{
    internal class RavenServerRunner
    {
        public static Process Run(ServerOptions options)
        {
            if (string.IsNullOrWhiteSpace(options.ServerDirectory))
                throw new ArgumentNullException(nameof(options.ServerDirectory));

            var serverDllPath = Path.Combine(options.ServerDirectory, "Raven.Server.dll");

            if (File.Exists(serverDllPath) == false)
                throw new FileNotFoundException("Server file was not found", serverDllPath);

            using (var currentProcess = Process.GetCurrentProcess())
            {
                options.CommandLineArgs.Add("--Embedded.ParentProcessId=" + currentProcess.Id);
            }

            options.CommandLineArgs.Add("--ServerUrl=http://127.0.0.1:0");
            options.CommandLineArgs.Add("--License.Eula.Accepted=true");
            options.CommandLineArgs.Add("--Setup.Mode=None");

            options.CommandLineArgs.Insert(0, serverDllPath);

            if (string.IsNullOrWhiteSpace(options.FrameworkVersion) == false)
                options.CommandLineArgs.Insert(0, $"--fx-version {options.FrameworkVersion}");

            var argumentsString = string.Join(" ", options.CommandLineArgs);

            var processStartInfo = new ProcessStartInfo
            {
                FileName = "dotnet",
                Arguments = argumentsString,
                CreateNoWindow = true,
                RedirectStandardOutput = true,
                RedirectStandardError = true,
                UseShellExecute = false,
                WorkingDirectory = options.WorkingDirectory ?? ServerOptions.Default.WorkingDirectory
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

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;

namespace Raven.TestDriver
{
    internal class RavenServerRunner<TLocator> where TLocator : RavenServerLocator
    {
        internal static string _emptySettingsFilePath;

        internal static string EmptySettingsFilePath
        {
            get
            {
                if (string.IsNullOrEmpty(_emptySettingsFilePath))
                    _emptySettingsFilePath = Path.Combine(Path.GetTempPath(), "testdriver.settings.json");

                return _emptySettingsFilePath;
            }
        }

        public static Process Run(TLocator locator)
        {
            var processStartInfo = GetProcessStartInfo(locator);
            Process result = null;
            try
            {
                result = Process.Start(processStartInfo);
            }
            catch (Exception e)
            {
                result?.Kill();
                throw new InvalidOperationException("Unable to execute server." + Environment.NewLine +
                    "Command was: " + Environment.NewLine +
                    (processStartInfo.WorkingDirectory ?? Directory.GetCurrentDirectory()) + "> "
                    + processStartInfo.FileName + " " + processStartInfo.Arguments
                    , e);
            }

            return result;
        }

        private static ProcessStartInfo GetProcessStartInfo(TLocator locator)
        {
            if (File.Exists(locator.ServerPath) == false)
            {
                throw new FileNotFoundException("Server file was not found", locator.ServerPath);
            }

            File.WriteAllText(EmptySettingsFilePath, "{}");

            using (var currentProcess = Process.GetCurrentProcess())
            {
                var commandArguments = new List<string>
                {
                    locator.CommandArguments,
                    $"-c {EmptySettingsFilePath}",
                    "--ServerUrl=http://127.0.0.1:0",
                    "--RunInMemory=true",
                    "--Testing.ParentProcessId=" + currentProcess.Id,
                    "--Setup.Mode=None"
                };

                var argumentsString = string.Join(" ", commandArguments);
                return new ProcessStartInfo
                {
                    FileName = locator.Command,
                    Arguments = argumentsString,
                    CreateNoWindow = true,
                    RedirectStandardOutput = true,
                    RedirectStandardError = true,
                    UseShellExecute = false,
                };
            }
        }
    }
}

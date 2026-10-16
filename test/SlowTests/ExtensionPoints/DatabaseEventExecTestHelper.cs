using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using Raven.Server.Config;
using Sparrow.Platform;

namespace SlowTests.ExtensionPoints
{
    /// <summary>
    /// Shared plumbing for the database event hook tests. Every hook script is run by bash on POSIX and by
    /// PowerShell on Windows, and receives its output file as the first user argument.
    /// </summary>
    internal static class DatabaseEventExecTestHelper
    {
        public static string Executable => PlatformDetails.RunningOnPosix ? "bash" : "powershell";

        public static string Script(string posix, string windows) => PlatformDetails.RunningOnPosix ? posix : windows;

        public static string Arguments(string scriptFile, string outputFile) =>
            PlatformDetails.RunningOnPosix ? $"{scriptFile} {outputFile}" : $"-NoProfile {scriptFile} {outputFile}";

        public static string NewScriptPath() => NewTempPath(PlatformDetails.RunningOnPosix ? ".sh" : ".ps1");

        public static string NewOutputPath() => NewTempPath(".txt");

        public static void ConfigureHook(IDictionary<string, string> settings, string scriptFile, string outputFile)
        {
            settings[RavenConfiguration.GetKey(x => x.Databases.OnDatabaseDeleteExec)] = Executable;
            settings[RavenConfiguration.GetKey(x => x.Databases.OnDatabaseDeleteExecArguments)] = Arguments(scriptFile, outputFile);
        }

        public static void TryDelete(params string[] paths)
        {
            foreach (string path in paths)
            {
                try
                {
                    if (File.Exists(path))
                        File.Delete(path);
                }
                catch (Exception e) when (e is IOException or UnauthorizedAccessException)
                {
                    // a still running script may hold the file on Windows
                }
            }
        }

        public static bool IsAlive(int pid)
        {
            try
            {
                using Process process = Process.GetProcessById(pid);
                return process.HasExited == false;
            }
            catch (ArgumentException)
            {
                return false;
            }
            catch (InvalidOperationException)
            {
                return false;
            }
        }

        private static string NewTempPath(string extension) => Path.Combine(Path.GetTempPath(), Path.ChangeExtension(Guid.NewGuid().ToString(), extension));
    }
}

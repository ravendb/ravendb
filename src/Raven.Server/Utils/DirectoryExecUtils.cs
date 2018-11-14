using System;
using System.Diagnostics;
using Raven.Server.Config.Categories;
using Sparrow;
using Sparrow.Logging;
using Sparrow.Utils;
using Voron;

namespace Raven.Server.Utils
{
    public static class DirectoryExecUtils
    {
        public static void SubscribeToOnDirectoryInitializeExec(StorageEnvironmentOptions options, StorageConfiguration config, string databaseName, EnvironmentType envType, Logger logger)
        {
            if (string.IsNullOrEmpty(config.OnDirectoryInitializeExec))
                return;

            var directoryParameters = new DirectoryParameters
            {
                OnDirectoryInitializeExec = config.OnDirectoryInitializeExec,
                OnDirectoryInitializeExecArguments = config.OnDirectoryInitializeExecArguments,
                OnDirectoryInitializeExecTimeout = config.OnDirectoryInitializeExecTimeout.AsTimeSpan,
                DatabaseName = databaseName,
                Type = envType
            };

            void OnDirectory(StorageEnvironmentOptions internalOptions)
            {
                OnDirectoryInitialize(internalOptions, directoryParameters, logger);
            }

            options.OnDirectoryInitialize += OnDirectory;
        }

        public static void OnDirectoryInitialize(StorageEnvironmentOptions options, DirectoryParameters parameters, Logger log)
        {
            RavenProcess process = null;
            try
            {
                var journalPath = string.Empty;
                if (options is StorageEnvironmentOptions.DirectoryStorageEnvironmentOptions dirOptions)
                    journalPath = dirOptions.JournalPath.FullPath;

                var userArgs = parameters.OnDirectoryInitializeExecArguments ?? string.Empty;
                var args = $"{userArgs} {parameters.Type} {parameters.DatabaseName} " +
                           $"{CommandLineArgumentEscaper.EscapeSingleArg(options.BasePath.ToString())} " +
                           $"{CommandLineArgumentEscaper.EscapeSingleArg(options.TempPath.ToString())} " +
                           $"{CommandLineArgumentEscaper.EscapeSingleArg(journalPath)}";

                var startInfo = new ProcessStartInfo
                {
                    FileName = parameters.OnDirectoryInitializeExec,
                    Arguments = args,
                    UseShellExecute = false,
                    RedirectStandardOutput = true,
                    RedirectStandardError = true,
                    CreateNoWindow = true
                };
                
                var sw = Stopwatch.StartNew();

                try
                {
                    process = RavenProcess.Start(startInfo);
                }
                catch (Exception e)
                {
                    throw new InvalidOperationException($"Unable to execute '{parameters.OnDirectoryInitializeExec} {args}'. Failed to start process.", e);
                }

                var readStdOut = process.StandardOutput.ReadToEndAsync();
                var readErrors = process.StandardError.ReadToEndAsync();

                string GetStdError()
                {
                    try
                    {
                        return readErrors.Result;
                    }
                    catch (Exception e)
                    {
                        return $"Unable to get stderr, got exception: {e}";
                    }
                }

                string GetStdOut()
                {
                    try
                    {
                        return readStdOut.Result;
                    }
                    catch (Exception e)
                    {
                        return $"Unable to get stdout, got exception: {e}";
                    }
                }

                if (process.WaitForExit((int)parameters.OnDirectoryInitializeExecTimeout.TotalMilliseconds) == false)
                {
                    process.Kill();
                    throw new InvalidOperationException($"Unable to execute '{parameters.OnDirectoryInitializeExec} {args}', waited for {(int)parameters.OnDirectoryInitializeExecTimeout.TotalMilliseconds} ms but the process didn't exit. Output: {GetStdOut()}{Environment.NewLine}Errors: {GetStdError()}");
                }

                try
                {
                    readStdOut.Wait(parameters.OnDirectoryInitializeExecTimeout);
                    readErrors.Wait(parameters.OnDirectoryInitializeExecTimeout);
                }
                catch (Exception e)
                {
                    throw new InvalidOperationException($"Unable to read redirected stderr and stdout when executing '{parameters.OnDirectoryInitializeExec} {args}'", e);
                }

                // Can have exit code o (success) but still get errors. We log the errors anyway.
                if (log.IsOperationsEnabled)
                    log.Operations(string.Format($"Executing '{parameters.OnDirectoryInitializeExec} {args}' took {sw.ElapsedMilliseconds:#,#;;0} ms. Exit code: {process.ExitCode}{Environment.NewLine}Output: {GetStdOut()}{Environment.NewLine}Errors: {GetStdError()}{Environment.NewLine}"));

                if (process.ExitCode != 0)
                {
                    throw new InvalidOperationException(
                        $"Command or executable '{parameters.OnDirectoryInitializeExec} {args}' failed. Exit code: {process.ExitCode}{Environment.NewLine}Output: {GetStdOut()}{Environment.NewLine}Errors: {GetStdError()}{Environment.NewLine}");
                }
            }
            finally 
            {
                process?.Dispose();
            }
        }

        public enum EnvironmentType
        {
            System,
            Database,
            Index,
            Configuration,
            Compaction
        }

        public class DirectoryParameters
        {
            public string OnDirectoryInitializeExec { get; set; }
            public string OnDirectoryInitializeExecArguments { get; set; }
            public TimeSpan OnDirectoryInitializeExecTimeout { get; set; }
            public string DatabaseName { get; set; }
            public EnvironmentType Type { get; set; }
        }
    }
}

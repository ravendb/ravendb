using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Text;
using Raven.Server.Config.Categories;
using Sparrow.Logging;
using Sparrow.Utils;
using Voron;

namespace Raven.Server.Utils
{
    public static class DirectoryExecUtils
    {
        public static void SubscribeToOnDirectoryExec(StorageEnvironmentOptions options, StorageConfiguration config, string databaseName, EnvironmentType envType, Logger logger)
        {
            if (string.IsNullOrEmpty(config.OnCreateDirectoryExec))
                return;

            var directoryParameters = new DirectoryParameters
            {
                OnCreateDirectoryExec = config.OnCreateDirectoryExec,
                OnCreateDirectoryExecArguments = config.OnCreateDirectoryExecArguments,
                OnCreateDirectoryExecTimeout = config.OnCreateDirectoryExecTimeout.AsTimeSpan,
                DatabaseName = databaseName,
                Type = envType
            };

            void OnDirectory(StorageEnvironmentOptions internalOptions)
            {
                OnCreateDirectory(internalOptions, directoryParameters, logger);
            }

            options.OnCreateDirectory += OnDirectory;
        }

        public static void OnCreateDirectory(StorageEnvironmentOptions options, DirectoryParameters parameters, Logger log)
        {
            Process process = null;
            try
            {
                var journalPath = string.Empty;
                if (options is StorageEnvironmentOptions.DirectoryStorageEnvironmentOptions dirOptions)
                    journalPath = dirOptions.JournalPath.FullPath;

                var userArgs = parameters.OnCreateDirectoryExecArguments ?? string.Empty;
                var args = $"{userArgs} {parameters.Type} {parameters.DatabaseName} " +
                           $"{CommandLineArgumentEscaper.EscapeSingleArg(options.BasePath.ToString())} " +
                           $"{CommandLineArgumentEscaper.EscapeSingleArg(options.TempPath.ToString())} " +
                           $"{CommandLineArgumentEscaper.EscapeSingleArg(journalPath)}";

                process = new Process
                {
                    StartInfo = new ProcessStartInfo
                    {
                        FileName = parameters.OnCreateDirectoryExec,
                        Arguments = args,
                        UseShellExecute = false,
                        RedirectStandardOutput = true,
                        RedirectStandardError = true,
                        CreateNoWindow = true
                    }
                };
                
                var sw = Stopwatch.StartNew();

                try
                {
                    process.Start();
                }
                catch (Exception e)
                {
                    throw new InvalidOperationException($"Unable to execute '{parameters.OnCreateDirectoryExec} {args}'. Failed to start process.", e);
                }

                var readStdOut = process.StandardOutput.ReadToEndAsync();
                var readErrors = process.StandardError.ReadToEndAsync();

                string GetStdError()
                {
                    try
                    {
                        return readErrors.Result;
                    }
                    catch
                    {
                        return "Unable to get stderr";
                    }
                }

                string GetStdOut()
                {
                    try
                    {
                        return readStdOut.Result;
                    }
                    catch
                    {
                        return "Unable to get stdout";
                    }
                }

                if (process.WaitForExit((int)parameters.OnCreateDirectoryExecTimeout.TotalMilliseconds) == false)
                {
                    process.Kill();
                    throw new InvalidOperationException($"Unable to execute '{parameters.OnCreateDirectoryExec} {args}', waited for {(int)parameters.OnCreateDirectoryExecTimeout.TotalMilliseconds} ms but the process didn't exit. Output: {GetStdOut()}{Environment.NewLine}Errors: {GetStdError()}");
                }

                try
                {
                    readStdOut.Wait(parameters.OnCreateDirectoryExecTimeout);
                    readErrors.Wait(parameters.OnCreateDirectoryExecTimeout);
                }
                catch (Exception e)
                {
                    throw new InvalidOperationException($"Unable to read redirected stderr and stdout when executing '{parameters.OnCreateDirectoryExec} {args}'", e);
                }

                // Can have exit code o (success) but still get errors. We log the errors anyway.
                if (log.IsOperationsEnabled)
                    log.Operations(string.Format($"Executing '{parameters.OnCreateDirectoryExec} {args}' took {sw.ElapsedMilliseconds:#,#;;0} ms. Exit code: {process.ExitCode}{Environment.NewLine}Output: {GetStdOut()}{Environment.NewLine}Errors: {GetStdError()}{Environment.NewLine}"));

                if (process.ExitCode != 0)
                {
                    throw new InvalidOperationException(
                        $"Command or executable '{parameters.OnCreateDirectoryExec} {args}' failed. Exit code: {process.ExitCode}{Environment.NewLine}Output: {GetStdOut()}{Environment.NewLine}Errors: {GetStdError()}{Environment.NewLine}");
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
            public string OnCreateDirectoryExec { get; set; }
            public string OnCreateDirectoryExecArguments { get; set; }
            public TimeSpan OnCreateDirectoryExecTimeout { get; set; }
            public string DatabaseName { get; set; }
            public EnvironmentType Type { get; set; }
        }
    }
}

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
            var journalPath = string.Empty;
            if (options is StorageEnvironmentOptions.DirectoryStorageEnvironmentOptions dirOptions)
                journalPath = dirOptions.JournalPath.FullPath;

            var userArgs = parameters.OnCreateDirectoryExecArguments ?? string.Empty;
            var args = $"{userArgs} {parameters.Type} {parameters.DatabaseName} " +
                       $"{CommandLineArgumentEscaper.EscapeSingleArg(options.BasePath.ToString())} " +
                       $"{CommandLineArgumentEscaper.EscapeSingleArg(options.TempPath.ToString())} " +
                       $"{CommandLineArgumentEscaper.EscapeSingleArg(journalPath)}";

            var process = new Process
            {
                StartInfo = new ProcessStartInfo
                {
                    FileName = parameters.OnCreateDirectoryExec,
                    Arguments = args,
                    UseShellExecute = false,
                    RedirectStandardOutput = false,
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

            if (process.WaitForExit((int)parameters.OnCreateDirectoryExecTimeout.TotalMilliseconds) == false)
            {
                process.Kill();
                throw new InvalidOperationException($"Unable to execute '{parameters.OnCreateDirectoryExec} {args}', waited for {(int)parameters.OnCreateDirectoryExecTimeout.TotalMilliseconds} ms but the process didn't exit. Stderr: {GetStdError()}");
            }
            try
            {
                readErrors.Wait(parameters.OnCreateDirectoryExecTimeout);
            }
            catch (Exception e)
            {
                throw new InvalidOperationException(
                    $"Unable to execute '{parameters.OnCreateDirectoryExec} {args}', waited for {(int)parameters.OnCreateDirectoryExecTimeout.TotalMilliseconds} ms but the process didn't exit. Stderr: {GetStdError()}",
                    e);

            }

            if (log.IsOperationsEnabled)
            {
                var errors = GetStdError();
                if (string.IsNullOrEmpty(errors))
                    errors = "none";
                log.Operations(string.Format($"Executing '{parameters.OnCreateDirectoryExec} {args}' took {sw.ElapsedMilliseconds:#,#;;0} ms. Exit code: {process.ExitCode}. Errors: {errors}"));
            }

            if (process.ExitCode != 0)
            {
                throw new InvalidOperationException(
                    $"Command or executable '{parameters.OnCreateDirectoryExec} {args}' failed. The exit code is {process.ExitCode}. Stderr: {GetStdError()}");
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

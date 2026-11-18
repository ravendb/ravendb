using System;
using System.Diagnostics;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Extensions;
using Sparrow.Logging;
using Sparrow.Utils;

namespace Raven.Server.Utils
{
    public static class DatabaseExecUtils
    {
        // once the process is gone its pipes are closed, so a read that still hasn't completed after
        // this is never going to complete - we give up on the output rather than block the hook
        private static readonly TimeSpan ReadOutputTimeout = TimeSpan.FromSeconds(5);

        /// <summary>
        /// Runs the configured database deletion hook once and throws if it fails to start, does not exit
        /// within <paramref name="timeout"/> or exits with a non-zero code.
        /// </summary>
        public static async Task ExecuteOnDatabaseDeleteAsync(string executable, string userArguments, TimeSpan timeout,
            string databaseName, bool hardDelete, long raftIndex, Logger logger)
        {
            string deletionKind = hardDelete ? "hard" : "soft";
            string databaseNameBase64 = Convert.ToBase64String(Encoding.UTF8.GetBytes(databaseName));

            // '--' terminates the user supplied options. A database name may legally begin with '-'
            // ('--force' is a valid name), and without the sentinel a script using getopt / argparse /
            // a PowerShell param() block would parse the name as a flag.
            //
            // The raft index identifies the deletion itself, which the name cannot: a database deleted
            // and re-created under the same name produces two events with identical names.
            string hookArgs = $"-- {CommandLineArgumentEscaper.EscapeSingleArg(databaseName)} {databaseNameBase64} {deletionKind} {raftIndex}";
            string args = string.IsNullOrEmpty(userArguments) ? hookArgs : $"{userArguments} {hookArgs}";

            // Never put 'args' in a log line or an exception message. The user supplied part comes from
            // a secured configuration entry and may carry credentials.
            string description = $"'{executable}' for the {deletionKind} deletion of database '{databaseName}' (raft index {raftIndex})";

            ProcessStartInfo startInfo = new ProcessStartInfo
            {
                FileName = executable,
                Arguments = args,
                UseShellExecute = false,
                RedirectStandardOutput = true,
                RedirectStandardError = true,
                CreateNoWindow = true
            };

            Stopwatch sw = Stopwatch.StartNew();

            Process process;
            try
            {
                process = Process.Start(startInfo);
            }
            catch (Exception e)
            {
                throw new InvalidOperationException($"Unable to execute {description}. Failed to start process.", e);
            }

            using (process)
            using (CancellationTokenSource timeoutCts = new CancellationTokenSource(timeout))
            {
                Task<string> readStdOut = process.StandardOutput.ReadToEndAsync();
                Task<string> readStdErr = process.StandardError.ReadToEndAsync();

                try
                {
                    await process.WaitForExitAsync(timeoutCts.Token).ConfigureAwait(false);
                }
                catch (OperationCanceledException)
                {
                    // kill the whole tree: a surviving child would keep the redirected pipes open
                    KillProcessTree(process, logger);

                    throw new InvalidOperationException(
                        $"Unable to execute {description}, waited for {(int)timeout.TotalMilliseconds} ms but the process didn't exit. " +
                        await ReadOutputAsync(readStdOut, readStdErr).ConfigureAwait(false));
                }

                string output = await ReadOutputAsync(readStdOut, readStdErr).ConfigureAwait(false);

                // Can have exit code 0 (success) but still get errors. We log the errors anyway.
                if (logger.IsInfoEnabled)
                    logger.Info($"Executing {description} took {sw.ElapsedMilliseconds:#,#;;0} ms. Exit code: {process.ExitCode}{Environment.NewLine}{output}");

                if (process.ExitCode != 0)
                    throw new InvalidOperationException($"Executing {description} failed. Exit code: {process.ExitCode}{Environment.NewLine}{output}");
            }
        }

        private static async Task<string> ReadOutputAsync(Task<string> readStdOut, Task<string> readStdErr)
        {
            Task<string[]> read = Task.WhenAll(readStdOut, readStdErr);

            if (await read.WaitWithTimeout(ReadOutputTimeout).ConfigureAwait(false) == false)
            {
                _ = read.IgnoreUnobservedExceptions();
                return "Output: <not available: output was not read in time>";
            }

            try
            {
                string[] results = await read.ConfigureAwait(false);
                return $"Output: {results[0]}{Environment.NewLine}Errors: {results[1]}";
            }
            catch (Exception e)
            {
                return $"Output: <not available: {e.Message}>";
            }
        }

        private static void KillProcessTree(Process process, Logger logger)
        {
            try
            {
                process.Kill(entireProcessTree: true);
            }
            catch (Exception e)
            {
                if (logger.IsInfoEnabled)
                    logger.Info("Failed to kill the process tree of the database deletion hook.", e);
            }
        }
    }
}

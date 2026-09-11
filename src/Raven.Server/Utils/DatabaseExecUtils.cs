using System;
using System.ComponentModel;
using System.Diagnostics;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Sparrow.Logging;
using Sparrow.Platform;
using Sparrow.Utils;

namespace Raven.Server.Utils
{
    public static class DatabaseExecUtils
    {
        public const int MaxAttempts = 3;

        private static readonly TimeSpan BackoffBase = TimeSpan.FromSeconds(1);
        private static readonly TimeSpan BackoffCap = TimeSpan.FromSeconds(8);

        // once the process is gone its pipes are closed, so a read that still hasn't completed after
        // this is never going to complete - we give up on the output rather than block the hook
        private static readonly TimeSpan ReadDrainBudget = TimeSpan.FromSeconds(5);

        private const string OutputNotAvailable = "<not available: output was not read in time>";

        // Failures that will never resolve by trying again: the executable is missing, is not
        // runnable, or we are not allowed to run it. Win32Exception.NativeErrorCode carries a Win32
        // code on Windows and an errno on POSIX, and the two namespaces collide - 8 is ENOEXEC on
        // POSIX but ERROR_NOT_ENOUGH_MEMORY, which is transient, on Windows - so one shared list
        // would misclassify on both platforms.
        private static readonly int[] PermanentStartErrorCodes = PlatformDetails.RunningOnPosix
            ? new[]
            {
                2, // ENOENT
                8, // ENOEXEC
                13 // EACCES
            }
            : new[]
            {
                2, // ERROR_FILE_NOT_FOUND
                3, // ERROR_PATH_NOT_FOUND
                5, // ERROR_ACCESS_DENIED
                193 // ERROR_BAD_EXE_FORMAT
            };

        public sealed class OnDatabaseDeleteParameters
        {
            public string Executable;
            public string Arguments;
            public TimeSpan Timeout;
            public TimeSpan MaxRetryDuration;
            public string DatabaseName;
            public bool HardDelete;
            public Logger Logger;
            public Action OnAttempt;
        }

        /// <summary>
        /// Runs the configured database deletion hook, retrying a failing script up to
        /// <see cref="MaxAttempts"/> times with jittered exponential backoff, bounded by a single
        /// wall clock budget that covers every attempt and the delays between them.
        /// </summary>
        public static async Task ExecuteOnDatabaseDeleteAsync(OnDatabaseDeleteParameters parameters, CancellationToken token)
        {
            string databaseNameBase64 = Convert.ToBase64String(Encoding.UTF8.GetBytes(parameters.DatabaseName));
            string userArgs = parameters.Arguments ?? string.Empty;
            string escapedDatabaseName = CommandLineArgumentEscaper.EscapeSingleArg(parameters.DatabaseName);
            string deletionKind = parameters.HardDelete ? "hard" : "soft";

            // '--' terminates the user supplied options. A database name may legally begin with '-'
            // ('--force' is a valid name), and without the sentinel a script using getopt / argparse /
            // a PowerShell param() block would parse the name as a flag.
            string args = string.IsNullOrEmpty(userArgs)
                ? $"-- {escapedDatabaseName} {databaseNameBase64} {deletionKind}"
                : $"{userArgs} -- {escapedDatabaseName} {databaseNameBase64} {deletionKind}";

            // Never put 'args' in a log line or an exception message. The user supplied part comes from
            // a secured configuration entry and may carry credentials, and the server log is readable by
            // an Operator while the settings endpoint redacts that value even for a cluster admin.
            int userArgsCount = userArgs.Split(' ', StringSplitOptions.RemoveEmptyEntries).Length;
            string commandDescription =
                $"'{parameters.Executable}' with {userArgsCount} configured argument(s), for the {deletionKind} deletion of database '{parameters.DatabaseName}'";

            Stopwatch totalDuration = Stopwatch.StartNew();

            for (int attempt = 1; ; attempt++)
            {
                token.ThrowIfCancellationRequested();

                // The budget has to bound the attempt itself, not only the gaps between attempts:
                // capping just the delays lets a configured total of 90s run for 90s plus one full
                // attempt timeout, and makes the budget meaningless when it is below the timeout.
                TimeSpan attemptTimeout = parameters.Timeout;
                TimeSpan budgetLeft = parameters.MaxRetryDuration - totalDuration.Elapsed;

                if (budgetLeft <= TimeSpan.Zero && attempt > 1)
                {
                    throw new InvalidOperationException(
                        $"Executing {commandDescription} was abandoned after {attempt - 1} attempt(s): the retry budget of {parameters.MaxRetryDuration} is exhausted.");
                }

                if (budgetLeft > TimeSpan.Zero && budgetLeft < attemptTimeout)
                    attemptTimeout = budgetLeft;

                parameters.OnAttempt?.Invoke();

                try
                {
                    await RunOnceAsync(parameters, args, commandDescription, attempt, attemptTimeout, token).ConfigureAwait(false);
                    return;
                }
                catch (OperationCanceledException) when (token.IsCancellationRequested)
                {
                    throw;
                }
                catch (PermanentExecutionFailureException)
                {
                    // retrying cannot help, and doing so would only multiply the log noise
                    throw;
                }
                catch (Exception e)
                {
                    if (attempt >= MaxAttempts)
                        throw new InvalidOperationException($"Executing {commandDescription} failed on all {MaxAttempts} attempts.", e);

                    TimeSpan delay = GetBackoffDelay(attempt);
                    TimeSpan remaining = parameters.MaxRetryDuration - totalDuration.Elapsed;
                    if (remaining <= delay)
                    {
                        throw new InvalidOperationException(
                            $"Executing {commandDescription} failed on attempt {attempt} of {MaxAttempts} and the retry budget of {parameters.MaxRetryDuration} is exhausted.", e);
                    }

                    if (parameters.Logger.IsInfoEnabled)
                        parameters.Logger.Info($"Executing {commandDescription} failed on attempt {attempt} of {MaxAttempts}, retrying in {delay}.", e);

                    await Task.Delay(delay, token).ConfigureAwait(false);
                }
            }
        }

        private static async Task RunOnceAsync(OnDatabaseDeleteParameters parameters, string args, string commandDescription, int attempt,
            TimeSpan attemptTimeout, CancellationToken token)
        {
            Process process = null;
            try
            {
                // one budget per attempt for the process wait, so the configured timeout means what
                // it says instead of being applied once per wait
                using (CancellationTokenSource attemptCts = CancellationTokenSource.CreateLinkedTokenSource(token))
                {
                    attemptCts.CancelAfter(attemptTimeout);
                    CancellationToken attemptToken = attemptCts.Token;

                    ProcessStartInfo startInfo = new ProcessStartInfo
                    {
                        FileName = parameters.Executable,
                        Arguments = args,
                        UseShellExecute = false,
                        RedirectStandardOutput = true,
                        RedirectStandardError = true,
                        CreateNoWindow = true
                    };

                    Stopwatch sw = Stopwatch.StartNew();

                    try
                    {
                        process = Process.Start(startInfo);
                    }
                    catch (Win32Exception e) when (Array.IndexOf(PermanentStartErrorCodes, e.NativeErrorCode) >= 0)
                    {
                        throw new PermanentExecutionFailureException(
                            $"Unable to execute {commandDescription}. Failed to start the process, and the failure will not resolve on retry.", e);
                    }
                    catch (Exception e)
                    {
                        throw new InvalidOperationException($"Unable to execute {commandDescription}. Failed to start process.", e);
                    }

                    // Deliberately bound to the shutdown token rather than to the attempt timeout: on a
                    // timeout we kill the tree and then drain whatever the script managed to write, and
                    // a read cancelled by the very token that defines the timeout always comes back
                    // empty. ReadWithBudgetAsync is what bounds these instead.
                    Task<string> readStdOut = process.StandardOutput.ReadToEndAsync(token);
                    Task<string> readStdErr = process.StandardError.ReadToEndAsync(token);

                    bool timedOut = false;
                    try
                    {
                        await process.WaitForExitAsync(attemptToken).ConfigureAwait(false);
                    }
                    catch (OperationCanceledException)
                    {
                        // Kill the entire tree whatever cancelled the wait. On a timeout a surviving
                        // grandchild holds the redirected pipes open, which is what used to make reading
                        // the output block forever. On shutdown it matters even more: Process.Dispose()
                        // closes handles but never terminates the process, so the tree would be
                        // reparented and outlive the server it was spawned by.
                        KillProcessTree(process);

                        if (token.IsCancellationRequested)
                        {
                            ObserveFailure(readStdOut);
                            ObserveFailure(readStdErr);
                            throw;
                        }

                        timedOut = true;
                    }

                    if (timedOut)
                    {
                        string timedOutStdOut = await ReadWithBudgetAsync(readStdOut).ConfigureAwait(false);
                        string timedOutStdErr = await ReadWithBudgetAsync(readStdErr).ConfigureAwait(false);

                        throw new InvalidOperationException(
                            $"Unable to execute {commandDescription}, waited for {(int)attemptTimeout.TotalMilliseconds} ms but the process didn't exit. " +
                            $"Output: {timedOutStdOut}{Environment.NewLine}Errors: {timedOutStdErr}");
                    }

                    string output = await ReadWithBudgetAsync(readStdOut).ConfigureAwait(false);
                    string errors = await ReadWithBudgetAsync(readStdErr).ConfigureAwait(false);

                    // Can have exit code 0 (success) but still get errors. We log the errors anyway.
                    // Note that this writes the script's own output to the log, so a script that echoes
                    // a secret leaks it - that is called out in the configuration description.
                    if (parameters.Logger.IsInfoEnabled)
                    {
                        parameters.Logger.Info(
                            $"Executing {commandDescription} (attempt {attempt} of {MaxAttempts}) took {sw.ElapsedMilliseconds:#,#;;0} ms. " +
                            $"Exit code: {process.ExitCode}{Environment.NewLine}Output: {output}{Environment.NewLine}Errors: {errors}{Environment.NewLine}");
                    }

                    if (process.ExitCode != 0)
                    {
                        throw new InvalidOperationException(
                            $"Executing {commandDescription} failed. Exit code: {process.ExitCode}{Environment.NewLine}" +
                            $"Output: {output}{Environment.NewLine}Errors: {errors}{Environment.NewLine}");
                    }
                }
            }
            finally
            {
                process?.Dispose();
            }
        }

        private static TimeSpan GetBackoffDelay(int attempt)
        {
            // decorrelated jitter: a bulk delete fires one hook per database at effectively the same
            // instant, and a fixed backoff would make them all retry in lockstep against an endpoint
            // that is already struggling
            double cappedMs = Math.Min(BackoffCap.TotalMilliseconds, BackoffBase.TotalMilliseconds * Math.Pow(2, attempt - 1));
            return TimeSpan.FromMilliseconds(Random.Shared.NextDouble() * cappedMs);
        }

        private static async Task<string> ReadWithBudgetAsync(Task<string> read)
        {
            Task completed = await Task.WhenAny(read, Task.Delay(ReadDrainBudget)).ConfigureAwait(false);
            if (completed != read)
            {
                ObserveFailure(read);
                return OutputNotAvailable;
            }

            try
            {
                return await read.ConfigureAwait(false);
            }
            catch (Exception e)
            {
                return $"<not available: {e.Message}>";
            }
        }

        private static void ObserveFailure(Task task)
        {
            // make sure an abandoned read failing later does not surface as an unobserved task exception
            _ = task.ContinueWith(static t => _ = t.Exception, TaskContinuationOptions.OnlyOnFaulted | TaskContinuationOptions.ExecuteSynchronously);
        }

        private static void KillProcessTree(Process process)
        {
            try
            {
                process.Kill(entireProcessTree: true);
            }
            catch (Exception)
            {
                // the process may have exited in the meantime, or the tree may not be walkable
                ProcessExtensions.TryKill(process);
            }
        }

        private sealed class PermanentExecutionFailureException : Exception
        {
            public PermanentExecutionFailureException(string message, Exception innerException)
                : base(message, innerException)
            {
            }
        }
    }
}

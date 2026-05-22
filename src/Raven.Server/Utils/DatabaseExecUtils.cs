using System;
using System.Diagnostics;
using System.Text;
using Sparrow.Logging;
using Sparrow.Utils;

namespace Raven.Server.Utils
{
    public static class DatabaseExecUtils
    {
        public static void ExecuteOnDatabaseEvent(string executable, string arguments, TimeSpan timeout, string databaseName, Logger log)
        {
            Process process = null;
            try
            {
                var databaseNameBase64 = Convert.ToBase64String(Encoding.UTF8.GetBytes(databaseName));

                var userArgs = arguments ?? string.Empty;
                var args = $"{userArgs} {CommandLineArgumentEscaper.EscapeSingleArg(databaseName)} {databaseNameBase64}";

                var startInfo = new ProcessStartInfo
                {
                    FileName = executable,
                    Arguments = args,
                    UseShellExecute = false,
                    RedirectStandardOutput = true,
                    RedirectStandardError = true,
                    CreateNoWindow = true
                };

                var sw = Stopwatch.StartNew();

                try
                {
                    process = Process.Start(startInfo);
                }
                catch (Exception e)
                {
                    throw new InvalidOperationException($"Unable to execute '{executable} {args}'. Failed to start process.", e);
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

                if (process.WaitForExit((int)timeout.TotalMilliseconds) == false)
                {
                    process.Kill();
                    throw new InvalidOperationException(
                        $"Unable to execute '{executable} {args}', waited for {(int)timeout.TotalMilliseconds} ms but the process didn't exit. Output: {GetStdOut()}{Environment.NewLine}Errors: {GetStdError()}");
                }

                try
                {
                    readStdOut.Wait(timeout);
                    readErrors.Wait(timeout);
                }
                catch (Exception e)
                {
                    throw new InvalidOperationException(
                        $"Unable to read redirected stderr and stdout when executing '{executable} {args}'", e);
                }

                // Can have exit code 0 (success) but still get errors. We log the errors anyway.
                if (log.IsOperationsEnabled)
                    log.Operations(
                        $"Executing '{executable} {args}' took {sw.ElapsedMilliseconds:#,#;;0} ms. Exit code: {process.ExitCode}{Environment.NewLine}Output: {GetStdOut()}{Environment.NewLine}Errors: {GetStdError()}{Environment.NewLine}");

                if (process.ExitCode != 0)
                {
                    throw new InvalidOperationException(
                        $"Command or executable '{executable} {args}' failed. Exit code: {process.ExitCode}{Environment.NewLine}Output: {GetStdOut()}{Environment.NewLine}Errors: {GetStdError()}{Environment.NewLine}");
                }
            }
            finally
            {
                process?.Dispose();
            }
        }
    }
}

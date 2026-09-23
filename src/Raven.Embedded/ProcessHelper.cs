#nullable enable
using System;
using System.ComponentModel;
using System.Diagnostics;
using System.IO;
using System.Text;
using System.Threading.Tasks;
using Raven.Client.Extensions;
using Sparrow.Platform;

namespace Raven.Embedded
{
    internal static class ProcessHelper
    {
        internal static ProcessOutput ReadOutput(this Process process, Action<string> onOutputLine) => new(process, onOutputLine);

        internal sealed class ProcessOutput
        {
            private readonly Process _process;
            private readonly string _executable;
            private readonly string _workingDirectory;
            private readonly string? _resolvedExecutable;
            private readonly CapturedOutput _standardOutput = new();
            private readonly CapturedOutput _standardError = new();

            internal Task Completion { get; }

            internal int? ExitCode => _process.HasExited ? _process.ExitCode : null;

            internal ProcessOutput(Process process, Action<string> onOutputLine)
            {
                _process = process;
                _executable = process.StartInfo.FileName;
                _workingDirectory = string.IsNullOrEmpty(process.StartInfo.WorkingDirectory) ? Directory.GetCurrentDirectory() : process.StartInfo.WorkingDirectory;
                try
                {
                    _resolvedExecutable = process.MainModule?.FileName;
                }
                catch (Exception error) when (error is Win32Exception or InvalidOperationException or NotSupportedException or NullReferenceException)
                {
                    // An early exit can make the image unavailable.
                    // .NET Framework can also throw NullReferenceException from ProcessModule.FileName while inspecting a child.
                }

                var exited = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
                process.Exited += OnExit;
                process.EnableRaisingEvents = true;
                if (process.HasExited)
                    exited.TrySetResult(true);

                // Start both readers immediately.
                // Keep draining after a server announces readiness - otherwise later console output can fill a pipe and block the running server.
                var outputReadStreamTask = ReadStream(process.StandardOutput, _standardOutput, onOutputLine);
                var errorOutputReadStreamTask = ReadStream(process.StandardError, _standardError, onLine: null);
                Completion = CompleteAsync();
                _ = Completion.IgnoreUnobservedExceptions();
                return;

                void OnExit(object? sender, EventArgs args) => exited.TrySetResult(true);

                async Task CompleteAsync()
                {
                    try
                    {
                        await Task.WhenAll(outputReadStreamTask, errorOutputReadStreamTask, exited.Task).ConfigureAwait(false);
                    }
                    finally
                    {
                        process.Exited -= OnExit;
                    }
                }
            }

            internal async Task DrainAsync(TimeSpan timeout)
            {
                try
                {
                    // Let the readers consume trailing output, including unterminated lines,
                    // before the caller builds its diagnostic or disposes the process.
                    if (await Completion.WaitWithTimeout(timeout).ConfigureAwait(false))
                        await Completion.ConfigureAwait(false);
                }
                catch (Exception error) when (error is Win32Exception or InvalidOperationException or IOException)
                {
                    // Cleanup must not replace the original startup/discovery failure.
                }
            }

            internal void AppendDiagnostics(StringBuilder message, int? exitCode)
            {
                // Server arguments can contain license keys and certificate passwords.
                // Report the executable here; callers may add arguments only when they are known to be safe.
                message.AppendLine($"Executable: '{_executable}'");
                message.AppendLine($"Working directory: '{_workingDirectory}'");
                message.AppendLine($"Resolved executable: {_resolvedExecutable ?? "unavailable (the process may have exited before it could be queried)"}");
                if (exitCode.HasValue)
                {
                    message.AppendLine($"Exit code: {exitCode.Value} (0x{exitCode.Value:X8})");
                    if (PlatformDetails.RunningOnPosix == false && exitCode.Value == unchecked((int)0xC0000135))
                        message.AppendLine("Windows reported STATUS_DLL_NOT_FOUND: a DLL required by the process could not be found.");
                }
                message.AppendLine("Standard output:");
                message.AppendLine(_standardOutput.ToString());
                message.AppendLine("Standard error:");
                message.AppendLine(_standardError.ToString());
            }

            private static async Task ReadStream(StreamReader stream, CapturedOutput output, Action<string>? onLine)
            {
                string? line;
                while ((line = await stream.ReadLineAsync().ConfigureAwait(false)) != null)
                {
                    output.AppendLine(line);
                    onLine?.Invoke(line);
                }
            }

            private sealed class CapturedOutput
            {
                private readonly StringBuilder _text = new();

                internal void AppendLine(string line)
                {
                    lock (_text)
                    {
                        _text.AppendLine(line);
                    }
                }

                public override string ToString()
                {
                    lock (_text)
                    {
                        return _text.Length == 0
                            ? "<empty>"
                            : _text.ToString();
                    }
                }
            }
        }
    }
}

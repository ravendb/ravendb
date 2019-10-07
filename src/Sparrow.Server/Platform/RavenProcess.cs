using System;
using System.Diagnostics;
using System.IO;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Win32.SafeHandles;
using Sparrow.Logging;

namespace Sparrow.Server.Platform
{
    public class ProcessExitedEventArgs : EventArgs
    {
        public int ExitCode { get; set; }
        public IntPtr Pid { get; set; }
    }

    public class RavenProcess : IDisposable
    {
        public ProcessStartInfo StartInfo { get; set; }
        private bool _hasExited;

        public event EventHandler<ProcessExitedEventArgs> ProcessExited;
        private void OnProcessExited(ProcessExitedEventArgs e)
        {
            var handler = ProcessExited;
            handler?.Invoke(this, e);
        }

        public event Action<object, string> LineOutput;
        private void OnLineOutput(string line, CancellationToken ctk)
        {
            if (ctk.IsCancellationRequested == false)
            {
                var handler = LineOutput;
                handler?.Invoke(this, line);
            }
        }

        public delegate void StreamWriteDelegate(Span<byte> bytes);

        private readonly Logger _logger = LoggingSource.Instance.GetLogger<RavenProcess>("RavenProcess");
        private IntPtr _pid = IntPtr.Zero;
        private SafeFileHandle StandardOutAndErr { get; set; }
        private SafeFileHandle StandardIn { get; set; }

        public void Start(CancellationToken ctk)
        {
            if (StartInfo?.FileName == null)
                throw new InvalidOperationException($"RavenProcess {nameof(Start)} must be supplied with valid {nameof(StartInfo)} object and set {nameof(StartInfo.FileName)}");

            var rc = Pal.rvn_spawn_process(StartInfo.FileName, StartInfo.Arguments, out var pid, out var stdin, out var stdout, out var errorCode);
            if (rc != PalFlags.FailCodes.Success)
                PalHelper.ThrowLastError(rc, errorCode, $"Failed to spawn command '{StartInfo.FileName} {StartInfo.Arguments}'");

            _pid = pid;
            StandardOutAndErr = stdout;
            StandardIn = stdin;
        }

        public static void Start(string filename, string arguments, StreamWriteDelegate outputDel)
        {
            using (var ravenProcess = new RavenProcess {StartInfo = new ProcessStartInfo {FileName = filename, Arguments = arguments}})
            {
                ravenProcess.Start(CancellationToken.None);
                ravenProcess.ReadTo(outputDel);
            }
        }

        private void ReadTo(StreamWriteDelegate outputDel)
        {
            var bytes = new byte[4096];
            using (var fs = new FileStream(StandardOutAndErr, FileAccess.Read))
            {
                var read = fs.Read(bytes, 0, 4096);
                while (read != 0)
                {
                    outputDel?.Invoke(new Span<byte>(bytes, 0, read));
                    read = fs.Read(bytes, 0, 4096);
                }
            }
        }

        private async Task<string> ReadToEndAsync()
        {
            using (var fs = new FileStream(StandardOutAndErr, FileAccess.Read))
            using (var sr = new StreamReader(fs))
            {
                return await sr.ReadToEndAsync().ConfigureAwait(false);
            }
        }

        private void Kill()
        {
            if (_pid != IntPtr.Zero)
            {
                var rc = Pal.rvn_kill_process(_pid, out var errorCode);
                if (rc != PalFlags.FailCodes.Success)
                    PalHelper.ThrowLastError(rc, errorCode, $"Failed to kill proc id={_pid.ToInt64()}. Command: '{StartInfo.FileName} {StartInfo.Arguments}'");
            }
        }

        private void ReadLines(StreamReader sr, Action<object, string> lineOutputHandler, CancellationToken ctk)
        {
            while (ctk.IsCancellationRequested == false)
            {
                var lineTask = sr.ReadLineAsync();
                if (lineTask.Wait(Timeout.Infinite, ctk) == false)
                    break;
                var line = lineTask.Result;
                OnLineOutput(line, ctk);
                if (line == null)
                    break;
            }
        }

        public static void Execute(string command, string arguments, int waitForExitTimeoutInSeconds, EventHandler<ProcessExitedEventArgs> exitHandler, Action<object, string> lineOutputHandler, CancellationToken ctk)
        {
            var startInfo = new ProcessStartInfo
            {
                FileName = command,
                Arguments = arguments
            };
            using (var process = new RavenProcess { StartInfo = startInfo })
            {
                if (exitHandler != null)
                    process.ProcessExited += exitHandler;
                if (lineOutputHandler != null)
                    process.LineOutput += lineOutputHandler;
                process.Start(ctk);
                using (var fs = new FileStream(process.StandardOutAndErr, FileAccess.Read))
                using (var sr = new StreamReader(fs, Encoding.UTF8))
                {
                    try
                    {
                        process.ReadLines(sr, lineOutputHandler, ctk);
                    }
                    catch
                    {
                        // ignore
                    }

                    var rc = Pal.rvn_wait_for_close_process(process._pid, waitForExitTimeoutInSeconds, out var exitCode, out var errorCode);
                    if (rc == PalFlags.FailCodes.Success ||
                        rc == PalFlags.FailCodes.FailChildProcessFailure)
                    {
                        process._hasExited = true;
                        var args = new ProcessExitedEventArgs
                        {
                            ExitCode = exitCode,
                            Pid = process._pid
                        };
                        process.OnProcessExited(args);
                    }
                }
            }
        }

        public void Dispose()
        {
            if (_hasExited == false)
            {
                _hasExited = true;
                var task = new Task(async () =>
                {
                    try
                    {
                        await ReadToEndAsync().ConfigureAwait(false);
                    }
                    catch
                    {
                        // nothing.. just flush
                    }
                });
                try
                {
                    task.Start();
                }
                catch
                {
                    // nothing.. just flush
                }



                var rc = Pal.rvn_wait_for_close_process(_pid, 5, out var exitCode, out var errorCode);
                if (rc != PalFlags.FailCodes.FailTimeout)
                    return;

                if (_logger.IsInfoEnabled)
                    _logger.Info($"Waited 5 seconds for {StartInfo.FileName} to close, but it didn't, trying to kill");
                try
                {
                    Kill();
                }
                catch (Exception ex)
                {
                    if (_logger.IsOperationsEnabled)
                        _logger.Operations($"Kill {StartInfo.FileName} failed", ex);
                }

                try
                {
                    if (task.IsFaulted == false)
                        task.Wait(TimeSpan.FromSeconds(15));
                }
                catch
                {
                    // nothing.. just flush
                }
            }
        }
    }
}
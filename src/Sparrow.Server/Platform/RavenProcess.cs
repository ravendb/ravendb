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
    public class RavenProcess
    {
        private readonly Logger _logger = LoggingSource.Instance.GetLogger<RavenProcess>("RavenProcess");
        private EventHandler _onExited;

        public IntPtr Pid { get; }
        private SafeFileHandle StandardOutAndErr { get; set; }
        public SafeFileHandle StandardIn { get; set; }
        public ProcessStartInfo StartInfo { get; set; }
        public int ExitCode = -1;
        public bool HasExited;

        public event EventHandler Exited
        {
            add { this._onExited += value; }
            remove { this._onExited -= value; }
        }

        public CancellationTokenSource cts = new CancellationTokenSource();

        public void WaitForClose()
        {
            Console.WriteLine("ADIADI::WaitForClose : " + StartInfo.FileName + " " + StartInfo.Arguments);
            while (true)
            {
                if (cts.Token.IsCancellationRequested)
                    break;
                var rc = Pal.rvn_wait_for_close_process(Pid, 1000, out var exitCode, out var errorCode);
                if (rc != PalFlags.FailCodes.FailTimeout)
                {
                    HasExited = true;
                    ExitCode = exitCode;
                    _onExited?.Invoke(this, new EventArgs());
                    break;
                }
            }
            Console.WriteLine("ADIADI::WaitForClose(Exit) : " + StartInfo.FileName + " " + StartInfo.Arguments);
        }

        public RavenProcess Start(Task waitForCloseTask)
        {
            var rc = Start();
            waitForCloseTask.Start();
            return rc;
        }

        public RavenProcess Start(bool dumpStdoutToConsole = true)
        {
            if (StartInfo?.FileName == null)
                throw new InvalidOperationException("RavenProcess Start() must be supplied with valid startInfo object and set Filename");

            Console.WriteLine("ADIADI::spawning : " + StartInfo.FileName + " " + StartInfo.Arguments);
            var rc = Pal.rvn_spawn_process(StartInfo.FileName, StartInfo.Arguments, out var pid, out var stdin, out var stdout, out var errorCode);
            if (rc != PalFlags.FailCodes.Success)
                PalHelper.ThrowLastError(rc, errorCode, $"Failed to spawn command '{StartInfo.FileName} {StartInfo.Arguments}'");

            if (dumpStdoutToConsole)
            {
                ReadTo(Console.Out);
            }

            StandardOutAndErr = stdout;
            StandardIn = stdin;

            return this;
        }

        public static RavenProcess Start(string filename, string arguments, bool dumpStdoutToConsole = true)
        {
            var ravenProcess = new RavenProcess {StartInfo = new ProcessStartInfo {FileName = filename, Arguments = arguments}};

            return ravenProcess.Start(dumpStdoutToConsole);
        }

        public static RavenProcess Start(ProcessStartInfo startInfo, bool dumpStdoutToConsole = true)
        {
            var ravenProcess = new RavenProcess();
            ravenProcess.StartInfo = startInfo;
            return ravenProcess.Start(dumpStdoutToConsole);
        }

        private void ReadTo(TextWriter output)
        {
            using (var fs = new FileStream(StandardOutAndErr, FileAccess.Read))
            {
                var buffer = new byte[4096];
                var read = fs.Read(buffer, 0, 4096);
                while (read != 0)
                {
                    output.Write(Encoding.UTF8.GetString(buffer, 0, read));
                    output.Flush();
                    if (output == Console.Out)
                        Console.Out.Flush();
                    read = fs.Read(buffer, 0, 4096);
                }
            }
        }

        public Task<string> ReadToEndAsync()
        {
            Console.WriteLine("ADIADI::ReadToEndAsync : " + StartInfo.FileName + " " + StartInfo.Arguments);
            using (var fs = new FileStream(StandardOutAndErr, FileAccess.Read))
            using (var sr = new StreamReader(fs))
            {
                return sr.ReadToEndAsync();
            }
        }

        public bool WaitForExit(int timeoutInMs)
        {
            Console.WriteLine("ADIADI::WaitForExit : " + StartInfo.FileName + " " + StartInfo.Arguments);
            var rc = Pal.rvn_wait_for_close_process(Pid, timeoutInMs, out var exitCode, out var errorCode);
            if (rc == PalFlags.FailCodes.Success)
            {
                HasExited = true;
                return true;
            }

            if (_logger.IsInfoEnabled)
                _logger.Info($"For {StartInfo.FileName}");
            return false;

        }

        public void Kill()
        {
            Console.WriteLine("ADIADI::Kill : " + StartInfo.FileName + " " + StartInfo.Arguments);
            var rc = Pal.rvn_kill_process(Pid, out var errorCode);
            if (rc != PalFlags.FailCodes.Success)
                PalHelper.ThrowLastError(rc, errorCode, $"Failed to spawn command '{StartInfo.FileName} {StartInfo.Arguments}'");
        }

        public static void Execute(string command, string arguments, MemoryStream ms, int timeoutInMs)
        {
            Console.WriteLine("ADIADI::Execute " + command + " " + arguments);
            var startInfo = new ProcessStartInfo
            {
                FileName = command,
                Arguments = arguments,
                UseShellExecute = false,
                RedirectStandardOutput = true,
                RedirectStandardError = true,
                CreateNoWindow = true
            };

            RavenProcess process;

            try
            {
                process = RavenProcess.Start(startInfo);
            }
            catch (Exception e)
            {
                throw new InvalidOperationException($"Unable to get cpu credits by executing {command} {arguments}. Failed to start process.", e);
            }

            var readErrors = process.ReadToEndAsync();
            using (var fs = new FileStream(process.StandardOutAndErr, FileAccess.Read))
            {
                string GetStdError()
                {
                    try
                    {
                        return readErrors.Result;
                    }
                    catch
                    {
                        return "Unable to get stdout";
                    }
                }

                try
                {
                    readErrors.Wait(timeoutInMs);
                }
                catch (Exception e)
                {
                    throw new InvalidOperationException(
                        $"Unable to get cpu credits by executing {command} {arguments}, waited for {timeoutInMs}ms but the process didn't exit. Stderr: {GetStdError()}",
                        e);
                }


                if (process.WaitForExit(timeoutInMs) == false)
                {
                    process.Kill();

                    throw new InvalidOperationException(
                        $"Unable to get cpu credits by executing {command} {arguments}, waited for {timeoutInMs}ms but the process didn't exit. Stderr: {GetStdError()}");
                }

                process.HasExited = true;
            }
        }

        public Task<string> ReadLineAsync()
        {
            Console.WriteLine("ADIADI::ReadLineAsync : " + StartInfo.FileName + " " + StartInfo.Arguments);
            using (var fs = new FileStream(StandardOutAndErr, FileAccess.Read))
            using (var sr = new StreamReader(fs))
            {
                return sr.ReadLineAsync();
            }
        }
    }
}

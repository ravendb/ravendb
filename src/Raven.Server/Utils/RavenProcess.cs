using System;
using System.IO;
using Custom.Raven.System.Diagnostics;
using Sparrow.Platform;

namespace Raven.Server.Utils
{
    public class RavenProcess : IDisposable
    {
        private readonly System.Diagnostics.Process _posixProcess;
        private readonly Process _windowsProcess;

        private RavenProcess(System.Diagnostics.Process process)
        {
            _posixProcess = process ?? throw new ArgumentNullException(nameof(process));
        }

        private RavenProcess(Process process)
        {
            _windowsProcess = process ?? throw new ArgumentNullException(nameof(process));
        }

        public StreamWriter StandardInput => _windowsProcess != null ? _windowsProcess.StandardInput : _posixProcess.StandardInput;
        public StreamReader StandardError => _windowsProcess != null ? _windowsProcess.StandardError : _posixProcess.StandardError;
        public StreamReader StandardOutput => _windowsProcess != null ? _windowsProcess.StandardOutput : _posixProcess.StandardOutput;
        public bool HasExited => _windowsProcess?.HasExited ?? _posixProcess.HasExited;
        public int ExitCode => _windowsProcess?.ExitCode ?? _posixProcess.ExitCode;

        public event EventHandler Exited
        {
            add
            {
                if (_windowsProcess != null)
                {
                    _windowsProcess.Exited += value;
                    return;
                }

                _posixProcess.Exited += value;
            }

            remove
            {
                if (_windowsProcess != null)
                {
                    _windowsProcess.Exited -= value;
                    return;
                }

                _posixProcess.Exited -= value;
            }
        }

        public static RavenProcess Start(ProcessStartInfo startInfo, bool? enableRaisingEvents = null)
        {
            if (PlatformDetails.RunningOnPosix)
            {
                var posixProcess = new System.Diagnostics.Process();
                if (enableRaisingEvents.HasValue)
                    posixProcess.EnableRaisingEvents = enableRaisingEvents.Value;

                posixProcess.StartInfo = Convert(startInfo);
                posixProcess.Start();

                return new RavenProcess(posixProcess);
            }

            var windowsProcess = new Process();
            if (enableRaisingEvents.HasValue)
                windowsProcess.EnableRaisingEvents = enableRaisingEvents.Value;

            windowsProcess.StartInfo = startInfo;
            windowsProcess.Start();

            return new RavenProcess(windowsProcess);
        }

        public void Kill()
        {
            if (_windowsProcess != null)
            {
                _windowsProcess.Kill();
                return;
            }

            _posixProcess.Kill();
        }
        
        public bool WaitForExit(int milliseconds)
        {
            return _windowsProcess?.WaitForExit(milliseconds) ?? _posixProcess.WaitForExit(milliseconds);
        }

        public void Dispose()
        {
            _posixProcess?.Dispose();
            _windowsProcess?.Dispose();
        }

        private static System.Diagnostics.ProcessStartInfo Convert(ProcessStartInfo startInfo)
        {
            return new System.Diagnostics.ProcessStartInfo
            {
                //Environment = startInfo.Environment,
                //EnvironmentVariables = startInfo.EnvironmentVariables,
                StandardOutputEncoding = startInfo.StandardOutputEncoding,
                Arguments = startInfo.Arguments,
                UseShellExecute = startInfo.UseShellExecute,
                //ArgumentList = startInfo.ArgumentList,
                CreateNoWindow = startInfo.CreateNoWindow,
                Domain = startInfo.Domain,
                ErrorDialog = startInfo.ErrorDialog,
                ErrorDialogParentHandle = startInfo.ErrorDialogParentHandle,
                FileName = startInfo.FileName,
                LoadUserProfile = startInfo.LoadUserProfile,
                Password = startInfo.Password,
                PasswordInClearText = startInfo.PasswordInClearText,
                RedirectStandardError = startInfo.RedirectStandardError,
                RedirectStandardInput = startInfo.RedirectStandardInput,
                RedirectStandardOutput = startInfo.RedirectStandardOutput,
                StandardErrorEncoding = startInfo.StandardErrorEncoding,
                StandardInputEncoding = startInfo.StandardInputEncoding,
                UserName = startInfo.UserName,
                Verb = startInfo.Verb,
                //WindowStyle = startInfo.WindowStyle,
                WorkingDirectory = startInfo.WorkingDirectory
            };
        }
    }
}

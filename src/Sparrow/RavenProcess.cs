using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Diagnostics;
using System.IO;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading;
using Microsoft.Win32.SafeHandles;
using Sparrow.Platform;

namespace Sparrow
{
    internal class RavenProcess : IDisposable
    {
        private readonly Process _process;

        private static readonly object CreateProcessLock = new object();

        private StreamReader _standardOutput;
        private StreamWriter _standardInput;
        private StreamReader _standardError;
        private int _processId;
        private SafeProcessHandle _processHandle;
        private bool _haveProcessHandle;
        private bool _signaled;
        private int? _exitCode;

        private RavenProcess()
        {
        }

        private RavenProcess(Process process)
        {
            _process = process ?? throw new ArgumentNullException(nameof(process));
        }

        public StreamReader StandardOutput
        {
            get
            {
                if (_process != null)
                    return _process.StandardOutput;

                return _standardOutput;
            }
        }

        public StreamReader StandardError
        {
            get
            {
                if (_process != null)
                    return _process.StandardError;

                return _standardError;
            }
        }

        public StreamWriter StandardInput
        {
            get
            {
                if (_process != null)
                    return _process.StandardInput;

                return _standardInput;
            }
        }

        public int ExitCode
        {
            get
            {
                if (_process != null)
                    return _process.ExitCode;

                if (_exitCode.HasValue)
                    return _exitCode.Value;

                using (SafeProcessHandle handle = GetProcessHandle(Win32.ProcessOptions.PROCESS_QUERY_LIMITED_INFORMATION | Win32.ProcessOptions.SYNCHRONIZE, false))
                {
                    if (Win32.GetExitCodeProcess(handle, out var localExitCode) && localExitCode != Win32.HandleOptions.STILL_ACTIVE)
                    {
                        _exitCode = localExitCode;
                        return localExitCode;
                    }

                    // The best check for exit is that the kernel process object handle is invalid, 
                    // or that it is valid and signaled.  Checking if the exit code != STILL_ACTIVE 
                    // does not guarantee the process is closed,
                    // since some process could return an actual STILL_ACTIVE exit code (259).
                    if (!_signaled) // if we just came from WaitForExit, don't repeat
                    {
                        using (var wh = new Win32.ProcessWaitHandle(handle))
                        {
                            _signaled = wh.WaitOne(0);
                        }
                    }

                    if (_signaled)
                    {
                        if (!Win32.GetExitCodeProcess(handle, out localExitCode))
                            throw new Win32Exception();

                        _exitCode = localExitCode;
                        return localExitCode;
                    }

                    return -1;
                }
            }
        }

        public static RavenProcess Start(ProcessStartInfo startInfo)
        {
            if (startInfo.UseShellExecute)
                throw new NotSupportedException();

            if (PlatformDetails.RunningOnPosix)
                return new RavenProcess(Process.Start(startInfo));

            var process = new RavenProcess();
            if (process.StartWithCreateProcess(startInfo) == false)
                throw new InvalidOperationException();

            return process;
        }

        public bool WaitForExit(int milliseconds)
        {
            if (_process != null)
                return _process.WaitForExit(milliseconds);

            SafeProcessHandle handle = null;
            try
            {
                handle = GetProcessHandle(Win32.ProcessOptions.SYNCHRONIZE, false);
                if (handle.IsInvalid)
                    return true;

                using (Win32.ProcessWaitHandle processWaitHandle = new Win32.ProcessWaitHandle(handle))
                {
                    return _signaled = processWaitHandle.WaitOne(milliseconds);
                }
            }
            finally
            {
                handle?.Dispose();
            }
        }

        public void Kill()
        {
            if (_process != null)
            {
                _process.Kill();
                return;
            }

            using (SafeProcessHandle handle = GetProcessHandle(Win32.ProcessOptions.PROCESS_TERMINATE))
            {
                if (!Win32.TerminateProcess(handle, -1))
                    throw new Win32Exception();
            }
        }

        public void Dispose()
        {
            if (_process != null)
            {
                _process.Dispose();
                return;
            }

            _processHandle?.Dispose();
            _standardOutput?.Dispose();
            _standardInput?.Dispose();
            _standardError?.Dispose();
        }

        private unsafe bool StartWithCreateProcess(ProcessStartInfo startInfo)
        {
            var commandLine = BuildCommandLine(startInfo.FileName, startInfo.Arguments);

            var startupInfo = new Win32.STARTUPINFO();
            var processInfo = new Win32.PROCESS_INFORMATION();
            var unused_SecAttrs = new Win32.SECURITY_ATTRIBUTES();
            SafeProcessHandle procSH = null;
            //SafeThreadHandle threadSH = new SafeThreadHandle();
            // handles used in parent process
            SafeFileHandle parentInputPipeHandle = null;
            SafeFileHandle childInputPipeHandle = null;
            SafeFileHandle parentOutputPipeHandle = null;
            SafeFileHandle childOutputPipeHandle = null;
            SafeFileHandle parentErrorPipeHandle = null;
            SafeFileHandle childErrorPipeHandle = null;
            lock (CreateProcessLock)
            {
                try
                {
                    startupInfo.cb = sizeof(Win32.STARTUPINFO);

                    // set up the streams
                    if (startInfo.RedirectStandardInput || startInfo.RedirectStandardOutput || startInfo.RedirectStandardError)
                    {
                        if (startInfo.RedirectStandardInput)
                        {
                            CreatePipe(out parentInputPipeHandle, out childInputPipeHandle, true);
                        }
                        else
                        {
                            childInputPipeHandle = new SafeFileHandle(Win32.GetStdHandle(Win32.HandleTypes.STD_INPUT_HANDLE), false);
                        }

                        if (startInfo.RedirectStandardOutput)
                        {
                            CreatePipe(out parentOutputPipeHandle, out childOutputPipeHandle, false);
                        }
                        else
                        {
                            childOutputPipeHandle = new SafeFileHandle(Win32.GetStdHandle(Win32.HandleTypes.STD_OUTPUT_HANDLE), false);
                        }

                        if (startInfo.RedirectStandardError)
                        {
                            CreatePipe(out parentErrorPipeHandle, out childErrorPipeHandle, false);
                        }
                        else
                        {
                            childErrorPipeHandle = new SafeFileHandle(Win32.GetStdHandle(Win32.HandleTypes.STD_ERROR_HANDLE), false);
                        }

                        startupInfo.hStdInput = childInputPipeHandle.DangerousGetHandle();
                        startupInfo.hStdOutput = childOutputPipeHandle.DangerousGetHandle();
                        startupInfo.hStdError = childErrorPipeHandle.DangerousGetHandle();

                        startupInfo.dwFlags = Win32.StartupInfoOptions.STARTF_USESTDHANDLES;
                    }

                    // set up the creation flags parameter
                    int creationFlags = 0;
                    if (startInfo.CreateNoWindow)
                        creationFlags |= Win32.StartupInfoOptions.CREATE_NO_WINDOW;

                    // set up the environment block parameter
                    string environmentBlock = null;
                    if (startInfo.Environment != null)
                    {
                        creationFlags |= Win32.StartupInfoOptions.CREATE_UNICODE_ENVIRONMENT;
                        environmentBlock = GetEnvironmentVariablesBlock(startInfo.Environment);
                    }
                    string workingDirectory = startInfo.WorkingDirectory;
                    if (workingDirectory == string.Empty)
                        workingDirectory = Directory.GetCurrentDirectory();

                    bool retVal;
                    int errorCode = 0;

                    if (startInfo.UserName.Length != 0)
                    {
                        throw new NotSupportedException();
                    }
                    else
                    {
                        fixed (char* environmentBlockPtr = environmentBlock)
                        {
                            retVal = Win32.CreateProcess(
                                null,                // we don't need this since all the info is in commandLine
                                commandLine,         // pointer to the command line string
                                ref unused_SecAttrs, // address to process security attributes, we don't need to inherit the handle
                                ref unused_SecAttrs, // address to thread security attributes.
                                false,               // handle inheritance flag
                                creationFlags,       // creation flags
                                (IntPtr)environmentBlockPtr, // pointer to new environment block
                                workingDirectory,    // pointer to current directory name
                                ref startupInfo,     // pointer to STARTUPINFO
                                ref processInfo      // pointer to PROCESS_INFORMATION
                            );

                            if (!retVal)
                                errorCode = Marshal.GetLastWin32Error();
                        }
                    }

                    if (processInfo.hProcess != IntPtr.Zero && processInfo.hProcess != new IntPtr(-1))
                        procSH = new SafeProcessHandle(processInfo.hProcess, true);
                    //if (processInfo.hThread != IntPtr.Zero && processInfo.hThread != new IntPtr(-1))
                    //    threadSH.InitialSetHandle(processInfo.hThread);

                    if (!retVal)
                    {
                        throw new Win32Exception(errorCode);
                    }
                }
                finally
                {
                    childInputPipeHandle?.Dispose();
                    childOutputPipeHandle?.Dispose();
                    childErrorPipeHandle?.Dispose();
                }
            }

            if (startInfo.RedirectStandardInput)
            {
                Encoding enc = startInfo.StandardOutputEncoding ?? Encoding.UTF8;
                _standardInput = new StreamWriter(new FileStream(parentInputPipeHandle, FileAccess.Write, 4096, false), enc, 4096);
                _standardInput.AutoFlush = true;
            }
            if (startInfo.RedirectStandardOutput)
            {
                Encoding enc = startInfo.StandardOutputEncoding ?? Encoding.UTF8;
                _standardOutput = new StreamReader(new FileStream(parentOutputPipeHandle, FileAccess.Read, 4096, false), enc, true, 4096);
            }
            if (startInfo.RedirectStandardError)
            {
                Encoding enc = startInfo.StandardErrorEncoding ?? Encoding.UTF8;
                _standardError = new StreamReader(new FileStream(parentErrorPipeHandle, FileAccess.Read, 4096, false), enc, true, 4096);
            }

            if (procSH == null || procSH.IsInvalid)
                return false;

            SetProcessHandle(procSH);
            SetProcessId((int)processInfo.dwProcessId);
            return true;
        }

        private void SetProcessId(int processId)
        {
            _processId = processId;
        }

        private void SetProcessHandle(SafeProcessHandle handle)
        {
            _processHandle = handle;
            _haveProcessHandle = true;
        }

        private static void CreatePipeWithSecurityAttributes(out SafeFileHandle hReadPipe, out SafeFileHandle hWritePipe, ref Win32.SECURITY_ATTRIBUTES lpPipeAttributes, int nSize)
        {
            bool ret = Win32.CreatePipe(out hReadPipe, out hWritePipe, ref lpPipeAttributes, nSize);
            if (!ret || hReadPipe.IsInvalid || hWritePipe.IsInvalid)
            {
                throw new Win32Exception();
            }
        }

        private static void CreatePipe(out SafeFileHandle parentHandle, out SafeFileHandle childHandle, bool parentInputs)
        {
            Win32.SECURITY_ATTRIBUTES securityAttributesParent = new Win32.SECURITY_ATTRIBUTES();
            securityAttributesParent.bInheritHandle = Win32.BOOL.TRUE;

            SafeFileHandle hTmp = null;
            try
            {
                if (parentInputs)
                {
                    CreatePipeWithSecurityAttributes(out childHandle, out hTmp, ref securityAttributesParent, 0);
                }
                else
                {
                    CreatePipeWithSecurityAttributes(out hTmp,
                                                          out childHandle,
                                                          ref securityAttributesParent,
                                                          0);
                }
                // Duplicate the parent handle to be non-inheritable so that the child process 
                // doesn't have access. This is done for correctness sake, exact reason is unclear.
                // One potential theory is that child process can do something brain dead like 
                // closing the parent end of the pipe and there by getting into a blocking situation
                // as parent will not be draining the pipe at the other end anymore. 
                SafeProcessHandle currentProcHandle = Win32.GetCurrentProcess();
                if (!Win32.DuplicateHandle(currentProcHandle,
                                                     hTmp,
                                                     currentProcHandle,
                                                     out parentHandle,
                                                     0,
                                                     false,
                                                     Win32.HandleOptions.DUPLICATE_SAME_ACCESS))
                {
                    throw new Win32Exception();
                }
            }
            finally
            {
                if (hTmp != null && !hTmp.IsInvalid)
                {
                    hTmp.Dispose();
                }
            }
        }

        private static StringBuilder BuildCommandLine(string executableFileName, string arguments)
        {
            // Construct a StringBuilder with the appropriate command line
            // to pass to CreateProcess.  If the filename isn't already 
            // in quotes, we quote it here.  This prevents some security
            // problems (it specifies exactly which part of the string
            // is the file to execute).
            StringBuilder commandLine = new StringBuilder();
            string fileName = executableFileName.Trim();
            bool fileNameIsQuoted = (fileName.StartsWith("\"", StringComparison.Ordinal) && fileName.EndsWith("\"", StringComparison.Ordinal));
            if (!fileNameIsQuoted)
            {
                commandLine.Append("\"");
            }

            commandLine.Append(fileName);

            if (!fileNameIsQuoted)
            {
                commandLine.Append("\"");
            }

            if (!string.IsNullOrEmpty(arguments))
            {
                commandLine.Append(" ");
                commandLine.Append(arguments);
            }

            return commandLine;
        }

        private static string GetEnvironmentVariablesBlock(IDictionary<string, string> sd)
        {
            // get the keys
            string[] keys = new string[sd.Count];
            sd.Keys.CopyTo(keys, 0);

            // sort both by the keys
            // Windows 2000 requires the environment block to be sorted by the key
            // It will first converting the case the strings and do ordinal comparison.

            // We do not use Array.Sort(keys, values, IComparer) since it is only supported
            // in System.Runtime contract from 4.20.0.0 and Test.Net depends on System.Runtime 4.0.10.0
            // we workaround this by sorting only the keys and then lookup the values form the keys.
            Array.Sort(keys, StringComparer.OrdinalIgnoreCase);

            // create a list of null terminated "key=val" strings
            StringBuilder stringBuff = new StringBuilder();
            for (int i = 0; i < sd.Count; ++i)
            {
                stringBuff.Append(keys[i]);
                stringBuff.Append('=');
                stringBuff.Append(sd[keys[i]]);
                stringBuff.Append('\0');
            }
            // an extra null at the end that indicates end of list will come from the string.
            return stringBuff.ToString();
        }

        private SafeProcessHandle GetProcessHandle(int access)
        {
            return GetProcessHandle(access, true);
        }

        private SafeProcessHandle GetProcessHandle(int access, bool throwIfExited)
        {
            if (_haveProcessHandle)
            {
                if (throwIfExited)
                {
                    // Since haveProcessHandle is true, we know we have the process handle
                    // open with at least SYNCHRONIZE access, so we can wait on it with 
                    // zero timeout to see if the process has exited.
                    using (Win32.ProcessWaitHandle waitHandle = new Win32.ProcessWaitHandle(_processHandle))
                    {
                        if (waitHandle.WaitOne(0))
                        {
                            //if (_haveProcessId)
                            //    throw new InvalidOperationException(SR.Format(SR.ProcessHasExited, _processId.ToString(CultureInfo.CurrentCulture)));
                            //else
                            throw new InvalidOperationException();
                        }
                    }
                }

                // If we dispose of our contained handle we'll be in a bad state. NetFX dealt with this
                // by doing a try..finally around every usage of GetProcessHandle and only disposed if
                // it wasn't our handle.
                return new SafeProcessHandle(_processHandle.DangerousGetHandle(), ownsHandle: false);
            }

            throw new NotSupportedException();
        }

        internal static class Win32
        {
            [DllImport("kernel32.dll", SetLastError = true)]
            internal static extern bool GetExitCodeProcess(SafeProcessHandle processHandle, out int exitCode);

            [DllImport("kernel32.dll", CharSet = CharSet.Unicode, SetLastError = true)]
            internal static extern bool TerminateProcess(SafeProcessHandle processHandle, int exitCode);

            [DllImport("kernel32.dll", SetLastError = true, BestFitMapping = false)]
            internal static extern bool DuplicateHandle(
                SafeProcessHandle hSourceProcessHandle,
                SafeHandle hSourceHandle,
                SafeProcessHandle hTargetProcess,
                out SafeFileHandle targetHandle,
                int dwDesiredAccess,
                bool bInheritHandle,
                int dwOptions
            );

            [DllImport("kernel32.dll", SetLastError = true, BestFitMapping = false)]
            internal static extern bool DuplicateHandle(
                SafeProcessHandle hSourceProcessHandle,
                SafeHandle hSourceHandle,
                SafeProcessHandle hTargetProcess,
                out SafeWaitHandle targetHandle,
                int dwDesiredAccess,
                bool bInheritHandle,
                int dwOptions
            );

            [DllImport("kernel32.dll", SetLastError = true)]
            internal static extern SafeProcessHandle GetCurrentProcess();

            [DllImport("kernel32.dll", SetLastError = true)]
            internal static extern bool CreatePipe(out SafeFileHandle hReadPipe, out SafeFileHandle hWritePipe, ref SECURITY_ATTRIBUTES lpPipeAttributes, int nSize);

            [DllImport("kernel32.dll", SetLastError = true)]
            internal static extern IntPtr GetStdHandle(int nStdHandle);  // param is NOT a handle, but it returns one!

            [DllImport("kernel32.dll", CharSet = CharSet.Unicode, SetLastError = true, BestFitMapping = false, EntryPoint = "CreateProcessW")]
            internal static extern bool CreateProcess(
                string lpApplicationName,
                StringBuilder lpCommandLine,
                ref SECURITY_ATTRIBUTES procSecAttrs,
                ref SECURITY_ATTRIBUTES threadSecAttrs,
                bool bInheritHandles,
                int dwCreationFlags,
                IntPtr lpEnvironment,
                string lpCurrentDirectory,
                ref STARTUPINFO lpStartupInfo,
                ref PROCESS_INFORMATION lpProcessInformation
            );

            [StructLayout(LayoutKind.Sequential)]
            internal struct PROCESS_INFORMATION
            {
                internal IntPtr hProcess;
                internal IntPtr hThread;
                internal int dwProcessId;
                internal int dwThreadId;
            }

            [StructLayout(LayoutKind.Sequential)]
            internal struct STARTUPINFO
            {
                internal int cb;
                internal IntPtr lpReserved;
                internal IntPtr lpDesktop;
                internal IntPtr lpTitle;
                internal int dwX;
                internal int dwY;
                internal int dwXSize;
                internal int dwYSize;
                internal int dwXCountChars;
                internal int dwYCountChars;
                internal int dwFillAttribute;
                internal int dwFlags;
                internal short wShowWindow;
                internal short cbReserved2;
                internal IntPtr lpReserved2;
                internal IntPtr hStdInput;
                internal IntPtr hStdOutput;
                internal IntPtr hStdError;
            }

            [StructLayout(LayoutKind.Sequential)]
            internal struct SECURITY_ATTRIBUTES
            {
                internal uint nLength;
                internal IntPtr lpSecurityDescriptor;
                internal BOOL bInheritHandle;
            }

            internal class HandleTypes
            {
                internal const int STD_INPUT_HANDLE = -10;
                internal const int STD_OUTPUT_HANDLE = -11;
                internal const int STD_ERROR_HANDLE = -12;
            }

            internal class StartupInfoOptions
            {
                internal const int STARTF_USESTDHANDLES = 0x00000100;
                internal const int CREATE_UNICODE_ENVIRONMENT = 0x00000400;
                internal const int CREATE_NO_WINDOW = 0x08000000;
                internal const uint STATUS_INFO_LENGTH_MISMATCH = 0xC0000004;
            }

            internal enum BOOL : int
            {
                FALSE = 0,
                TRUE = 1,
            }

            internal class HandleOptions
            {
                internal const int DUPLICATE_SAME_ACCESS = 2;
                internal const int STILL_ACTIVE = 0x00000103;
                internal const int TOKEN_ADJUST_PRIVILEGES = 0x20;
            }

            internal class ProcessOptions
            {
                internal const int PROCESS_TERMINATE = 0x0001;
                internal const int PROCESS_VM_READ = 0x0010;
                internal const int PROCESS_SET_QUOTA = 0x0100;
                internal const int PROCESS_SET_INFORMATION = 0x0200;
                internal const int PROCESS_QUERY_INFORMATION = 0x0400;
                internal const int PROCESS_QUERY_LIMITED_INFORMATION = 0x1000;
                internal const int PROCESS_ALL_ACCESS = STANDARD_RIGHTS_REQUIRED | SYNCHRONIZE | 0xFFF;


                internal const int STANDARD_RIGHTS_REQUIRED = 0x000F0000;
                internal const int SYNCHRONIZE = 0x00100000;
            }

            internal sealed class ProcessWaitHandle : WaitHandle
            {
                internal ProcessWaitHandle(SafeProcessHandle processHandle)
                {
                    SafeWaitHandle waitHandle = null;
                    SafeProcessHandle currentProcHandle = Win32.GetCurrentProcess();
                    bool succeeded = Win32.DuplicateHandle(
                        currentProcHandle,
                        processHandle,
                        currentProcHandle,
                        out waitHandle,
                        0,
                        false,
                        Win32.HandleOptions.DUPLICATE_SAME_ACCESS);

                    if (!succeeded)
                    {
                        Marshal.ThrowExceptionForHR(Marshal.GetHRForLastWin32Error());
                    }

                    this.SetSafeWaitHandle(waitHandle);
                }
            }
        }
    }
}

using System;
using System.ComponentModel;
using System.Runtime.InteropServices;
using Sparrow.Logging;
using Sparrow.Platform;
using Sparrow.Platform.Posix;

namespace Sparrow.Utils
{
    public class Threading
    {
        private static readonly Logger _log = LoggingSource.Instance.GetLogger<Threading>("Raven/Server");
        private const int SCHED_OTHER = 0;
        private const int SCHED_RR = 2;

        public static void TrySettingCurrentThreadPriority(ThreadPriority priority)
        {
            try
            {
                SetCurrentThreadPriority(priority);
            }
            catch (Exception e)
            {
                if (_log.IsInfoEnabled)
                    _log.Info($"Could not change the thread priority to {priority}", e);
            }
        }

        public static unsafe void SetCurrentThreadPriority(ThreadPriority priority)
        {
            if (PlatformDetails.RunningOnPosix)
            {
                var nice = FixLinuxPriority(priority);

                var threadId = Syscall.gettid();
                var success = Syscall.setpriority((int)Prio.PROCESS, threadId, nice);
                if (success != 0)
                {
                    int lastError = Marshal.GetLastWin32Error();
                    if (_log.IsInfoEnabled)
                        _log.Info($"SetThreadPriority failed to set thread priority. threadId:{threadId}, error code {lastError} - {(Errno)lastError}");

                    throw new InvalidOperationException("Failed to set priority to thread " + threadId + " with " + (Errno)lastError);
                }
            }
            else
            {
                IntPtr handle = IntPtr.Zero;
                try
                {
                    uint threadId = Win32ThreadsMethods.GetCurrentThreadId();
                    handle = Win32ThreadsMethods.OpenThread(ThreadAccess.SetInformation, false, threadId);
                    if (handle == IntPtr.Zero)
                        throw new Win32Exception("Failed to setup thread priority, couldn't open the current thread");

                    var success = Win32ThreadsMethods.SetThreadPriority(handle, (int)priority);
                    if (success == 0)
                    {
                        int lastWin32ErrorCode = Marshal.GetLastWin32Error();

                       if (_log.IsInfoEnabled)
                        _log.Info($"SetThreadPriority failed to set thread priority. threadId:{threadId}, error code {lastWin32ErrorCode}");

                        throw new Win32Exception(lastWin32ErrorCode,"Failed to set priority to thread " + threadId);
                    }
                }
                finally
                {
                    Win32ThreadsMethods.CloseHandle(handle);
                }
            }
        }

        public static ThreadPriority GetCurrentThreadPriority()
        {
            if (PlatformDetails.RunningOnPosix)
            {
                var threadId = Syscall.gettid();
                var value = Syscall.getpriority((int)Prio.PROCESS, threadId);
                // we assume no failure here, since clearing errno isn't really possible here
                return FixLinuxPriority(value);
            }

            IntPtr handle = IntPtr.Zero;
            try
            {
                uint threadId = Win32ThreadsMethods.GetCurrentThreadId();
                handle = Win32ThreadsMethods.OpenThread(ThreadAccess.QueryInformation, false, threadId);
                if (handle == IntPtr.Zero)
                    throw new Win32Exception("Failed to setup thread priority, couldn't open the current thread");
                return Win32ThreadsMethods.GetThreadPriority(handle);
            }
            finally
            {
                if (handle != IntPtr.Zero)
                    Win32ThreadsMethods.CloseHandle(handle);
            }
        }

        private static int FixLinuxPriority(ThreadPriority priority)
        {
            switch (priority)
            {
                case ThreadPriority.Lowest:
                    return 19;
                case ThreadPriority.BelowNormal:
                    return 5;
                case ThreadPriority.AboveNormal:
                    return -5;
                case ThreadPriority.Highest:
                    return -15;
                case ThreadPriority.Normal:
                    return 0;
                default:
                    throw new ArgumentOutOfRangeException(nameof(priority), "Unknown value " + priority);
            }
        }

        private static ThreadPriority FixLinuxPriority(int priority)
        {
            if (priority > 5 && priority < 20)
                return ThreadPriority.Lowest;
            if (priority > 0 )
                return ThreadPriority.BelowNormal;
            if (priority == 0)
                return ThreadPriority.Normal;
            if (priority > -15)
                return ThreadPriority.AboveNormal;
            if (priority > -20)
                return ThreadPriority.Highest;

            throw new ArgumentOutOfRangeException(nameof(priority), "Uknown range for priority " + priority);
        }
    }
}

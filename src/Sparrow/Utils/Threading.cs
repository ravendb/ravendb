using System;
using System.ComponentModel;
using System.Runtime.InteropServices;
using Sparrow.Logging;

namespace Sparrow.Utils
{
    public class Threading
    {
        private static readonly Logger _log = LoggingSource.Instance.GetLogger<Threading>("Raven/Server");
        private const int SCHED_OTHER = 0;
        private const int SCHED_RR = 2;

        public static void TryLowerCurrentThreadPriority()
        {
            try
            {
                SetCurrentThreadPriority(ThreadPriority.BelowNormal);
            }
            catch (Exception e)
            {
                if (_log.IsInfoEnabled)
                    _log.Info("Could not reduce the thread priority", e);
            }
        }

        public static unsafe void SetCurrentThreadPriority(ThreadPriority priority)
        {
            if (Platform.RunningOnPosix)
            {
                ulong threadId = PosixThreadsMethods.pthread_self();
                sched_param param = new sched_param
                {
                    sched_priority = (int)priority
                };

                int policy = priority != ThreadPriority.Normal ? SCHED_RR : SCHED_OTHER;

                param.sched_priority = FixPosixPriority(priority);

                var success = PosixThreadsMethods.pthread_setschedparam(threadId, policy, &param);
                if (success != 0)
                {
                    int lastWin32ErrorCode = Marshal.GetLastWin32Error();
                    if (_log.IsInfoEnabled)
                        _log.Info($"SetThreadPriority failed to set thread priority. threadId:{threadId}, error code {lastWin32ErrorCode}");

                    throw new Win32Exception(lastWin32ErrorCode, "Failed to set priority to thread " + threadId);
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

        public static unsafe ThreadPriority GetCurrentThreadPriority()
        {
            ThreadPriority threadPriority;
            if (Platform.RunningOnPosix)
            {
                int policy = 0;
                sched_param param = new sched_param();
                ulong threadId = PosixThreadsMethods.pthread_self();
                var success = PosixThreadsMethods.pthread_getschedparam(threadId, &policy, &param);
                if (success != 0)
                {
                    int lastWin32ErrorCode = Marshal.GetLastWin32Error();
                    throw new Win32Exception("Failed to set priority to thread " + threadId,
                        new Win32Exception(lastWin32ErrorCode));

                }
                threadPriority = FixPosixPriority(param.sched_priority);
            }
            else
            {
                IntPtr handle = IntPtr.Zero;
                try
                {
                    uint threadId = Win32ThreadsMethods.GetCurrentThreadId();
                    handle = Win32ThreadsMethods.OpenThread(ThreadAccess.QueryInformation, false, threadId);
                    if (handle == IntPtr.Zero)
                        throw new Win32Exception("Failed to setup thread priority, couldn't open the current thread");
                    threadPriority = Win32ThreadsMethods.GetThreadPriority(handle);
                    if (threadId == int.MaxValue)
                        throw new Win32Exception("Failed to setup thread priority, couldn't get the thread priority");
                }
                finally
                {
                    if (handle != IntPtr.Zero)
                        Win32ThreadsMethods.CloseHandle(handle);
                }
            }
            return threadPriority;
        }

        private static int FixPosixPriority(ThreadPriority priority)
        {
            switch (priority)
            {
                case ThreadPriority.Lowest:
                    return 1;
                case ThreadPriority.BelowNormal:
                    return 25;
                case ThreadPriority.AboveNormal:
                    return 50;
                case ThreadPriority.Highest:
                    return 99;
                case ThreadPriority.Normal:
                    return 0;
                default:
                    throw new ArgumentOutOfRangeException(nameof(priority), "Unknown value " + priority);
            }
        }

        private static ThreadPriority FixPosixPriority(int priority)
        {
            if(priority == 0)
                return ThreadPriority.Normal;
            if (priority >= 1 && priority <= 24)
                return ThreadPriority.Lowest;
            if (priority >= 25 && priority <= 49)
                return ThreadPriority.BelowNormal;
            if (priority >= 50 && priority <= 74)
                return ThreadPriority.AboveNormal;
            if (priority >= 75 && priority <= 99)
                return ThreadPriority.Highest;

            throw new ArgumentOutOfRangeException(nameof(priority), "Uknown range for priority " + priority);
        }
    }
}

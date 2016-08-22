using System;
using System.ComponentModel;
using System.Diagnostics;
using System.Runtime.InteropServices;
using System.Threading;
using NLog;

namespace Sparrow
{
    public static class ThreadMethods
    {
        private static readonly Logger _log = LogManager.GetLogger(nameof(ThreadMethods));
        private const int SCHED_OTHER = 0;
        private const int SCHED_RR = 2;

        public unsafe static void SetThreadPriority(ThreadPriority priority)
        {
            if (Platform.RunningOnPosix)
            {
                ulong threadId = PosixThreadsMethods.pthread_self();
                sched_param param = new sched_param
                {
                    sched_priority = (int)priority
                };

                int policy = SCHED_OTHER;
                if (priority != ThreadPriority.Normal)
                {
                    policy = SCHED_RR;
                    param.sched_priority = FixPosixPriority(priority);
                }

                var success = PosixThreadsMethods.pthread_setschedparam(threadId, policy, &param);
                if (success != 0)
                {
                    int lastWin32ErrorCode = Marshal.GetLastWin32Error();
                    throw new Win32Exception("Failed to set priority to thread " + threadId,
                        new Win32Exception(lastWin32ErrorCode));
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

                        if (_log.IsDebugEnabled)
                            _log.Debug($"SetThreadPriority failed to set thread priority. threadId:{threadId}");

                        throw new Win32Exception("Failed to set priority to thread " + threadId,
                            new Win32Exception(lastWin32ErrorCode));
                    }
                }
                finally
                {
                    Win32ThreadsMethods.CloseHandle(handle);
                }
            }
        }

        public unsafe static ThreadPriority GetThreadPriority()
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

                    if (_log.IsDebugEnabled)
                        _log.Debug($"PosixThreadsMethods.pthread_getschedparam failed to retrieve thread priority. threadId:{threadId}");

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
            int fixPriority = 0;
            switch (priority)
            {
                case ThreadPriority.Lowest:
                    fixPriority = 1;
                    break;
                case ThreadPriority.BelowNormal:
                    fixPriority = 25;
                    break;
                case ThreadPriority.AboveNormal:
                    fixPriority = 50;
                    break;
                case ThreadPriority.Highest:
                    fixPriority = 99;
                    break;
            }
            return fixPriority;
        }

        private static ThreadPriority FixPosixPriority(int priority)
        {

            ThreadPriority fixPriority = ThreadPriority.Normal;

            if (priority >= 1 && priority <= 24)
                fixPriority = ThreadPriority.Lowest;
            else if (priority >= 25 && priority <= 49)
                fixPriority = ThreadPriority.BelowNormal;
            else if (priority >= 50 && priority <= 74)
                fixPriority = ThreadPriority.AboveNormal;
            else if (priority >= 75 && priority <= 99)
                fixPriority = ThreadPriority.Highest;

            return fixPriority;
        }
    }
}

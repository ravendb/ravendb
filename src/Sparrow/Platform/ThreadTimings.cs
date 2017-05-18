using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Runtime.InteropServices;
using Sparrow.Json.Parsing;

namespace Sparrow.Platform
{
    public class ThreadTimings
    {
        public static IEnumerable<ThreadInfo> GetRunawayThreads()
        {
            if (PlatformDetails.RunningOnPosix)
            {
                throw new NotImplementedException(); //TODO : do not forget to finish
            }
            else
            {
                var threadsSnapshotHandle = Win32ThreadsMethods.INVALID_HANDLE_VALUE;
                try
                {
                    var currentProcessId = Win32ThreadsMethods.GetCurrentProcessId();
                    threadsSnapshotHandle = Win32ThreadsMethods.CreateToolhelp32Snapshot(
                        SnapshotFlags.Thread, 0);
                    if (threadsSnapshotHandle == Win32ThreadsMethods.INVALID_HANDLE_VALUE)
                    {
                        var lastWin32ErrorCode = Marshal.GetLastWin32Error();
                        throw new InvalidOperationException("CreateToolhelp32Snapshot() returned invalid handle.", new Win32Exception(lastWin32ErrorCode));
                    }

                    var te = new THREADENTRY32
                    {
                        dwSize = (uint)Marshal.SizeOf<THREADENTRY32>()
                    };

                    if (!Win32ThreadsMethods.Thread32First(threadsSnapshotHandle, ref te))
                        yield break;

                    do
                    {
                        if(te.th32OwnerProcessID != currentProcessId)
                            continue;
                        var threadHandle = IntPtr.Zero;
                        try
                        {
                            if(te.th32ThreadID == 0)
                                continue;

                            threadHandle = Win32ThreadsMethods.OpenThread(ThreadAccess.QueryInformation, true, te.th32ThreadID);

                            Win32ThreadsMethods.GetThreadTimes(threadHandle,
                                out long lpCreationTime,
                                out long lpExitTime,
                                out long lpKernelTime,
                                out long lpUserTime);                                
                                
                            yield return new ThreadInfo
                            {
                                OsId = te.th32ThreadID,
                                UserTimeMilliseconds = lpUserTime / 10000,
                                KernelTimeMilliseconds = lpKernelTime / 10000,
                            };
                        }
                        finally
                        {
                            if (threadHandle != IntPtr.Zero)
                                Win32ThreadsMethods.CloseHandle(threadHandle);
                        }
                        te.dwSize = (uint)Marshal.SizeOf<THREADENTRY32>();
                    } while (Win32ThreadsMethods.Thread32Next(threadsSnapshotHandle, ref te));
                }
                finally
                {
                    if (threadsSnapshotHandle != Win32ThreadsMethods.INVALID_HANDLE_VALUE)
                    {
                        Win32ThreadsMethods.CloseHandle(threadsSnapshotHandle);
                    }
                }
            }
        }
    }

    public class ThreadInfo
    {
        public long OsId { get; set; }

        public long UserTimeMilliseconds { get; set; }

        public long KernelTimeMilliseconds { get; set; }

        public long TotalTimeMilliseconds => UserTimeMilliseconds + KernelTimeMilliseconds;

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(OsId)] = OsId,
                [nameof(UserTimeMilliseconds)] = UserTimeMilliseconds,
                [nameof(KernelTimeMilliseconds)] = KernelTimeMilliseconds,
                [nameof(TotalTimeMilliseconds)] = TotalTimeMilliseconds
            };
        }
    }
}

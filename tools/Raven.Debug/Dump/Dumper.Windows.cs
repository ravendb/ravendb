// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.

using System;
using System.ComponentModel;
using System.IO;
using System.Runtime.InteropServices;
using Microsoft.Win32.SafeHandles;

namespace Microsoft.Diagnostics.Tools.Dump
{
    public partial class Dumper
    {
        private static class Windows
        {
            internal static void CollectDump(int processId, string outputFile, DumpTypeOption type)
            {
                // The managed Process (via Process.GetProcessById) type can not be used to get the handle for a process that is in the middle of exiting such
                // that it has set an exit code, but not actually exited. This is because for compat reasons the Process class will throw in these circumstances.
                using SafeProcessHandle processHandle = NativeMethods.OpenProcess(NativeMethods.PROCESS_QUERY_INFORMATION | NativeMethods.PROCESS_VM_READ, false, processId);
                if (processHandle.IsInvalid)
                {
                    int error = Marshal.GetLastWin32Error();
                    string errorText = new Win32Exception(error).Message;
                    throw new ArgumentException($"Invalid process id {processId} - {errorText} ({error})");
                }

                // Open the file for writing
                using (FileStream stream = new(outputFile, FileMode.Create, FileAccess.ReadWrite, FileShare.None))
                {
                    NativeMethods.MINIDUMP_TYPE dumpType = NativeMethods.MINIDUMP_TYPE.MiniDumpNormal;
                    switch (type)
                    {
                        case DumpTypeOption.Full:
                            dumpType = NativeMethods.MINIDUMP_TYPE.MiniDumpWithFullMemory |
                                       NativeMethods.MINIDUMP_TYPE.MiniDumpWithDataSegs |
                                       NativeMethods.MINIDUMP_TYPE.MiniDumpWithHandleData |
                                       NativeMethods.MINIDUMP_TYPE.MiniDumpWithUnloadedModules |
                                       NativeMethods.MINIDUMP_TYPE.MiniDumpWithFullMemoryInfo |
                                       NativeMethods.MINIDUMP_TYPE.MiniDumpWithThreadInfo |
                                       NativeMethods.MINIDUMP_TYPE.MiniDumpWithTokenInformation;
                            break;
                        case DumpTypeOption.Heap:
                            dumpType = NativeMethods.MINIDUMP_TYPE.MiniDumpWithPrivateReadWriteMemory |
                                       NativeMethods.MINIDUMP_TYPE.MiniDumpWithDataSegs |
                                       NativeMethods.MINIDUMP_TYPE.MiniDumpWithHandleData |
                                       NativeMethods.MINIDUMP_TYPE.MiniDumpWithUnloadedModules |
                                       NativeMethods.MINIDUMP_TYPE.MiniDumpWithFullMemoryInfo |
                                       NativeMethods.MINIDUMP_TYPE.MiniDumpWithThreadInfo |
                                       NativeMethods.MINIDUMP_TYPE.MiniDumpWithTokenInformation;
                            break;
                        case DumpTypeOption.Mini:
                            dumpType = NativeMethods.MINIDUMP_TYPE.MiniDumpNormal |
                                       NativeMethods.MINIDUMP_TYPE.MiniDumpWithDataSegs |
                                       NativeMethods.MINIDUMP_TYPE.MiniDumpWithHandleData |
                                       NativeMethods.MINIDUMP_TYPE.MiniDumpWithThreadInfo;
                            break;
                        case DumpTypeOption.Triage:
                            dumpType = NativeMethods.MINIDUMP_TYPE.MiniDumpFilterTriage |
                                       NativeMethods.MINIDUMP_TYPE.MiniDumpIgnoreInaccessibleMemory |
                                       NativeMethods.MINIDUMP_TYPE.MiniDumpWithoutOptionalData |
                                       NativeMethods.MINIDUMP_TYPE.MiniDumpWithProcessThreadData |
                                       NativeMethods.MINIDUMP_TYPE.MiniDumpFilterModulePaths |
                                       NativeMethods.MINIDUMP_TYPE.MiniDumpWithUnloadedModules |
                                       NativeMethods.MINIDUMP_TYPE.MiniDumpFilterMemory |
                                       NativeMethods.MINIDUMP_TYPE.MiniDumpWithHandleData;
                            break;
                    }

                    int loopEnd = 10;
                    // Retry the write dump on ERROR_PARTIAL_COPY
                    for (int i = 0; i <= loopEnd; i++)
                    {
                        // Dump the process!
                        if (NativeMethods.MiniDumpWriteDump(processHandle.DangerousGetHandle(), (uint)processId, stream.SafeFileHandle, dumpType, IntPtr.Zero, IntPtr.Zero, IntPtr.Zero))
                        {
                            break;
                        }
                        else
                        {
                            int err = Marshal.GetHRForLastWin32Error();
                            if (err != NativeMethods.HR_ERROR_PARTIAL_COPY || i == loopEnd)
                            {
                                Marshal.ThrowExceptionForHR(err);
                            }
                            else
                            {
                                Console.WriteLine($"retrying due to PARTIAL_COPY #{i}");
                            }
                        }
                    }
                }
            }

            private static class NativeMethods
            {
                public const int HR_ERROR_PARTIAL_COPY = unchecked((int)0x8007012b);

                public const int PROCESS_VM_READ = 0x0010;
                public const int PROCESS_QUERY_INFORMATION = 0x0400;

                [DllImport("kernel32.dll", SetLastError = true)]
                public static extern SafeProcessHandle OpenProcess(int access, [MarshalAs(UnmanagedType.Bool)] bool inherit, int processId);

                [DllImport("Dbghelp.dll", SetLastError = true)]
                public static extern bool MiniDumpWriteDump(IntPtr hProcess, uint ProcessId, SafeFileHandle hFile, MINIDUMP_TYPE DumpType, IntPtr ExceptionParam, IntPtr UserStreamParam, IntPtr CallbackParam);

                [StructLayout(LayoutKind.Sequential, Pack = 4)]
                public struct MINIDUMP_EXCEPTION_INFORMATION
                {
                    public uint ThreadId;
                    public IntPtr ExceptionPointers;
                    public int ClientPointers;
                }

                [Flags]
                public enum MINIDUMP_TYPE : uint
                {
                    MiniDumpNormal = 0,
                    MiniDumpWithDataSegs = 1 << 0,
                    MiniDumpWithFullMemory = 1 << 1,
                    MiniDumpWithHandleData = 1 << 2,
                    MiniDumpFilterMemory = 1 << 3,
                    MiniDumpScanMemory = 1 << 4,
                    MiniDumpWithUnloadedModules = 1 << 5,
                    MiniDumpWithIndirectlyReferencedMemory = 1 << 6,
                    MiniDumpFilterModulePaths = 1 << 7,
                    MiniDumpWithProcessThreadData = 1 << 8,
                    MiniDumpWithPrivateReadWriteMemory = 1 << 9,
                    MiniDumpWithoutOptionalData = 1 << 10,
                    MiniDumpWithFullMemoryInfo = 1 << 11,
                    MiniDumpWithThreadInfo = 1 << 12,
                    MiniDumpWithCodeSegs = 1 << 13,
                    MiniDumpWithoutAuxiliaryState = 1 << 14,
                    MiniDumpWithFullAuxiliaryState = 1 << 15,
                    MiniDumpWithPrivateWriteCopyMemory = 1 << 16,
                    MiniDumpIgnoreInaccessibleMemory = 1 << 17,
                    MiniDumpWithTokenInformation = 1 << 18,
                    MiniDumpWithModuleHeaders = 1 << 19,
                    MiniDumpFilterTriage = 1 << 20,
                    MiniDumpWithAvxXStateContext = 1 << 21,
                    MiniDumpWithIptTrace = 1 << 22,
                    MiniDumpValidTypeFlags = (-1) ^ ((~1) << 22)
                }
            }
        }
    }
}

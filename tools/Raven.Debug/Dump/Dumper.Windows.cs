// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using Microsoft.Win32.SafeHandles;
using System;
using System.Diagnostics;
using System.IO;
using System.Runtime.InteropServices;
using System.Threading.Tasks;

namespace Microsoft.Diagnostics.Tools.Dump
{
    public partial class Dumper
    {
        private static class Windows
        {
            internal static Task CollectDumpAsync(Process process, string outputFile, DumpTypeOption type)
            {
                // We can't do this "asynchronously" so just Task.Run it. It shouldn't be "long-running" so this is fairly safe.
                return Task.Run(() =>
                {
                    // Open the file for writing
                    using (var stream = new FileStream(outputFile, FileMode.Create, FileAccess.ReadWrite, FileShare.None))
                    {
                        var exceptionInfo = new NativeMethods.MINIDUMP_EXCEPTION_INFORMATION();
                        var dumpType = type == DumpTypeOption.Mini ? NativeMethods.MINIDUMP_TYPE.MiniDumpWithThreadInfo :
                            NativeMethods.MINIDUMP_TYPE.MiniDumpWithDataSegs |
                            NativeMethods.MINIDUMP_TYPE.MiniDumpWithPrivateReadWriteMemory |
                            NativeMethods.MINIDUMP_TYPE.MiniDumpWithHandleData |
                            NativeMethods.MINIDUMP_TYPE.MiniDumpWithUnloadedModules |
                            NativeMethods.MINIDUMP_TYPE.MiniDumpWithFullMemoryInfo |
                            NativeMethods.MINIDUMP_TYPE.MiniDumpWithThreadInfo |
                            NativeMethods.MINIDUMP_TYPE.MiniDumpWithTokenInformation;

                        // Retry the write dump on ERROR_PARTIAL_COPY
                        for (int i = 0; i < 5; i++)
                        {
                            // Dump the process!
                            if (NativeMethods.MiniDumpWriteDump(process.Handle, (uint)process.Id, stream.SafeFileHandle, dumpType, ref exceptionInfo, IntPtr.Zero, IntPtr.Zero))
                            {
                                break;
                            }
                            else
                            {
                                int err = Marshal.GetHRForLastWin32Error();
                                if (err != NativeMethods.ERROR_PARTIAL_COPY)
                                {
                                    Marshal.ThrowExceptionForHR(err);
                                }
                            }
                        }
                    }
                });
            }

            private static class NativeMethods
            {
                public const int ERROR_PARTIAL_COPY = unchecked((int)0x8007012b);

                [DllImport("Dbghelp.dll", SetLastError = true)]
                public static extern bool MiniDumpWriteDump(IntPtr hProcess, uint ProcessId, SafeFileHandle hFile, MINIDUMP_TYPE DumpType, ref MINIDUMP_EXCEPTION_INFORMATION ExceptionParam, IntPtr UserStreamParam, IntPtr CallbackParam);

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

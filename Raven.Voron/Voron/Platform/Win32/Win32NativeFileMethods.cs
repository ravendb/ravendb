// -----------------------------------------------------------------------
//  <copyright file="NativeFileMethods.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.ComponentModel;
using System.IO;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading;
using Microsoft.Win32.SafeHandles;
using Voron.Exceptions;

namespace Voron.Platform.Win32
{
    public static unsafe class Win32NativeFileMethods
    {
        public const int ErrorIOPending = 997;
        public const int ErrorSuccess = 0;
        public const int ErrorHandleEof = 38;


        [StructLayout(LayoutKind.Explicit, Size = 8)]
        public struct FileSegmentElement
        {
            [FieldOffset(0)]
            public IntPtr Buffer;
            [FieldOffset(0)]
            public UInt64 Alignment;
        }


        [DllImport("kernel32.dll", SetLastError = true)]
        [return: MarshalAs(UnmanagedType.Bool)]
        public static extern bool WriteFileGather(
            SafeFileHandle hFile,
            FileSegmentElement* aSegmentArray,
            uint nNumberOfBytesToWrite,
            IntPtr lpReserved,
            NativeOverlapped* lpOverlapped);

        [DllImport("kernel32.dll", SetLastError = true)]
        public static extern bool GetOverlappedResult(SafeFileHandle hFile,
            NativeOverlapped* lpOverlapped,
            out uint lpNumberOfBytesTransferred, bool bWait);

        [DllImport("kernel32.dll", SetLastError = true)]
        public static extern bool SetFilePointerEx(SafeFileHandle hFile, long liDistanceToMove,
           IntPtr lpNewFilePointer, Win32NativeFileMoveMethod dwMoveMethod);

        public delegate void WriteFileCompletionDelegate(UInt32 dwErrorCode, UInt32 dwNumberOfBytesTransfered, NativeOverlapped* lpOverlapped);

        [DllImport("kernel32.dll", SetLastError = true)]
        public static extern bool WriteFileEx(SafeFileHandle hFile, byte* lpBuffer,
           uint nNumberOfBytesToWrite, NativeOverlapped* lpOverlapped,
           WriteFileCompletionDelegate lpCompletionRoutine);

        [DllImport("kernel32.dll", SetLastError = true)]
        public static extern bool WriteFile(SafeFileHandle hFile, byte* lpBuffer, int nNumberOfBytesToWrite,
                                            out int lpNumberOfBytesWritten, NativeOverlapped* lpOverlapped);



        [DllImport(@"kernel32.dll", SetLastError = true)]
        public static extern bool ReadFile(
            SafeFileHandle hFile,
            byte* pBuffer,
            int numBytesToRead,
            out int pNumberOfBytesRead,
            NativeOverlapped* lpOverlapped
            );

        [DllImport("kernel32.dll", SetLastError = true, CharSet = CharSet.Auto)]
        public static extern SafeFileHandle CreateFile(string lpFileName,
                                                       Win32NativeFileAccess dwDesiredAccess, Win32NativeFileShare dwShareMode,
                                                       IntPtr lpSecurityAttributes,
                                                       Win32NativeFileCreationDisposition dwCreationDisposition,
                                                       Win32NativeFileAttributes dwFlagsAndAttributes, IntPtr hTemplateFile);

        [DllImport("kernel32.dll", SetLastError = true)]
        public static extern bool CloseHandle(IntPtr hObject);

        [DllImport("kernel32.dll", SetLastError = true)]
        private static extern bool SetEndOfFile(SafeFileHandle hFile);

        [DllImport("kernel32.dll", SetLastError = true)]
        public static extern bool FlushFileBuffers(SafeFileHandle hFile);

        [DllImport("kernel32.dll", EntryPoint = "GetFinalPathNameByHandleW", CharSet = CharSet.Unicode, SetLastError = true)]
        public static extern int GetFinalPathNameByHandle(SafeFileHandle handle, [In, Out] StringBuilder path, int bufLen, int flags);
        
        public static void SetFileLength(SafeFileHandle fileHandle, long length)
        {
            if (SetFilePointerEx(fileHandle, length, IntPtr.Zero, Win32NativeFileMoveMethod.Begin) == false)
            {
                throw new Win32Exception(Marshal.GetLastWin32Error());
            }

            if (SetEndOfFile(fileHandle) == false)
            {
                var lastError = Marshal.GetLastWin32Error();

                if (lastError == (int) Win32NativeFileErrors.ERROR_DISK_FULL)
                {
                    var filePath = new StringBuilder(256);

                    while (GetFinalPathNameByHandle(fileHandle, filePath, filePath.Capacity, 0) > filePath.Capacity && 
                        filePath.Capacity < 32767) // max unicode path length
                    {
                        filePath.EnsureCapacity(filePath.Capacity*2);
                    }

                    filePath = filePath.Replace(@"\\?\", String.Empty); // remove extended-length path prefix

                    var fullFilePath = filePath.ToString();
                    var driveLetter = Path.GetPathRoot(fullFilePath);
                    var driveInfo = new DriveInfo(driveLetter);

                    throw new DiskFullException(driveInfo, fullFilePath, length);
                }

                var exception = new Win32Exception(lastError);

                if (lastError == (int) Win32NativeFileErrors.ERROR_NOT_READY || lastError == (int) Win32NativeFileErrors.ERROR_FILE_NOT_FOUND)
                    throw new VoronUnrecoverableErrorException("Could not set the file size because it is inaccessible", exception);

                throw exception;
            }
        }
    }

    public enum Win32NativeFileErrors
    {
        ERROR_FILE_NOT_FOUND = 0x2,
        ERROR_DISK_FULL = 0x70,
        ERROR_NOT_READY = 0x15
    }

    public enum Win32NativeFileMoveMethod : uint
    {
        Begin = 0,
        Current = 1,
        End = 2
    }

    [Flags]
    public enum Win32NativeFileAccess : uint
    {
        //
        // Standard Section
        //

        AccessSystemSecurity = 0x1000000,   // AccessSystemAcl access type
        MaximumAllowed = 0x2000000,     // MaximumAllowed access type

        Delete = 0x10000,
        ReadControl = 0x20000,
        WriteDAC = 0x40000,
        WriteOwner = 0x80000,
        Synchronize = 0x100000,

        StandardRightsRequired = 0xF0000,
        StandardRightsRead = ReadControl,
        StandardRightsWrite = ReadControl,
        StandardRightsExecute = ReadControl,
        StandardRightsAll = 0x1F0000,
        SpecificRightsAll = 0xFFFF,

        FILE_READ_DATA = 0x0001,        // file & pipe
        FILE_LIST_DIRECTORY = 0x0001,       // directory
        FILE_WRITE_DATA = 0x0002,       // file & pipe
        FILE_ADD_FILE = 0x0002,         // directory
        FILE_APPEND_DATA = 0x0004,      // file
        FILE_ADD_SUBDIRECTORY = 0x0004,     // directory
        FILE_CREATE_PIPE_INSTANCE = 0x0004, // named pipe
        FILE_READ_EA = 0x0008,          // file & directory
        FILE_WRITE_EA = 0x0010,         // file & directory
        FILE_EXECUTE = 0x0020,          // file
        FILE_TRAVERSE = 0x0020,         // directory
        FILE_DELETE_CHILD = 0x0040,     // directory
        FILE_READ_ATTRIBUTES = 0x0080,      // all
        FILE_WRITE_ATTRIBUTES = 0x0100,     // all

        //
        // Generic Section
        //

        GenericRead = 0x80000000,
        GenericWrite = 0x40000000,
        GenericExecute = 0x20000000,
        GenericAll = 0x10000000,

        SPECIFIC_RIGHTS_ALL = 0x00FFFF,
        FILE_ALL_ACCESS =
        StandardRightsRequired |
        Synchronize |
        0x1FF,

        FILE_GENERIC_READ =
        StandardRightsRead |
        FILE_READ_DATA |
        FILE_READ_ATTRIBUTES |
        FILE_READ_EA |
        Synchronize,

        FILE_GENERIC_WRITE =
        StandardRightsWrite |
        FILE_WRITE_DATA |
        FILE_WRITE_ATTRIBUTES |
        FILE_WRITE_EA |
        FILE_APPEND_DATA |
        Synchronize,

        FILE_GENERIC_EXECUTE =
        StandardRightsExecute |
          FILE_READ_ATTRIBUTES |
          FILE_EXECUTE |
          Synchronize
    }

    [Flags]
    public enum Win32NativeFileShare : uint
    {
        /// <summary>
        ///
        /// </summary>
        None = 0x00000000,
        /// <summary>
        /// Enables subsequent open operations on an object to request read access.
        /// Otherwise, other processes cannot open the object if they request read access.
        /// If this flag is not specified, but the object has been opened for read access, the function fails.
        /// </summary>
        Read = 0x00000001,
        /// <summary>
        /// Enables subsequent open operations on an object to request write access.
        /// Otherwise, other processes cannot open the object if they request write access.
        /// If this flag is not specified, but the object has been opened for write access, the function fails.
        /// </summary>
        Write = 0x00000002,
        /// <summary>
        /// Enables subsequent open operations on an object to request delete access.
        /// Otherwise, other processes cannot open the object if they request delete access.
        /// If this flag is not specified, but the object has been opened for delete access, the function fails.
        /// </summary>
        Delete = 0x00000004
    }

    public enum Win32NativeFileCreationDisposition : uint
    {
        /// <summary>
        /// Creates a new file. The function fails if a specified file exists.
        /// </summary>
        New = 1,
        /// <summary>
        /// Creates a new file, always.
        /// If a file exists, the function overwrites the file, clears the existing attributes, combines the specified file attributes,
        /// and flags with FILE_ATTRIBUTE_ARCHIVE, but does not set the security descriptor that the SECURITY_ATTRIBUTES structure specifies.
        /// </summary>
        CreateAlways = 2,
        /// <summary>
        /// Opens a file. The function fails if the file does not exist.
        /// </summary>
        OpenExisting = 3,
        /// <summary>
        /// Opens a file, always.
        /// If a file does not exist, the function creates a file as if dwCreationDisposition is CREATE_NEW.
        /// </summary>
        OpenAlways = 4,
        /// <summary>
        /// Opens a file and truncates it so that its size is 0 (zero) bytes. The function fails if the file does not exist.
        /// The calling process must open the file with the GENERIC_WRITE access right.
        /// </summary>
        TruncateExisting = 5
    }

    [Flags]
    public enum Win32NativeFileAttributes : uint
    {
        None = 0x00000000,
        Readonly = 0x00000001,
        Hidden = 0x00000002,
        System = 0x00000004,
        Directory = 0x00000010,
        Archive = 0x00000020,
        Device = 0x00000040,
        Normal = 0x00000080,
        Temporary = 0x00000100,
        SparseFile = 0x00000200,
        ReparsePoint = 0x00000400,
        Compressed = 0x00000800,
        Offline = 0x00001000,
        NotContentIndexed = 0x00002000,
        Encrypted = 0x00004000,
        Write_Through = 0x80000000,
        Overlapped = 0x40000000,
        NoBuffering = 0x20000000,
        RandomAccess = 0x10000000,
        SequentialScan = 0x08000000,
        DeleteOnClose = 0x04000000,
        BackupSemantics = 0x02000000,
        PosixSemantics = 0x01000000,
        OpenReparsePoint = 0x00200000,
        OpenNoRecall = 0x00100000,
        FirstPipeInstance = 0x00080000
    }

    public static unsafe class Win32MemoryMapNativeMethods
    {
        public static readonly IntPtr INVALID_HANDLE_VALUE = new IntPtr(-1);

        // ReSharper disable once InconsistentNaming - Win32
        public struct WIN32_MEMORY_RANGE_ENTRY
        {
            public void* VirtualAddress;
            public IntPtr NumberOfBytes;
        }

        [DllImport("kernel32.dll", SetLastError = true, CallingConvention =CallingConvention.Winapi)]
        [return: MarshalAs(UnmanagedType.Bool)]
        public extern static bool PrefetchVirtualMemory(IntPtr hProcess, UIntPtr NumberOfEntries,
            WIN32_MEMORY_RANGE_ENTRY* VirtualAddresses, ulong Flags);

        [Flags]
        public enum FileMapProtection : uint
        {
            PageReadonly = 0x02,
            PageReadWrite = 0x04,
            PageWriteCopy = 0x08,
            PageExecuteRead = 0x20,
            PageExecuteReadWrite = 0x40,
            SectionCommit = 0x8000000,
            SectionImage = 0x1000000,
            SectionNoCache = 0x10000000,
            SectionReserve = 0x4000000,
        }



        [DllImport("kernel32.dll", SetLastError = true, CharSet = CharSet.Auto)]
        public static extern IntPtr CreateFileMapping(
            IntPtr hFile,
            IntPtr lpFileMappingAttributes,
            FileMapProtection flProtect,
            uint dwMaximumSizeHigh,
            uint dwMaximumSizeLow,
            [MarshalAs(UnmanagedType.LPStr)] string lpName);

        // ReSharper disable UnusedMember.Local
        [Flags]
        public enum NativeFileMapAccessType : uint
        {
            Copy = 0x01,
            Write = 0x02,
            Read = 0x04,
            AllAccess = 0x08,
            Execute = 0x20,
        }
        // ReSharper restore UnusedMember.Local

        [DllImport("kernel32.dll", SetLastError = true)]
        [return: MarshalAs(UnmanagedType.Bool)]
        public static extern bool UnmapViewOfFile(byte* lpBaseAddress);

        [DllImport("kernel32.dll", SetLastError = true)]
        public static extern byte* MapViewOfFileEx(IntPtr hFileMappingObject,
                                                    NativeFileMapAccessType dwDesiredAccess,
                                                    uint dwFileOffsetHigh,
                                                    uint dwFileOffsetLow,
                                                    UIntPtr dwNumberOfBytesToMap,
                                                    byte* lpBaseAddress);


        [DllImport("kernel32.dll", SetLastError = true)]
        [return: MarshalAs(UnmanagedType.Bool)]
        public static extern bool FlushFileBuffers(SafeFileHandle hFile);


        [DllImport("kernel32.dll", SetLastError = true)]
        [return: MarshalAs(UnmanagedType.Bool)]
        public static extern bool FlushViewOfFile(byte* lpBaseAddress, IntPtr dwNumberOfBytesToFlush);
    }
    public static unsafe class Win32NativeMethods
    {
        [StructLayout(LayoutKind.Sequential)]
        public struct SYSTEM_INFO
        {
            public ushort processorArchitecture;
            // ReSharper disable once FieldCanBeMadeReadOnly.Local
            ushort reserved;
            public uint pageSize;
            public IntPtr minimumApplicationAddress;
            public IntPtr maximumApplicationAddress;
            public IntPtr activeProcessorMask;
            public uint numberOfProcessors;
            public uint processorType;
            public uint allocationGranularity;
            public ushort processorLevel;
            public ushort processorRevision;
        }

        [StructLayout(LayoutKind.Sequential)]
        public struct MEMORY_BASIC_INFORMATION
        {
            public UIntPtr BaseAddress;
            public UIntPtr AllocationBase;
            public uint AllocationProtect;
            public IntPtr RegionSize;
            public uint State;
            public uint Protect;
            public uint Type;
        }

        [DllImport("kernel32.dll", SetLastError = true)]
        public static extern IntPtr GetCurrentProcess();

        [DllImport("kernel32.dll", SetLastError = true)]
        public static extern uint SleepEx(uint dwMilliseconds, bool bAlertable);

        [DllImport("kernel32.dll", SetLastError = true)]
        public static extern uint WaitForSingleObjectEx(IntPtr hHandle, int dwMilliseconds,
           bool bAlertable);
    
        [DllImport("kernel32.dll")]
        public static extern void SetLastError(uint dwErrCode);

        [DllImport("kernel32.dll", SetLastError = true)]
        public static extern byte* VirtualAlloc(byte* lpAddress, UIntPtr dwSize,
            AllocationType flAllocationType, MemoryProtection flProtect);

        [DllImport("kernel32.dll", SetLastError = true)]
        [return: MarshalAs(UnmanagedType.Bool)]
        public static extern bool VirtualFree(byte* lpAddress, UIntPtr dwSize,
            FreeType dwFreeType);

        [DllImport("kernel32.dll")]
        public static extern int VirtualQuery(
            byte* lpAddress,
            MEMORY_BASIC_INFORMATION* lpBuffer,
            IntPtr dwLength
        );

        [Flags]
        public enum FreeType : uint
        {
            MEM_DECOMMIT = 0x4000,
            MEM_RELEASE = 0x8000
        }

        [Flags]
        public enum AllocationType : uint
        {
            COMMIT = 0x1000,
            RESERVE = 0x2000,
            RESET = 0x80000,
            LARGE_PAGES = 0x20000000,
            PHYSICAL = 0x400000,
            TOP_DOWN = 0x100000,
            WRITE_WATCH = 0x200000
        }

        [Flags]
        public enum MemoryProtection : uint
        {
            EXECUTE = 0x10,
            EXECUTE_READ = 0x20,
            EXECUTE_READWRITE = 0x40,
            EXECUTE_WRITECOPY = 0x80,
            NOACCESS = 0x01,
            READONLY = 0x02,
            READWRITE = 0x04,
            WRITECOPY = 0x08,
            GUARD_Modifierflag = 0x100,
            NOCACHE_Modifierflag = 0x200,
            WRITECOMBINE_Modifierflag = 0x400
        }

        // ReSharper restore InconsistentNaming

        [DllImport("kernel32.dll")]
        public static extern void GetSystemInfo(out SYSTEM_INFO lpSystemInfo);

        [DllImport("kernel32.dll", SetLastError = true)]
        [return: MarshalAs(UnmanagedType.Bool)]
        public static extern bool CloseHandle(IntPtr hObject);
    }
}

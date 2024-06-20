#nullable enable

using System;
using System.ComponentModel;
using System.Diagnostics;
using System.IO;
using System.IO.MemoryMappedFiles;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Sparrow;
using Sparrow.Logging;
using Sparrow.Platform;
using Sparrow.Server.Meters;
using Sparrow.Server.Platform;
using Sparrow.Server.Platform.Win32;
using Sparrow.Utils;
using Voron.Exceptions;
using Voron.Platform.Win32;
using Voron.Global;
using NativeMemory = Sparrow.Utils.NativeMemory;

namespace Voron.Impl.Paging;

public unsafe partial class Pager2
{
#if VALIDATE
    public const bool ProtectPages = true;
#else
    public const bool ProtectPages = false;
#endif

    public static class Win64
    {
        public const int AllocationGranularity = 64 * Constants.Size.Kilobyte;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static long NearestSizeToAllocationGranularity(long size)
        {
            return ((size / AllocationGranularity) + 1) * AllocationGranularity;
        }

        public static Functions CreateFunctions() => new()
        {
            Init = &Init,
            AcquirePagePointer = &AcquirePagePointer,
            AcquireRawPagePointer = &AcquirePagePointer,
            AcquirePagePointerForNewPage = &AcquirePagePointerForNewPage,
            AllocateMorePages = &AllocateMorePages,
            Sync = &Sync,
            ProtectPageRange = ProtectPages ? &ProtectPageRange : &ProtectPageNoop,
            UnprotectPageRange = ProtectPages ? &UnprotectPageRange : &ProtectPageNoop,
            EnsureMapped = &EnsureMapped,
            RecoverFromMemoryLockFailure = &RecoverFromMemoryLockFailure,
        };

        private static bool EnsureMapped(Pager2 pager, State state, ref PagerTransactionState txState, long pageNumber, int numberOfPages)
        {
            _ = pager;
            _ = state;
            _ = txState;
            _ = pageNumber;
            _ = numberOfPages;
            return false;
        }

        public static void ProtectPageNoop(byte* start, ulong size) { }

        public static void ProtectPageRange(byte* start, ulong size)
        {
            var rc = Win32MemoryProtectMethods.VirtualProtect(start, new UIntPtr(size), Win32MemoryProtectMethods.MemoryProtection.READONLY, out _);
            if (!rc)
            {
                throw new Win32Exception(Marshal.GetLastWin32Error());
            }
        }

        public static void UnprotectPageRange(byte* start, ulong size)
        {
            bool rc = Win32MemoryProtectMethods.VirtualProtect(start, new UIntPtr(size), Win32MemoryProtectMethods.MemoryProtection.READWRITE, out _);
            if (!rc)
            {
                throw new Win32Exception(Marshal.GetLastWin32Error());
            }
        }

        public static void Sync(Pager2 pager, State state)
        {
            if (pager._temporaryOrDeleteOnClose)
                return;

            if (Win32MemoryMapNativeMethods.FlushViewOfFile(state.BaseAddress, new IntPtr(state.TotalAllocatedSize)) == false ||
                Win32MemoryMapNativeMethods.FlushFileBuffers(state.Handle) == false)
            {
                throw new Win32Exception(Marshal.GetLastWin32Error());
            }
        }

        public static void AllocateMorePages(Pager2 pager, long newLength, ref State state)
        {
            var newLengthAfterAdjustment = NearestSizeToAllocationGranularity(newLength);

            if (newLengthAfterAdjustment <= state.TotalAllocatedSize)
                return;

            var allocationSize = newLengthAfterAdjustment - state.TotalAllocatedSize;

            Win32NativeFileMethods.SetFileLength(state.Handle, state.TotalAllocatedSize + allocationSize, pager.FileName);

            var newState = state.Clone();
            try
            {
                newState.TotalAllocatedSize = state.TotalAllocatedSize + allocationSize;
                newState.NumberOfAllocatedPages = newState.TotalAllocatedSize / Constants.Storage.PageSize;

                MapFile(newState);
                CreateFileMapping(pager, newState);
            }
            catch
            {
                newState.Dispose();
                throw;
            }
            
            pager.InstallState(newState);

            state.MoveFileOwnership();

            state = newState;
        }

        public static byte* AcquirePagePointerForNewPage(Pager2 pager, long pageNumber, int numberOfPages, State state, ref PagerTransactionState txState)
        {
            _ = numberOfPages;
            return AcquirePagePointer(pager, state, ref txState, pageNumber);
        }

        public static byte* AcquirePagePointer(Pager2 pager, State state, ref PagerTransactionState txState, long pageNumber)
        {
            _ = txState;
            if (pageNumber > state.NumberOfAllocatedPages || pageNumber < 0)
                goto InvalidPage;
            if (state.Disposed)
                goto AlreadyDisposed;

            if (pager._canPrefetch && pager._prefetchState.ShouldPrefetchSegment(pageNumber, out long offsetFromFileBase, out long bytes))
            {
                var command = new PalDefinitions.PrefetchRanges(state.BaseAddress + offsetFromFileBase, bytes);
                GlobalPrefetchingBehavior.GlobalPrefetcher.Value.CommandQueue.TryAdd(command, 0);
            }

            return state.BaseAddress + pageNumber * Constants.Storage.PageSize;

            InvalidPage:
            VoronUnrecoverableErrorException.Raise(pager.Options, $"The page {pageNumber} was not allocated in {pager.FileName}");

            AlreadyDisposed:
            throw new ObjectDisposedException("PagerState was already disposed");
        }

        public static State Init(Pager2 pager, OpenFileOptions openFileOptions)
        {
            var copyOnWriteMode = pager.Options.CopyOnWriteMode && openFileOptions.File.EndsWith(Constants.DatabaseFilename);

            var state = new State(pager, null)
            {
                FileAccess =
                    openFileOptions.ReadOnly || copyOnWriteMode
                        ? Win32NativeFileAccess.GenericRead
                        : Win32NativeFileAccess.GenericRead | Win32NativeFileAccess.GenericWrite,
                FileAttributes = (openFileOptions.Temporary
                                     ? Win32NativeFileAttributes.Temporary | Win32NativeFileAttributes.DeleteOnClose
                                     : Win32NativeFileAttributes.Normal) |
                                 (openFileOptions.SequentialScan ? Win32NativeFileAttributes.SequentialScan : Win32NativeFileAttributes.RandomAccess) |
                                 (copyOnWriteMode ? Win32NativeFileAttributes.Readonly : Win32NativeFileAttributes.None),
                MemAccess = copyOnWriteMode
                    ? Win32MemoryMapNativeMethods.NativeFileMapAccessType.Copy
                    : (openFileOptions.ReadOnly
                        ? Win32MemoryMapNativeMethods.NativeFileMapAccessType.Read
                        : Win32MemoryMapNativeMethods.NativeFileMapAccessType.Read | Win32MemoryMapNativeMethods.NativeFileMapAccessType.Write)
            };


            try
            {
                OpenFile(pager, openFileOptions, state);

                MapFile(state);
                CreateFileMapping(pager, state);

                if (openFileOptions.Temporary && pager.Options.DiscardVirtualMemory)
                {
                    // RavenDB-22465 - TBD
                    // var result = Pal.rvn_discard_virtual_memory(state.BaseAddress, state.TotalAllocatedSize, out int errorCode);
                    // if (result != PalFlags.FailCodes.Success)
                    //     PalHelper.ThrowLastError(result, errorCode, $"Attempted to discard file memory. Path: {openFileOptions.File} Size: {state.TotalAllocatedSize}.");
                }

                pager.InstallState(state);
            }
            catch
            {
                try
                {
                    state.Dispose();
                }
                catch
                {
                    // ignored
                }

                throw;
            }

            return state;
        }

        public static void OpenFile(Pager2 pager, OpenFileOptions openFileOptions, State state)
        {
            pager.UniquePhysicalDriveId = GetUniquePhysicalDriveId(pager._logger, openFileOptions.File);
            OpenFileHandle(openFileOptions, state);
            OpenFileStream(openFileOptions, state);
        }

        private static void OpenFileHandle(OpenFileOptions openFileOptions, State state)
        {
            state.Handle = Win32NativeFileMethods.CreateFile(openFileOptions.File,
                state.FileAccess,
                Win32NativeFileShare.Read | Win32NativeFileShare.Write | Win32NativeFileShare.Delete,
                IntPtr.Zero,
                Win32NativeFileCreationDisposition.OpenAlways,
                state.FileAttributes,
                IntPtr.Zero);
            if (state.Handle.IsInvalid)
                ThrowInvalidHandle();

            void ThrowInvalidHandle()
            {
                string message = $"Failed to open file storage of {nameof(Pager2)} for {openFileOptions.File}";

                int lastWin32ErrorCode = Marshal.GetLastWin32Error();

                if (lastWin32ErrorCode is (int)Win32NativeFileErrors.ERROR_SHARING_VIOLATION or (int)Win32NativeFileErrors.ERROR_LOCK_VIOLATION)
                {
                    try
                    {
                        message += $". {WhoIsLocking.ThisFile(openFileOptions.File)}";
                    }
                    catch
                    {
                        // ignored
                    }
                }

                throw new IOException(message, new Win32Exception(lastWin32ErrorCode));
            }
        }

        private static void OpenFileStream(OpenFileOptions openFileOptions, State state)
        {
            var streamAccessType = openFileOptions.ReadOnly
                ? FileAccess.Read
                : FileAccess.ReadWrite;
            state.FileStream = SafeFileStream.Create(state.Handle, streamAccessType);

            if (openFileOptions.ReadOnly == false)
            {
                var fileLength = state.FileStream.Length;
                state.TotalAllocatedSize = fileLength;
                if (fileLength == 0)
                {
                    var newFileLength = NearestSizeToAllocationGranularity(openFileOptions.InitializeFileSize ?? fileLength);
                    if (newFileLength != fileLength)
                    {
                        Win32NativeFileMethods.SetFileLength(state.Handle, newFileLength, openFileOptions.File);
                        state.TotalAllocatedSize = newFileLength;
                    }
                }
            }
            else
            {
                state.TotalAllocatedSize = state.FileStream.Length;
            }

            state.NumberOfAllocatedPages = state.TotalAllocatedSize / Constants.Storage.PageSize;
        }

        public static uint GetUniquePhysicalDriveId(Logger logger, string file)
        {
            var fileInfo = new FileInfo(file);
            var drive = fileInfo.Directory?.Root.Name.TrimEnd('\\');

            try
            {
                uint id = 0;
                if (drive is not null)
                    AbstractPager.PhysicalDrivePerMountCache.TryGetValue(drive, out id);

                if (logger.IsInfoEnabled)
                    logger.Info($"Physical drive '{drive}' unique id = '{id}' for file '{file}'");

                return id;
            }
            catch (Exception ex)
            {
                if (logger.IsInfoEnabled)
                    logger.Info($"Failed to determine physical drive Id for drive letter '{drive}', file='{file}'", ex);
                return 0;
            }
        }


        public static void MapFile(State state)
        {
            var memAccessType = state.MemAccess switch
            {
                Win32MemoryMapNativeMethods.NativeFileMapAccessType.Copy => MemoryMappedFileAccess.CopyOnWrite,
                _ when state.FileAccess == Win32NativeFileAccess.GenericRead => MemoryMappedFileAccess.Read,
                _ => MemoryMappedFileAccess.ReadWrite
            };

            state.MemoryMappedFile = MemoryMappedFile.CreateFromFile(state.FileStream!, null, state.TotalAllocatedSize,
                memAccessType, HandleInheritability.None, true);
        }

        private static void CreateFileMapping(Pager2 pager, State state)
        {
            var fileMappingHandle = state.MemoryMappedFile!.SafeMemoryMappedFileHandle.DangerousGetHandle();

            state.BaseAddress = Win32MemoryMapNativeMethods.MapViewOfFileEx(fileMappingHandle,
                state.MemAccess,
                0, 0,
                UIntPtr.Zero, //map all what was "reserved" in CreateFileMapping on previous row
                null);

            if (state.BaseAddress == (byte*)0) //system didn't succeed in mapping the address where we wanted
            {
                var innerException = new Win32Exception(Marshal.GetLastWin32Error(), "Failed to MapView of file " + state.Pager.FileName);
                var errorMessage =
                    $"Unable to allocate more pages - unsuccessfully tried to allocate continuous block of virtual memory with size = {new Size(state.TotalAllocatedSize, SizeUnit.Bytes)}";
                throw new OutOfMemoryException(errorMessage, innerException);
            }

            
            if (pager._lockMemory)
            {
                pager.Lock(state.BaseAddress, state.TotalAllocatedSize);
            }
            
            // We don't need to manage size updates, we'll register a new allocation, instead
            NativeMemory.RegisterFileMapping(state.Pager.FileName, (nint)(state.BaseAddress), state.TotalAllocatedSize, null);

            CreateFunctions().ProtectPageRange(state.BaseAddress, (ulong)state.TotalAllocatedSize);
        }

        public static bool RecoverFromMemoryLockFailure(Pager2 pager, byte* addressToLock, long sizeToLock)
        {

            using var currentProcess = Process.GetCurrentProcess();

            var retries = 10;
            while (retries > 0)
            {
                // From: https://msdn.microsoft.com/en-us/library/windows/desktop/ms686234(v=vs.85).aspx
                // "The maximum number of pages that a process can lock is equal to the number of pages in its minimum working set minus a small overhead"
                // let's increase the max size of memory we can lock by increasing the MinWorkingSet. On Windows, that is available for all users
                var nextWorkingSetSize = GetNearestFileSize(currentProcess.MinWorkingSet.ToInt64() + sizeToLock);

                if (nextWorkingSetSize > int.MaxValue && PlatformDetails.Is32Bits)
                {
                    nextWorkingSetSize = int.MaxValue;
                }

                // Minimum working set size must be less than or equal to the maximum working set size.
                // Let's increase the max as well.
                if (nextWorkingSetSize > currentProcess.MaxWorkingSet)
                {
                    try
                    {
#pragma warning disable CA1416 // Validate platform compatibility
                        currentProcess.MaxWorkingSet = new IntPtr(nextWorkingSetSize);
#pragma warning restore CA1416 // Validate platform compatibility
                    }
                    catch (Exception e)
                    {
                        throw new InsufficientMemoryException(
                            $"Need to increase the min working set size from {new Size(currentProcess.MinWorkingSet.ToInt64(), SizeUnit.Bytes)} to {new Size(nextWorkingSetSize, SizeUnit.Bytes)} but the max working set size was too small: {new Size(currentProcess.MaxWorkingSet.ToInt64(), SizeUnit.Bytes)}. " +
                            $"Failed to increase the max working set size so we can lock {new Size(sizeToLock, SizeUnit.Bytes)} for {pager.FileName}. With encrypted " +
                            "databases we lock some memory in order to avoid leaking secrets to disk. Treating this as a catastrophic error " +
                            "and aborting the current operation.", e);
                    }
                }

                try
                {
#pragma warning disable CA1416 // Validate platform compatibility
                    currentProcess.MinWorkingSet = new IntPtr(nextWorkingSetSize);
#pragma warning restore CA1416 // Validate platform compatibility
                }
                catch (Exception e)
                {
                    throw new InsufficientMemoryException(
                        $"Failed to increase the min working set size to {new Size(nextWorkingSetSize, SizeUnit.Bytes)} so we can lock {new Size(sizeToLock, SizeUnit.Bytes)} for {pager.FileName}. With encrypted " +
                        "databases we lock some memory in order to avoid leaking secrets to disk. Treating this as a catastrophic error " +
                        "and aborting the current operation.", e);
                }

                if (Sodium.Lock(addressToLock, (UIntPtr)sizeToLock) == 0)
                    return true;

                // let's retry, since we increased the WS, but other thread might have locked the memory
                retries--;
            }

            return false;
        }
    }
}

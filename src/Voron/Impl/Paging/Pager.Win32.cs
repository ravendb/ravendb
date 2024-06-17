using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.InteropServices;
using System.Threading;
using Sparrow;
using Sparrow.Collections;
using Voron.Exceptions;
using Voron.Global;
using Voron.Platform.Win32;
using NativeMemory = Sparrow.Utils.NativeMemory;

namespace Voron.Impl.Paging;

public unsafe partial class Pager2
{
    public static class Win32
    {
        public static Functions CreateFunctions() => new()
        {
            Init = &Win32.Init,
            AcquirePagePointer = &Win32.AcquirePagePointer,
            AcquireRawPagePointer = &Win32.AcquirePagePointer,
            AcquirePagePointerForNewPage = &Win32.AcquirePagePointerForNewPage,
            AllocateMorePages = &Win32.AllocateMorePages,
            Sync = &Win32.Sync,
            ProtectPageRange = ProtectPages ? &Win64.ProtectPageRange : &Win64.ProtectPageNoop,
            UnprotectPageRange = ProtectPages ? &Win64.UnprotectPageRange : &Win64.ProtectPageNoop,
            EnsureMapped = &Win32.EnsureMapped
        };
        
        private const int NumberOfPagesInAllocationGranularity = Win64.AllocationGranularity / Constants.Storage.PageSize;


        public static void DirectWrite(Pager2 pager, ref State state, ref PagerTransactionState txState,long posBy4Kbs, int numberOf4Kbs, byte* source)
        {
            Debug.Assert(txState.Sync == null || txState.Sync == SyncAfterDirectWrite);
            
            txState.Sync = SyncAfterDirectWrite;
            const int pageSizeTo4KbRatio = (Constants.Storage.PageSize / (4 * Constants.Size.Kilobyte));
            var pageNumber = posBy4Kbs / pageSizeTo4KbRatio;
            var offsetBy4Kb = posBy4Kbs % pageSizeTo4KbRatio;
            var numberOfPages = numberOf4Kbs / pageSizeTo4KbRatio;
            if (posBy4Kbs % pageSizeTo4KbRatio != 0 ||
                numberOf4Kbs % pageSizeTo4KbRatio != 0)
                numberOfPages++;

            pager.EnsureContinuous(ref state, pageNumber, numberOfPages);

            EnsureMapped(pager, state, ref txState, pageNumber, numberOfPages);
            var page = AcquirePagePointer(pager, state, ref txState, pageNumber);

            var toWrite = numberOf4Kbs * 4 * Constants.Size.Kilobyte;
            byte* destination = page + offsetBy4Kb * (4 * Constants.Size.Kilobyte);

            
            pager._functions.UnprotectPageRange(destination, (ulong)toWrite);

            Memory.Copy(destination, source, toWrite);

            pager._functions.ProtectPageRange(destination, (ulong)toWrite);
        }

        private static void SyncAfterDirectWrite(Pager2 pager, State state, ref PagerTransactionState txState)
        {
            var stateFor32Bits = GetTxState(pager, ref txState);
            foreach (var kvp in stateFor32Bits.LoadedPages)
            {
                var page = kvp.Value;
                if (!Win32MemoryMapNativeMethods.FlushViewOfFile((byte*)page.Pointer, (IntPtr)(page.NumberOfPages * Constants.Storage.PageSize)))
                {
                    var lastWinError = Marshal.GetLastSystemError();
                    throw new Win32Exception(lastWinError, "Failed to flush to file " + pager.FileName);
                }
            }

            var canCleanup = false;
            foreach (var addr in stateFor32Bits.AddressesToUnload)
            {
                canCleanup |= Interlocked.Decrement(ref addr.Usages) == 0;
            }
            if (canCleanup)
                CleanupMemory(pager, stateFor32Bits);
        }

        public static bool EnsureMapped(Pager2 pager, State state, ref PagerTransactionState txState, long pageNumber, int numberOfPages)
        {
            var pagerTxState = GetTxState(pager, ref txState);

            var distanceFromStart = (pageNumber % NumberOfPagesInAllocationGranularity);
            var allocationStartPosition = pageNumber - distanceFromStart;

            if (pagerTxState.LoadedPages.TryGetValue(allocationStartPosition, out var page))
            {
                if (distanceFromStart + numberOfPages < page.NumberOfPages)
                    return false; // already mapped large enough here
            }

            var amountToMapInBytes = Win64.NearestSizeToAllocationGranularity((distanceFromStart + numberOfPages) * Constants.Storage.PageSize);
            MapPages(pager, state, pagerTxState, allocationStartPosition, amountToMapInBytes);
            return true;
        }

        private static TxStateFor32Bits GetTxState(Pager2 pager, ref PagerTransactionState txState)
        {
            txState.For32Bits ??= new Dictionary<Pager2, TxStateFor32Bits>();
            if (txState.For32Bits.TryGetValue(pager, out var pagerTxState) == false)
            {
                txState.For32Bits[pager] = pagerTxState = new TxStateFor32Bits();
                txState.OnDispose += OnTxDispose;
            }

            return pagerTxState;
        }

        private static void OnTxDispose(Pager2 pager, State state, ref PagerTransactionState txState)
        {
            if (txState.For32Bits?.Remove(pager, out var pagerTxState) != true)
                return;

            var canCleanup = false;
            foreach (var addr in pagerTxState.AddressesToUnload)
            {
                canCleanup |= Interlocked.Decrement(ref addr.Usages) == 0;
            }

            if (canCleanup)
            {
                CleanupMemory(pager, pagerTxState);
            }
        }
        
        private static void CleanupMemory(Pager2 pager, TxStateFor32Bits txState)
        {
            var pager32BitsState = pager._32BitsState!;
            pager32BitsState.AllocationLock.EnterWriteLock();
            try
            {
                foreach (var addr in txState.AddressesToUnload)
                {
                    if (addr.Usages != 0)
                        continue;

                    if (!pager32BitsState.MemoryMapping.TryGetValue(addr.StartPage, out var set))
                        continue;

                    if (!set.TryRemove(addr))
                        continue;

                    Win32MemoryMapNativeMethods.UnmapViewOfFile((byte*)addr.Address);
                    NativeMemory.UnregisterFileMapping(addr.File, addr.Address, addr.Size);

                    if (set.IsEmpty)
                    {
                        pager32BitsState.MemoryMapping.TryRemove(addr.StartPage, out set);
                    }
                }
            }
            finally
            {
                pager32BitsState.AllocationLock.ExitWriteLock();
            }
        }


        public static State Init(Pager2 pager, OpenFileOptions openFileOptions)
        {
            var copyOnWriteMode = pager.Options.CopyOnWriteMode && openFileOptions.File.EndsWith(Constants.DatabaseFilename);
            if (copyOnWriteMode)
                ThrowNotSupportedOption(pager.FileName);

            var state = new State(pager, null)
            {
                FileAccess = openFileOptions.ReadOnly 
                        ? Win32NativeFileAccess.GenericRead
                        : Win32NativeFileAccess.GenericRead | Win32NativeFileAccess.GenericWrite,
                FileAttributes = (openFileOptions.Temporary
                                     ? Win32NativeFileAttributes.Temporary | Win32NativeFileAttributes.DeleteOnClose
                                     : Win32NativeFileAttributes.Normal) |
                                 (openFileOptions.SequentialScan ? Win32NativeFileAttributes.SequentialScan : Win32NativeFileAttributes.RandomAccess),
                MemAccess =openFileOptions.ReadOnly ? 
                    Win32MemoryMapNativeMethods.NativeFileMapAccessType.Read : 
                    Win32MemoryMapNativeMethods.NativeFileMapAccessType.Read | Win32MemoryMapNativeMethods.NativeFileMapAccessType.Write
            };

            try
            {
                Win64.OpenFile(pager, openFileOptions, state);
                Win64.MapFile(state);
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
        
        public static void AllocateMorePages(Pager2 pager, long newLength, ref State state)
        {
            var newLengthAfterAdjustment = Win64.NearestSizeToAllocationGranularity(newLength);

            if (newLengthAfterAdjustment <= state.TotalAllocatedSize)
                return;

            var allocationSize = newLengthAfterAdjustment - state.TotalAllocatedSize;
 
            Win32NativeFileMethods.SetFileLength(state.Handle, state.TotalAllocatedSize + allocationSize, pager.FileName);

            var newState = state.Clone();
            
            newState.TotalAllocatedSize = state.TotalAllocatedSize + allocationSize;
            newState.NumberOfAllocatedPages = newState.TotalAllocatedSize / Constants.Storage.PageSize;
            
            Win64.MapFile(newState);
            
            pager.InstallState(newState);
            
            state.MoveFileOwnership();
            
            state = newState;
        }
        
        [DoesNotReturn]
        private static void ThrowNotSupportedOption(string file)
        {
            throw new NotSupportedException(
                "CopyOnWriteMode is currently not supported for 32 bits, error on " +
                file);
        }
        public static void Sync(Pager2 pager, State state)
        {
            if (pager._temporaryOrDeleteOnClose)
                return;
            
            if (Win32MemoryMapNativeMethods.FlushFileBuffers(state.Handle) == false)
            {
                throw new Win32Exception(Marshal.GetLastWin32Error());
            }
        }
        public static byte* AcquirePagePointerForNewPage(Pager2 pager, long pageNumber, int numberOfPages, State state, ref PagerTransactionState txState)
        {
            _ = numberOfPages;
            return AcquirePagePointer(pager, state, ref txState, pageNumber);
        }

        public static byte* AcquirePagePointer(Pager2 pager, State state, ref PagerTransactionState txState, long pageNumber)
        {
            var pagerTxState = GetTxState(pager, ref txState);
            
            if (pageNumber > state.NumberOfAllocatedPages || pageNumber < 0)
                goto InvalidPage;
            if (state.Disposed)
                goto AlreadyDisposed;


            var distanceFromStart = (pageNumber % NumberOfPagesInAllocationGranularity);
            var allocationStartPosition = pageNumber - distanceFromStart;
            
            if (pagerTxState.LoadedPages.TryGetValue(allocationStartPosition, out var page))
                return page.Pointer + (distanceFromStart * Constants.Storage.PageSize);

            page = MapPages(pager, state, pagerTxState, allocationStartPosition,Win64.AllocationGranularity);
            return page.Pointer + (distanceFromStart * Constants.Storage.PageSize);

            
            InvalidPage:
            VoronUnrecoverableErrorException.Raise(pager.Options, $"The page {pageNumber} was not allocated in {pager.FileName}");
            
            AlreadyDisposed:
            throw new ObjectDisposedException("PagerState was already disposed");

        }
        
        
        private static LoadedPage MapPages(Pager2 pager, State state, TxStateFor32Bits pagerTxState, long startPage, long size)
        {
            pager._32BitsState!.AllocationLock.EnterReadLock();
            try
            {
                var addresses = pager._32BitsState.MemoryMapping.GetOrAdd(startPage,
                    _ => new ConcurrentSet<MappedAddresses>());

                foreach (var addr in addresses)
                {
                    if (addr.Size < size)
                        continue;

                    Interlocked.Increment(ref addr.Usages);
                    return AddMappingToTransaction(pagerTxState, startPage, size, addr);
                }


                var offset = new WindowsMemoryMapPager.SplitValue
                {
                    Value = (ulong)startPage * Constants.Storage.PageSize
                };

                if ((long)offset.Value + size > state.TotalAllocatedSize)
                {
                    // this can happen when the file size is not a natural multiple of the allocation granularity
                    // frex: granularity of 64KB, and the file size is 80KB. In this case, a request to map the last
                    // 64 kb will run into a problem, there aren't any. In this case only, we'll map the bytes that are
                    // actually there in the file, and if the code will attempt to access beyond the end of file, we'll get
                    // an access denied error, but this is already handled in higher level of the code, since we aren't just
                    // handing out access to the full range we are mapping immediately.
                    if ((long)offset.Value < state.TotalAllocatedSize)
                        size = state.TotalAllocatedSize - (long) offset.Value;
                    else
                        ThrowInvalidMappingRequested(startPage, size, state.TotalAllocatedSize);
                }

                IntPtr hFileMappingObject = state.MemoryMappedFile!.SafeMemoryMappedFileHandle.DangerousGetHandle();
                var result = Win32MemoryMapNativeMethods.MapViewOfFileEx(hFileMappingObject, state.MemAccess, offset.High,
                    offset.Low,
                    (UIntPtr)size, null);

                if (result == null)
                {
                    ThrowOnInvalidMapping(pager, state, startPage, size);
                }

                NativeMemory.RegisterFileMapping(pager.FileName, new IntPtr(result), size, null);
                var mappedAddresses = new MappedAddresses
                {
                    Address = (IntPtr)result,
                    File = pager.FileName,
                    Size = size,
                    StartPage = startPage,
                    Usages = 1
                };
                addresses.Add(mappedAddresses);
                return AddMappingToTransaction(pagerTxState, startPage, size, mappedAddresses);
            }
            finally
            {
                pager._32BitsState.AllocationLock.ExitReadLock();
            }
        }

        private static void ThrowOnInvalidMapping(Pager2 pager, State state, long startPage, long size)
        {
            var lastWin32Error = Marshal.GetLastWin32Error();

            const int ERROR_NOT_ENOUGH_MEMORY = 8;
            if (lastWin32Error == ERROR_NOT_ENOUGH_MEMORY)
            {
                throw new OutOfMemoryException($"Unable to map {size / Constants.Size.Kilobyte:#,#0} kb starting at {startPage} on {pager.FileName}", new Win32Exception(lastWin32Error));
            }

            const int INVALID_HANDLE = 6;

            if (lastWin32Error == INVALID_HANDLE && state.Disposed)
                throw new ObjectDisposedException("Pager " + pager.FileName + " was already disposed");

            throw new Win32Exception(
                $"Unable to map {size / Constants.Size.Kilobyte:#,#0} kb starting at {startPage} on {pager.FileName} (lastWin32Error={lastWin32Error})",
                new Win32Exception(lastWin32Error));
        }

        private static LoadedPage AddMappingToTransaction(TxStateFor32Bits state, long startPage, long size, MappedAddresses mappedAddresses)
        {
            state.TotalLoadedSize += size;
            state.AddressesToUnload.Add(mappedAddresses);
            var loadedPage = new LoadedPage
            {
                Pointer = (byte*)mappedAddresses.Address,
                NumberOfPages = (int)(size / Constants.Storage.PageSize),
                StartPage = startPage
            };
            state.LoadedPages[startPage] = loadedPage;
            return loadedPage;
        }
        
        [DoesNotReturn]
        private static void ThrowInvalidMappingRequested(long startPage, long size, long allocationSize)
        {
            throw new InvalidOperationException(
                $"Was asked to map page {startPage} + {size / 1024:#,#0} kb, but the file size is only {allocationSize}, can't do that.");
        }

    }     
}

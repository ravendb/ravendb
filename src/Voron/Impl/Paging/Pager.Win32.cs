using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.InteropServices;
using System.Threading;
using Sparrow;
using Sparrow.Collections;
using Sparrow.Platform;
using Sparrow.Server.Platform;
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
            AcquirePagePointer = &Win32.AcquirePagePointer,
            AcquireRawPagePointer = &Win32.AcquirePagePointer,
            AcquirePagePointerForNewPage = &Win32.AcquirePagePointerForNewPage,
            ProtectPageRange = ProtectPages ? &Win64.ProtectPageRange : &Win64.ProtectPageNoop,
            UnprotectPageRange = ProtectPages ? &Win64.UnprotectPageRange : &Win64.ProtectPageNoop,
            EnsureMapped = &Win32.EnsureMapped,
        };
        
        private const int NumberOfPagesInAllocationGranularity = Win64.AllocationGranularity / Constants.Storage.PageSize;

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
            if (txState.For32Bits is null)
            {
                txState.For32Bits = new Dictionary<Pager2, TxStateFor32Bits>();
                txState.OnDispose += OnTxDispose;
            }
            if (txState.For32Bits.TryGetValue(pager, out var pagerTxState) == false)
            {
                txState.For32Bits[pager] = pagerTxState = new TxStateFor32Bits();
            }
            return pagerTxState;
        }

        private static void OnTxDispose(StorageEnvironment env, ref State state, ref PagerTransactionState txState)
        {
            if (txState.For32Bits is null)
                return;

            foreach (var (pager, pagerTxState) in txState.For32Bits)
            {
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
                    if (set.IsEmpty)
                    {
                        pager32BitsState.MemoryMapping.TryRemove(addr.StartPage, out set);
                    }
                    
                    NativeMemory.UnregisterFileMapping(addr.File, addr.Address, addr.Size);
                    var rc = Pal.rvn_unmap_memory((void*)addr.Address, out var errorCode);
                    if (rc != PalFlags.FailCodes.Success)
                    {
                        PalHelper.ThrowLastError(rc, errorCode, $"Failed to unmap memory in 32 bits mode for {pager.FileName}");
                    }
                }
            }
            finally
            {
                pager32BitsState.AllocationLock.ExitWriteLock();
            }
        }

        private static byte* AcquirePagePointerForNewPage(Pager2 pager, long pageNumber, int numberOfPages, State state, ref PagerTransactionState txState)
        {
            EnsureMapped(pager, state, ref txState, pageNumber, numberOfPages);
            return AcquirePagePointer(pager, state, ref txState, pageNumber);
        }

        private static byte* AcquirePagePointer(Pager2 pager, State state, ref PagerTransactionState txState, long pageNumber)
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
        
        
        [StructLayout(LayoutKind.Explicit)]
        public struct SplitValue
        {
            [FieldOffset(0)]
            public ulong Value;

            [FieldOffset(0)]
            public uint Low;

            [FieldOffset(4)]
            public uint High;
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


                long offset = startPage * Constants.Storage.PageSize;
                
                if (offset + size > state.TotalAllocatedSize)
                {
                    // this can happen when the file size is not a natural multiple of the allocation granularity
                    // frex: granularity of 64KB, and the file size is 80KB. In this case, a request to map the last
                    // 64 kb will run into a problem, there aren't any. In this case only, we'll map the bytes that are
                    // actually there in the file, and if the code will attempt to access beyond the end of file, we'll get
                    // an access denied error, but this is already handled in higher level of the code, since we aren't just
                    // handing out access to the full range we are mapping immediately.
                    if (offset < state.TotalAllocatedSize)
                        size = state.TotalAllocatedSize - offset;
                    else
                        ThrowInvalidMappingRequested(startPage, size, state.TotalAllocatedSize);
                }

                var rc = Pal.rvn_map_memory(state.Handle, offset, size, out var result, out int errorCode);
                if (rc != PalFlags.FailCodes.Success)
                {
                    PalHelper.ThrowLastError(rc, errorCode, $"Failed to map in 32 bits mode from {pager.FileName}");
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

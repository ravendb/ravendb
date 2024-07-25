using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.InteropServices;
using System.Threading;
using Sparrow.Collections;
using Sparrow.Server.Platform;
using Voron.Exceptions;
using Voron.Global;
using NativeMemory = Sparrow.Utils.NativeMemory;

namespace Voron.Impl.Paging;

public unsafe partial class Pager
{
    public static class Bits32
    {
        public static Functions CreateFunctions() => new()
        {
            AcquirePagePointer = &Bits32.AcquirePagePointer,
            AcquireRawPagePointer = &Bits32.AcquirePagePointer,
            AcquirePagePointerForNewPage = &Bits32.AcquirePagePointerForNewPage,
            EnsureMapped = &Bits32.EnsureMapped,
        };
        
        private const int NumberOfPagesInAllocationGranularity = Bits64.AllocationGranularity / Constants.Storage.PageSize;

        public static bool EnsureMapped(Pager pager, State state, ref PagerTransactionState txState, long pageNumber, int numberOfPages)
        {
            var pagerTxState = GetTxState(pager, ref txState);

            var distanceFromStart = (pageNumber % NumberOfPagesInAllocationGranularity);
            var allocationStartPosition = pageNumber - distanceFromStart;

            if (pagerTxState.LoadedPages.TryGetValue(allocationStartPosition, out var page))
            {
                if (distanceFromStart + numberOfPages < page.NumberOfPages)
                    return false; // already mapped large enough here
            }

            var amountToMapInBytes = Bits64.NearestSizeToAllocationGranularity((distanceFromStart + numberOfPages) * Constants.Storage.PageSize);
            MapPages(pager, state, pagerTxState, allocationStartPosition, amountToMapInBytes);
            return true;
        }

        private static TxStateFor32Bits GetTxState(Pager pager, ref PagerTransactionState txState)
        {
            if (txState.For32Bits is null)
            {
                txState.For32Bits = new Dictionary<Pager, TxStateFor32Bits>();
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
        
        private static void CleanupMemory(Pager pager, TxStateFor32Bits txState)
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
                    var rc = Pal.rvn_unmap_memory((void*)addr.Address, addr.Size, out var errorCode);
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

        private static byte* AcquirePagePointerForNewPage(Pager pager, long pageNumber, int numberOfPages, State state, ref PagerTransactionState txState)
        {
            EnsureMapped(pager, state, ref txState, pageNumber, numberOfPages);
            return AcquirePagePointer(pager, state, ref txState, pageNumber);
        }

        private static byte* AcquirePagePointer(Pager pager, State state, ref PagerTransactionState txState, long pageNumber)
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

            page = MapPages(pager, state, pagerTxState, allocationStartPosition,Bits64.AllocationGranularity);
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
        
        private static LoadedPage MapPages(Pager pager, State state, TxStateFor32Bits pagerTxState, long startPage, long size)
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

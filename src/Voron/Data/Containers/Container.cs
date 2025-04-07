using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Numerics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Runtime.Intrinsics;
using System.Runtime.Intrinsics.X86;
using System.Text;
using Sparrow;
using Sparrow.Server;
using Sparrow.Server.Binary;
using Voron.Data.Lookups;
using Voron.Exceptions;
using Voron.Global;
using Voron.Impl;
using Voron.Impl.Paging;

namespace Voron.Data.Containers
{
    [StructLayout(LayoutKind.Explicit, Pack = 1)]
    public struct ContainerRootHeader
    {
        [FieldOffset(0)]
        public RootObjectType RootObjectType;

        [FieldOffset(1)]
        public long ContainerId;
    }
    
    public readonly unsafe ref struct Container
    {
        public const int InvalidId = -1;
        private const int MinimumAdditionalFreeSpaceToConsider = 64;
        private const int NumberOfReservedEntries = 4; // all pages, free pages, number of entries, next free page

        public class TransactionState
        {
            // page -> page-level-metadata
            public Dictionary<long, long> FreeListAdditions = new();
            public HashSet<long> FreeListRemovals = new();
            
            public HashSet<long> Removals = new();
            public HashSet<long> Additions = new();

            public Dictionary<long, long> LastFreePageByPageLevelMetadata = new();

            public long ContainerId;

            public TransactionState(long containerId)
            {
                ContainerId = containerId;
            }

            private Lookup<Int64LookupKey> _allPages, _freePages;

            public Lookup<Int64LookupKey> GetAllPages(LowLevelTransaction llt)
            {
                if (_allPages != null)
                    return _allPages;
                
                var rootContainer = new Container(llt.GetPage(ContainerId));
                ref var allPagesState = ref MemoryMarshal.AsRef<LookupState>(rootContainer.GetItem(ContainerPageHeader.AllPagesOffset));
                _allPages = Lookup<Int64LookupKey>.Open(llt, allPagesState);
                return _allPages;
            }
            
            public Lookup<Int64LookupKey> GetFreePages(LowLevelTransaction llt)
            {
                if (_freePages != null)
                    return _freePages;
                
                var rootContainer = new Container(llt.GetPage(ContainerId));
                ref var allPagesState = ref MemoryMarshal.AsRef<LookupState>(rootContainer.GetItem(ContainerPageHeader.FreeListOffset));
                _freePages = Lookup<Int64LookupKey>.Open(llt, allPagesState);
                return _freePages;
            }

            public void PrepareForCommit(Transaction tx)
            {
                var maxPages = Math.Max(Additions.Count, Removals.Count);
                var maxFree = Math.Max(FreeListAdditions.Count, FreeListRemovals.Count);

                using var __ = tx.Allocator.Allocate(Math.Max(maxPages, maxFree) * 2, out Span<long> buffer);

                var allPages = GetAllPages(tx.LowLevelTransaction);
                int index = AddAndSortBuffer(Additions, buffer);
                
                allPages.InitializeCursorState();
                for (int i = 0; i < index; i++)
                {
                    var key = new Int64LookupKey(buffer[i]);
                    allPages.TryGetNextValue(ref key, out _);
                    allPages.AddOrSetAfterGetNext(ref key, 0);
                }

                index = AddAndSortBuffer(Removals, buffer);

                allPages.InitializeCursorState();
                for (int i = 0; i < index; i++)
                {
                    var key = new Int64LookupKey(buffer[i]);
                    if (allPages.TryGetNextValue(ref key, out _))
                        allPages.TryRemoveExistingValue(ref key, out _);
                }

                var freePages = GetFreePages(tx.LowLevelTransaction);

                var len = buffer.Length / 2;
                var keys = buffer[..len];
                var vals = buffer[len..];
                index = AddAndSort(FreeListAdditions, ref keys, ref vals);

                freePages.InitializeCursorState();
                for (int i = 0; i < index; i++)
                {
                    var key = new Int64LookupKey(keys[i]);
                    freePages.TryGetNextValue(ref key, out _);
                    freePages.AddOrSetAfterGetNext(ref key, vals[i]);
                }
                
                index = AddAndSortBuffer(FreeListRemovals, buffer);
                
                freePages.InitializeCursorState();
                for (int i = 0; i < index; i++)
                {
                    var key = new Int64LookupKey(buffer[i]);
                    if(freePages.TryGetNextValue(ref key, out _))
                        freePages.TryRemoveExistingValue(ref key, out _);
                }
                
                var rootContainer = new Container(tx.LowLevelTransaction.ModifyPage(ContainerId));
                
                ref var allPagesState = ref MemoryMarshal.AsRef<LookupState>(rootContainer.GetItem(ContainerPageHeader.AllPagesOffset));
                allPagesState = allPages.State;
                
                ref var freePagesState = ref MemoryMarshal.AsRef<LookupState>(rootContainer.GetItem(ContainerPageHeader.FreeListOffset));
                freePagesState = freePages.State;
            }

            private static int AddAndSort(Dictionary<long, long> freeListAdditions, ref Span<long> keys, ref Span<long> vals)
            {
                int index = 0;
                foreach (var (k, v) in freeListAdditions)
                {
                    keys[index] = k;
                    vals[index] = v;
                    index++;
                }

                keys = keys[..index];
                vals = vals[..index];
                
                keys.Sort(vals);
                return index;
            }

            private int AddAndSortBuffer(HashSet<long> set, Span<long> buffer)
            {
                var index = 0;
                foreach (long addition in set)
                {
                    buffer[index++] = addition;
                }

                buffer[..index].Sort();
                return index;
            }
        }
        
        internal struct ItemMetadata
        {
            /// <summary>
            /// This is to store in a compact form (16 bits) offset and size
            /// of the value (if small). The format is:
            /// 5 bits  - size of the value
            /// 11 bits - offset into the page, assuming 4 bytes alignment
            ///
            /// The size can be:
            ///  0      - freed
            ///  1..29  - actual size of the value
            ///  30     - big item up to 256 bytes (offset points to the byte with the size)
            ///  31     - big item up to ~4 kb     (offset points to a ushort with the size)
            /// </summary>
            private ushort _compactBackingStore;

            public const ushort SizeMask = 0x1F;
            public const ushort OffsetMask = 0xFFFC;
            public const int OffsetShift = 3;
            public const ushort FreeElement = 0;
            public const ushort ByteSizeElement = 30;
            public const ushort UshortSizeElement = 31;
            
            public bool IsFree
            {
                [MethodImpl(MethodImplOptions.AggressiveInlining)]
                get => _compactBackingStore == 0;
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public int Get(ref byte* pagePointer)
            {
                // The offset it stored in bits 5..16, but it is actually 4 bytes
                // aligned, so we shift by 9 to get the raw offset
                var offset = _compactBackingStore >> OffsetShift & OffsetMask;
                int size = _compactBackingStore & SizeMask;
                switch (size)
                {
                    case FreeElement: // means it is freed 
                        return 0;
                    case ByteSizeElement: // size is one byte  at offset
                        size = *(pagePointer + offset);
                        offset += sizeof(byte);
                        break;
                    case UshortSizeElement: // size is two bytes at offset
                        size = *(ushort*)(pagePointer + offset);
                        offset += sizeof(ushort);
                        break;
                    default:
                        Debug.Assert(size is < 30 and > 0);
                        break;
                }
                pagePointer += offset;
                return size;
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining | MethodImplOptions.AggressiveOptimization)]
            public int GetSize(byte* pagePointer) => Get(ref pagePointer);

            [MethodImpl(MethodImplOptions.AggressiveInlining|MethodImplOptions.AggressiveOptimization)]
            public void SetSize(int size, byte* pagePointer, ref int entryOffset)
            {
                Debug.Assert((entryOffset & 0b11) == 0, "entryOffset must always be 4 bytes aligned");
                Debug.Assert(size < ushort.MaxValue);
                int modifiedOffset = (entryOffset << OffsetShift); // lowest two bits already cleared
                switch (size)
                {
                    case < 30:
                        _compactBackingStore = (ushort)(modifiedOffset | size);
                        return;
                    // one byte size
                    case <= byte.MaxValue:
                        *(pagePointer + entryOffset) = (byte)size;
                        entryOffset++;
                        _compactBackingStore = (ushort)(modifiedOffset | 30);
                        return;
                    // two bytes size
                    default:
                        _compactBackingStore = (ushort)(modifiedOffset | 31);
                        *(ushort*)(pagePointer + entryOffset) = (ushort)size;
                        entryOffset += sizeof(ushort);
                        break;
                }
            }

            
            [MethodImpl(MethodImplOptions.AggressiveInlining|MethodImplOptions.AggressiveOptimization)]
            public void Clear(byte* pagePointer)
            {
#if DEBUG       // Skipping this outside of debug, since the cost 2%+ of indexing see: RavenDB-21027
                var size = Get(ref pagePointer);
                new Span<byte>(pagePointer, size).Clear();
#endif
                _compactBackingStore = 0;
            }
        }
        
        static Container()
        {
            Debug.Assert(sizeof(ItemMetadata) == sizeof(ushort));
        }        

        private readonly Page _page;

        public ref ContainerPageHeader Header => ref MemoryMarshal.AsRef<ContainerPageHeader>(_page.AsSpan());

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal ref ItemMetadata MetadataFor(int pos = 0)
        {
            return ref Unsafe.AsRef<ItemMetadata>(_page.DataPointer + sizeof(ItemMetadata) * pos);
        }

        public string Dump()
        {
            var sb = new StringBuilder();
            ushort numberOfOffsets = Header.NumberOfOffsets;
            sb.Append("NumberOfOffsets: ").Append(numberOfOffsets)
                .Append(" Free: ").Append(Header.FloorOfData - Header.CeilingOfOffsets)
                .AppendLine();
            
            for (var index = 0; index < numberOfOffsets; index++)
            {
                ItemMetadata itemMetadata = MetadataFor(index);
                sb.Append(itemMetadata.IsFree  ? " - " : " + ").Append(index).Append(" - ");
                if (itemMetadata.IsFree == false)
                {
                    var p = _page.Pointer;
                    var size = itemMetadata.Get(ref p);

                    long offset = p - _page.Pointer ;
                    sb.Append(size).Append(" @ ").Append(offset);
                }
                else
                {
                    sb.Append("Free");
                }

                sb.AppendLine();
            }

            return sb.ToString();
        }

        /// <summary>
        /// Checks if the current container has any item.
        /// For reference check the scalar version: Voron.Data.Containers.Container.ItemMetadata.IsFree
        /// </summary>
        /// <returns>True when any item contains a value, otherwise false.</returns>
        internal bool HasEntries()
        {
            Debug.Assert(sizeof(ItemMetadata) == sizeof(ushort));
            ushort numberOfOffsets = Header.NumberOfOffsets;
            var index = 0;

            if (AdvInstructionSet.IsAcceleratedVector512)
            {
                var N = Vector512<ushort>.Count;
                for (; index + N <= numberOfOffsets; index += N)
                {
                    var metadataItems = Vector512.Load((ushort*)_page.DataPointer + index);
                    if (metadataItems.Equals(Vector512<ushort>.Zero) == false)
                        return true;
                }
            }
            
            if (AdvInstructionSet.IsAcceleratedVector256)
            {
                var N = Vector256<ushort>.Count;
                for (; index + N <= numberOfOffsets; index += N)
                {
                    var metadataItems = Vector256.Load((ushort*)_page.DataPointer + index);
                    if (metadataItems.Equals(Vector256<ushort>.Zero) == false)
                        return true;
                }
            }

            if (AdvInstructionSet.IsAcceleratedVector128)
            {
                var N = Vector128<ushort>.Count;
                for (; index + N <= numberOfOffsets; index += N)
                {
                    var metadataItems = Vector128.Load((ushort*)_page.DataPointer + index);
                    if (metadataItems.Equals(Vector128<ushort>.Zero) == false)
                        return true;
                }
            }

            ref var currentMetadata = ref MetadataFor();
            for (; index < numberOfOffsets; index++)
                if (Unsafe.Add(ref currentMetadata, index).IsFree == false)
                    return true;
            
            return false;
        }

        /// <summary>
        /// Calculates space used in all container items.
        /// Equals operation forEachMetadata.Sum(i => container.MetadataFor(i).Get(ptr)).
        /// </summary>
        /// <param name="pagePtr">Storage of size</param>
        /// <param name="usedItems">Number of items that are currently in use.</param>
        /// <returns></returns>
        internal int SpaceUsedInItems(byte* pagePtr, out int usedItems)
        {
            int numberOfOffsets = Header.NumberOfOffsets;
            var size = 0;
            usedItems = numberOfOffsets;
            var index = 0;

            Debug.Assert(Vector512.IsHardwareAccelerated == false || Vector512<ushort>.Count == 32);
            //Vector512<ushort>.Count; explicit declaration when hardware does not support Vector
            ushort* offsetArray = stackalloc ushort[32]; 
            
            if (AdvInstructionSet.IsAcceleratedVector512)
            {
                var N = Vector512<ushort>.Count;
                for (; index + N <= numberOfOffsets; index += N)
                {
                    var metadataItems = Vector512.Load((ushort*)_page.DataPointer + index);
                    var offsets = Vector512.BitwiseAnd(
                        left: (metadataItems >> ItemMetadata.OffsetShift), 
                        right: Vector512.Create(ItemMetadata.OffsetMask));
                    offsets.Store(offsetArray);
                    
                    var sizes = Vector512.BitwiseAnd(
                        left: metadataItems, 
                        right: Vector512.Create(ItemMetadata.SizeMask));
                    
                    var byteElements = Vector512.Equals(
                        left: sizes, 
                        right: Vector512.Create(ItemMetadata.ByteSizeElement));
                    
                    var ushortElements = Vector512.Equals(
                        left: sizes, 
                        right: Vector512.Create(ItemMetadata.UshortSizeElement));
                    
                    var byteSizedPopulation = byteElements.ExtractMostSignificantBits();
                    var ushortSizedPopulation = ushortElements.ExtractMostSignificantBits();
                    var usedPopulation = byteSizedPopulation | ushortSizedPopulation;
                    var zeroPopulation = Vector512.Equals(sizes, Vector512<ushort>.Zero);
                    usedItems -= BitOperations.PopCount(zeroPopulation.ExtractMostSignificantBits());
                    
                    
                    //sizes are expected to be between 0 and 31, so it is safe to sum
                    size += Vector512.Sum(sizes);
                    size -= ItemMetadata.ByteSizeElement * BitOperations.PopCount(byteSizedPopulation);
                    size -= ItemMetadata.UshortSizeElement * BitOperations.PopCount(ushortSizedPopulation);
                    
#if DEBUG
                    var lessThanOrEqualUshortElement = Vector512.LessThanOrEqualAll(
                        left: sizes, 
                        right: Vector512.Create(ItemMetadata.UshortSizeElement));
                    Debug.Assert(lessThanOrEqualUshortElement);

                    var greaterThanOrEqualZero = Vector512.GreaterThanOrEqualAll(
                        left: sizes, 
                        right: Vector512<ushort>.Zero);
                    Debug.Assert(greaterThanOrEqualZero);
#endif
                    
                    while (usedPopulation != 0)
                    {
                        int currentOffset = BitOperations.TrailingZeroCount(usedPopulation);
                        
                        //Sets are disjoint, so if the bit is set in byteMask, we read the value as byte,
                        //otherwise it is ushort
                        var sizePtr = (pagePtr + offsetArray[currentOffset]);
                        size += (byteSizedPopulation & (1UL << currentOffset)) != 0 
                            ? *sizePtr
                            : *(ushort*)sizePtr;
                        
                        usedPopulation &= (usedPopulation - 1);
                    }
                }
            }
            
            if (AdvInstructionSet.IsAcceleratedVector256)
            {
                var N = Vector256<ushort>.Count;
                for (; index + N <= numberOfOffsets; index += N)
                {
                    var metadataItems = Vector256.Load((ushort*)_page.DataPointer + index);
                    var offsets = Vector256.BitwiseAnd(
                        left: (metadataItems >> ItemMetadata.OffsetShift), 
                        right: Vector256.Create(ItemMetadata.OffsetMask));
                    offsets.Store(offsetArray);
                    
                    var sizes = Vector256.BitwiseAnd(
                        left: metadataItems, 
                        right: Vector256.Create(ItemMetadata.SizeMask));
                    
                    var byteElements = Vector256.Equals(
                        left: sizes, 
                        right: Vector256.Create(ItemMetadata.ByteSizeElement));
                    
                    var ushortElements = Vector256.Equals(
                        left: sizes, 
                        right: Vector256.Create(ItemMetadata.UshortSizeElement));
                    
                    
                    var byteSizedPopulation = byteElements.ExtractMostSignificantBits();
                    var ushortSizedPopulation = ushortElements.ExtractMostSignificantBits();
                    var zeroPopulation = Vector256.Equals(sizes, Vector256<ushort>.Zero);

                    var usedPopulation = byteSizedPopulation | ushortSizedPopulation;
                    usedItems -= BitOperations.PopCount(zeroPopulation.ExtractMostSignificantBits());
                    
                    
                    size += Vector256.Sum(sizes);
                    size -= ItemMetadata.ByteSizeElement * BitOperations.PopCount(byteSizedPopulation);
                    size -= ItemMetadata.UshortSizeElement * BitOperations.PopCount(ushortSizedPopulation);
                    
#if DEBUG
                    var lessThanOrEqualUshortElement = Vector256.LessThanOrEqualAll(
                        left: sizes, 
                        right: Vector256.Create(ItemMetadata.UshortSizeElement));
                    Debug.Assert(lessThanOrEqualUshortElement);

                    var greaterThanOrEqualZero = Vector256.GreaterThanOrEqualAll(
                        left: sizes, 
                        right: Vector256<ushort>.Zero);
                    Debug.Assert(greaterThanOrEqualZero);
#endif
                    
                    
                    while (usedPopulation != 0)
                    {
                        int currentOffset = BitOperations.TrailingZeroCount(usedPopulation);
                        var sizePtr = (pagePtr + offsetArray[currentOffset]);
                        size += (byteSizedPopulation & ((uint)1 << currentOffset)) != 0 
                            ? *sizePtr
                            : *(ushort*)sizePtr;
                        
                        usedPopulation &= (usedPopulation - 1);
                    }
                }
            }
            
            if (AdvInstructionSet.IsAcceleratedVector128)
            {
                var N = Vector128<ushort>.Count;
                for (; index + N <= numberOfOffsets; index += N)
                {
                    var metadataItems = Vector128.Load((ushort*)_page.DataPointer + index);
                    var offsets = Vector128.BitwiseAnd(
                        left: (metadataItems >> ItemMetadata.OffsetShift),
                        right: Vector128.Create(ItemMetadata.OffsetMask));
                    offsets.Store(offsetArray);

                    var sizes = Vector128.BitwiseAnd(
                        left: metadataItems,
                        right: Vector128.Create(ItemMetadata.SizeMask));

                    var byteElements = Vector128.Equals(
                        left: sizes,
                        right: Vector128.Create(ItemMetadata.ByteSizeElement));

                    var ushortElements = Vector128.Equals(
                        left: sizes,
                        right: Vector128.Create(ItemMetadata.UshortSizeElement));

                    var byteSizedPopulation = byteElements.ExtractMostSignificantBits();
                    var ushortSizedPopulation = ushortElements.ExtractMostSignificantBits();
                    var usedPopulation = byteSizedPopulation | ushortSizedPopulation;

                    var zeroPopulation = Vector128.Equals(sizes, Vector128<ushort>.Zero);
                    usedItems -= BitOperations.PopCount(zeroPopulation.ExtractMostSignificantBits());
                    size += Vector128.Sum(sizes);
                    size -= ItemMetadata.ByteSizeElement * BitOperations.PopCount(byteSizedPopulation);
                    size -= ItemMetadata.UshortSizeElement * BitOperations.PopCount(ushortSizedPopulation);

#if DEBUG
                    var lessThanOrEqualUshortElement = Vector128.LessThanOrEqualAll(
                        left: sizes, 
                        right: Vector128.Create(ItemMetadata.UshortSizeElement));
                    Debug.Assert(lessThanOrEqualUshortElement);

                    var greaterThanOrEqualZero = Vector128.GreaterThanOrEqualAll(
                        left: sizes, 
                        right: Vector128<ushort>.Zero);
                    Debug.Assert(greaterThanOrEqualZero);
#endif

                    while (usedPopulation != 0)
                    {
                        int currentOffset = BitOperations.TrailingZeroCount(usedPopulation);

                        var sizePtr = (pagePtr + offsetArray[currentOffset]);
                        size += (byteSizedPopulation & ((uint)1 << currentOffset)) != 0
                            ? *sizePtr
                            : *(ushort*)sizePtr;
                        usedPopulation &= (usedPopulation - 1);
                    }
                }
            }
            
            ref var metadata = ref MetadataFor();
            for (; index < numberOfOffsets; index++)
            {
                var metadataSize = Unsafe.Add(ref metadata, index).GetSize(pagePtr);
                usedItems -= (metadataSize == ItemMetadata.FreeElement).ToInt32();
                size += metadataSize;
            }
            return size;
        }

        /// <summary>
        /// Calculates total space used by the current container.
        /// </summary>
        /// <returns>Used space in bytes.</returns>
        public int SpaceUsed()
        {
            int numberOfOffsets = Header.NumberOfOffsets;
            var size = numberOfOffsets * sizeof(ItemMetadata) + PageHeader.SizeOf;
            return size + SpaceUsedInItems(_page.Pointer, out _);
        }
        
        public Container(Page page)
        {
            Debug.Assert(page.IsOverflow == false);
            Debug.Assert(((ContainerPageHeader*)page.Pointer)->ContainerFlags == ExtendedPageType.Container);

            _page = page;            
        }

        public static long Create(LowLevelTransaction llt)
        {
            var page = AllocateContainerPage(llt);

            var root = new Container(page);
            root.Header.NumberOfOverflowPages = 0;
            root.Header.PageLevelMetadata = -1;
            
            root.Allocate(sizeof(LookupState), ContainerPageHeader.FreeListOffset, out var freeListStateBuf);
            root.Allocate(sizeof(LookupState), ContainerPageHeader.AllPagesOffset, out var allPagesStateBuf);
            root.Allocate(sizeof(long), ContainerPageHeader.NumberOfEntriesOffset, out var numberOfEntriesBuffer);
            root.Allocate(sizeof(long), ContainerPageHeader.NextFreePageOffset, out var nextFreePageBuffer);
            Unsafe.WriteUnaligned<long>(ref numberOfEntriesBuffer[0], 4L);
            Unsafe.WriteUnaligned(ref nextFreePageBuffer[0], page.PageNumber);

            // We are creating a set where we will store the free list.
            ref var freeListState = ref MemoryMarshal.AsRef<LookupState>(freeListStateBuf);
            Lookup<Int64LookupKey>.Create(llt, out freeListState);
            ref var allPagesListState = ref MemoryMarshal.AsRef<LookupState>(allPagesStateBuf);
            Lookup<Int64LookupKey>.Create(llt, out allPagesListState);
            
            var list = Lookup<Int64LookupKey>.Open(llt, allPagesListState);
            list.Add(page.PageNumber, 0);
            allPagesListState = list.State;
            return page.PageNumber;
        }

        private static Page AllocateContainerPage( LowLevelTransaction llt )
        {
            var page = llt.AllocatePage(1);

            ref var header = ref MemoryMarshal.AsRef<ContainerPageHeader>(page.AsSpan());
            header.Flags = PageFlags.Single | PageFlags.Other;
            header.ContainerFlags = ExtendedPageType.Container;
            header.FloorOfData = Constants.Storage.PageSize;

            return page;
        }

        // this is computed so this will fit exactly two items of max size in a container page. Beyond that, we'll have enough
        // fragmentation that we might as well use a dedicated page.
        public const int MaxSizeInsideContainerPage = (Constants.Storage.PageSize - PageHeader.SizeOf) / 2 - sizeof(ushort) * 4;

        private void Defrag(LowLevelTransaction llt)
        {
            using var _ = llt.Allocator.Allocate(Constants.Storage.PageSize, out ByteString tmpBuffer);
            tmpBuffer.Clear();
            byte* tmpPtr = tmpBuffer.Ptr;
            Memory.Copy(tmpPtr, _page.Pointer, Header.CeilingOfOffsets);
            ref var tmpHeader = ref Unsafe.AsRef<ContainerPageHeader>(tmpPtr);
            tmpHeader.FloorOfData = Constants.Storage.PageSize;
            var numberOfOffsets = Header.NumberOfOffsets;
            var tmpOffsetsPtr = tmpPtr + PageHeader.SizeOf; 
            for (var i = 0; i < numberOfOffsets; i++, tmpOffsetsPtr += sizeof(ItemMetadata))
            {
                ref var tmpOffset = ref Unsafe.AsRef<ItemMetadata>(tmpOffsetsPtr);
                if (tmpOffset.IsFree)
                    continue;
                byte* p = _page.Pointer;
                int entrySize = tmpOffset.Get(ref p);
                tmpHeader.FloorOfData -= (ushort)ComputeRequiredSize(entrySize);
                tmpHeader.FloorOfData &= ContainerPageHeader.Ensure4BytesAlignmentMask; // ensure that this is aligned of 4 bytes boundary
                int entryOffset = tmpHeader.FloorOfData;
                tmpOffset.SetSize(entrySize, tmpPtr, ref entryOffset);
                Memory.Copy(tmpPtr + entryOffset, p, entrySize);
            }

            tmpOffsetsPtr = tmpPtr + PageHeader.SizeOf + (sizeof(ItemMetadata) * (tmpHeader.NumberOfOffsets - 1) );
            while (tmpHeader.NumberOfOffsets > 0)
            {
                ref var tmpOffset = ref Unsafe.AsRef<ItemMetadata>(tmpOffsetsPtr);
                if (tmpOffset.IsFree == false)
                    break;
                tmpOffsetsPtr -= sizeof(ItemMetadata);
                tmpHeader.NumberOfOffsets--;
            }
           
            Debug.Assert(llt.IsDirty(_page.PageNumber));
            Memory.Copy(_page.Pointer, tmpPtr, Constants.Storage.PageSize);
        }

        public static long Allocate(LowLevelTransaction llt, long containerId, int size, out Span<byte> allocatedSpace)
        {
            return Allocate(llt, containerId, size, pageLevelMetadata: -1, out allocatedSpace);
        }

        /// <summary>
        /// The `pageLevelMetadata` element is used to store some information at the page level of the container. It is assumed
        /// that there are *very few* distinct values, since we store that at the page level and all items in the page *must*
        /// share the same value.
        ///
        /// If the current page isn't a match to the pageLevelMetadata value passed, we'll allocate a *new* page for that purpose.
        /// </summary>
        public static long Allocate(LowLevelTransaction llt, long containerId, int size, long pageLevelMetadata, out Span<byte> allocatedSpace)
        {
            long AllocateOverflowPageUnlikely(Container rootContainer, out Span<byte> allocatedSpace)
            {
                // The space to allocate is big enough to be allocated in a dedicated overflow page.
                // We will figure out how many pages we will need to store it.
                var overflowPage = llt.AllocateOverflowRawPage(size, out var numberOfPages);
                var overflowPageHeader = (ContainerPageHeader*)overflowPage.Pointer;
                overflowPageHeader->Flags |= PageFlags.Other;
                overflowPageHeader->ContainerFlags = ExtendedPageType.ContainerOverflow;
                overflowPageHeader->PageLevelMetadata = pageLevelMetadata;

                rootContainer.Header.NumberOfOverflowPages += numberOfPages;
                AddToAllPagesList(llt, rootContainer, overflowPage.PageNumber);

                allocatedSpace = overflowPage.AsSpan(PageHeader.SizeOf, size);
                return overflowPage.PageNumber * Constants.Storage.PageSize;
            }

            // This method will return the allocated space inside the container and also the entry internal id to 
            // address this allocation in the future.
            
            if(size <= 0)
                throw new ArgumentOutOfRangeException(nameof(size));
            
            var rootContainer = new Container(llt.ModifyPage(containerId));
            rootContainer.UpdateNumberOfEntries(1);
            
            if (size > MaxSizeInsideContainerPage)
                return AllocateOverflowPageUnlikely(rootContainer, out allocatedSpace);

            var p = rootContainer.GetNextFreePage();
            var activePage = llt.ModifyPage(p);
            var container = new Container(activePage);
            
            var (reqSize, pos) = container.GetRequiredSizeAndPosition(size);
            bool pageMatch = PageMetadataMatch(container, pageLevelMetadata) &&
                             // we limit the number of entries per page to ensure we always
                             // have the bottom 3 bits free, see also IndexToOffset
                             pos < 1024;

            if (pageMatch == false || 
                container.HasEnoughSpaceFor(reqSize) == false)
            {
                var freedSpace = false;
                if (pageMatch && container.SpaceUsed() < (Constants.Storage.PageSize / 2))
                {
                    container.Defrag(llt);
                    // IMPORTANT: We have to account for the *larger* size here, otherwise we may
                    // have a size based on existing item metadata, but after the defrag, need
                    // to allocate a metadata slot as well. Therefor, we *always* assume that this
                    // is requiring the additional metadata size
                    freedSpace = container.HasEnoughSpaceFor(sizeof(ItemMetadata) + reqSize);
                }

                if (freedSpace == false)
                    container = MoveToNextPage(llt, containerId, pageLevelMetadata, container, size);
                
                (reqSize, pos) = container.GetRequiredSizeAndPosition(size);
                Debug.Assert(pos < 1024, "pos < 1024");
            }

            if (container.HasEnoughSpaceFor(reqSize) == false)
                throw new VoronErrorException($"After checking for space and defrag, we ended up with not enough free space ({reqSize}) on page {container.Header.PageNumber}");

            Debug.Assert(container.Header.PageLevelMetadata == -1 || container.Header.PageLevelMetadata == pageLevelMetadata || pageLevelMetadata == -1, 
                "container.Header.PageLevelMetadata == -1 || container.Header.PageLevelMetadata == pageLevelMetadata || pageLevelMetadata == -1");
            if (pageLevelMetadata != -1) // we may place an entry with -1 in any page, so don't modify the page value if the caller doesn't care
            {
                container.Header.PageLevelMetadata = pageLevelMetadata;
            }
            return container.Allocate(size, pos, out allocatedSpace);
        }

        private long Allocate(int size, int pos, out Span<byte> allocatedSpace)
        {
            Debug.Assert(pos < 1024, "pos < 1024");
            var reqSize = ComputeRequiredSize(size);
            Debug.Assert(HasEnoughSpaceFor(reqSize));

            Header.FloorOfData -= (ushort)reqSize;
            Header.FloorOfData &= ContainerPageHeader.Ensure4BytesAlignmentMask;
            if (pos == Header.NumberOfOffsets)
            {
                Header.NumberOfOffsets++;
            }
            int entryStartOffset = Header.FloorOfData;
            ref ItemMetadata item = ref MetadataFor(pos);
            Debug.Assert(item.IsFree);
            item.SetSize(size, _page.Pointer, ref entryStartOffset);
            allocatedSpace = _page.AsSpan(entryStartOffset, size);

            long id = Header.PageNumber * Constants.Storage.PageSize + IndexToOffset(pos);
            return id;
        }

        private static long IndexToOffset(int pos)
        {
            // Each ItemMetadata == 2 bytes, and item alignment means that
            // min size of item is 4 bytes, 2048 items = 8KB minimum just for
            // the items in the page, so the *max* size is 1,354 items per page
            // assuming all under 4 bytes in size. We consider 4 bytes value to
            // be rare, most of them are likely going to be larger, so we allow
            // up to 1,024 items per page, meaning that we can spare 3 bits in
            // the actual id to allow the caller to reuse in whatever way they like
            return pos << 3;
        }
        
        private static int OffsetToIndex(long offset)
        {
            return (int)offset >> 3;
        }

        private static Container MoveToNextPage(LowLevelTransaction llt, long containerId, long pageLevelMetadata, Container container, int size)
        {
            Debug.Assert(size <= MaxSizeInsideContainerPage);

            var rootPage = llt.ModifyPage(containerId);
            var rootContainer = new Container(rootPage);
            // we'll only remove from the free list if the page level metadata matches, since it probably has space otherwise
            if (pageLevelMetadata == container.Header.PageLevelMetadata)
            {
                // we take it out now..., we'll add to the free list when we delete from it
                RemoveFromFreeList(llt, rootContainer, container._page.PageNumber);
            }
            else
            {
                AddToFreeList(llt, rootContainer, container._page.PageNumber, container.Header.PageLevelMetadata);
            }
            
        
            var txState = llt.Transaction.GetContainerState(containerId);
            if(txState.LastFreePageByPageLevelMetadata.TryGetValue(pageLevelMetadata, out var lastFreePage))
            {
                var page = llt.ModifyPage(lastFreePage);
                var maybe = new Container(page);
                if (maybe.HasEnoughSpaceFor(size + MinimumAdditionalFreeSpaceToConsider))
                    return maybe;
            }

            if (SearchForFreeListPage(llt, rootContainer, txState, pageLevelMetadata, size, out Container nextPage))
            {
                txState.LastFreePageByPageLevelMetadata[pageLevelMetadata] = nextPage._page.PageNumber;
                return nextPage;
            }

            // no existing pages remaining, allocate new one
            var newPage = AllocateContainerPage(llt);
            rootContainer.UpdateNextFreePage(newPage.PageNumber);
            
            container = new Container(newPage);
            container.Header.PageLevelMetadata = pageLevelMetadata;
            
            AddToAllPagesList(llt, rootContainer, newPage.PageNumber);
            AddToFreeList(llt, rootContainer, newPage.PageNumber, pageLevelMetadata);

            txState.LastFreePageByPageLevelMetadata[pageLevelMetadata] = newPage.PageNumber;
            return container;
        }

        private static bool SearchForFreeListPage(LowLevelTransaction llt, Container rootContainer, TransactionState txState, long pageLevelMetadata, int size,
            out Container maybe)
        {
            // PERF: Even if this condition never happens, we need the code to ensure that we have a bounded time to find a free page.
            // This is the case where at some point we need to just give up or end up wasting more time to find a page than the time
            // we will use to create and store in disk a new one.
            
            // We wont work as hard if we know that the entry is too big.
            bool isBigEntry = size >= (Constants.Storage.PageSize / 6);
            int tries = isBigEntry ? 4 : 128;

            var it = new FreeListIterator(llt, txState, pageLevelMetadata);
            for (int i =0; i < tries; i++)
            {
                if (it.MoveNext(out var pageNum) == false)
                    break;

                var page = llt.ModifyPage(pageNum);
                maybe = new Container(page);

                // During indexing, we are going to very quickly shift between different pages 
                // with multiple page-level-metadata, we can't remove it from the free list too soon 
                if (PageMetadataMatch(maybe, pageLevelMetadata) == false)
                    continue;

                // we want to ensure that the free list doesnt get too big...
                // if we don't have space here, we should discard it from the free list
                // however we need to be sure you are not going to do so when the entries
                // are abnormally big. In those cases, the reasonable thing to do is just
                // skip it and create a new page for it but without discarding pages that
                // would be reasonably used by following requests. 
                if (!isBigEntry)
                {
                    RemoveFromFreeList(llt, rootContainer, maybe._page.PageNumber);
                }

                if (maybe.HasEnoughSpaceFor(size + MinimumAdditionalFreeSpaceToConsider) == false)
                    continue;

                // we register it as the next free page
                rootContainer.UpdateNextFreePage(page.PageNumber);

                Debug.Assert(maybe.Header.PageLevelMetadata == pageLevelMetadata || pageLevelMetadata == -1);
                return true;
            }

            maybe = default;
            return false;
        }

        private struct FreeListIterator
        {
            private readonly LowLevelTransaction _llt;
            private readonly long _pageLevelMetadata;
            private readonly TransactionState _txState;
            private Dictionary<long, long>.Enumerator _inMemEnum;
            private Lookup<Int64LookupKey>.ForwardIterator _persistentEnum;
            private bool _hasPersistentIt;

            public FreeListIterator(LowLevelTransaction llt, TransactionState txState, long pageLevelMetadata)
            {
                _llt = llt;
                _txState = txState;
                _pageLevelMetadata = pageLevelMetadata;
                _inMemEnum = _txState.FreeListAdditions.GetEnumerator();
                _persistentEnum = default;
            }

            public bool MoveNext(out long pageNum)
            {
                while (_inMemEnum.MoveNext())
                {
                    if (_pageLevelMetadata != -1 &&
                        _pageLevelMetadata != _inMemEnum.Current.Value) 
                        continue;
                    
                    pageNum = _inMemEnum.Current.Key;
                    return true;
                }

                if (_hasPersistentIt == false)
                {
                    _hasPersistentIt = true;
                    _persistentEnum = _txState.GetFreePages(_llt).Iterate();
                }

                while (_persistentEnum.MoveNext(out Int64LookupKey key, out var pageLevelMetadata, out _))
                {
                    pageNum = key.Value;
                    if(_txState.Removals.Contains(pageNum)) // skip removals from the list
                        continue;
                    if (_txState.FreeListAdditions.ContainsKey(pageNum)) // already scanned those
                        continue;
                    if (_txState.FreeListRemovals.Contains(pageNum)) // was removed from free list, probably for a good reason
                        continue;
                    
                    if (_pageLevelMetadata == -1 || _pageLevelMetadata == pageLevelMetadata)
                        return true;
                }

                pageNum = -1;
                return false;
            }
        }

        private static bool PageMetadataMatch(Container maybe, long pageLevelMetadata)
        {
            if (pageLevelMetadata == -1) // caller doesn't care, can be anything
                return true;
            if (maybe.Header.PageLevelMetadata == -1) // page doesn't have any entries that care, we can modify
                return true;
            return pageLevelMetadata == maybe.Header.PageLevelMetadata;
        }

        public static List<long> GetAllIds(LowLevelTransaction llt, long containerId)
        {
            var list = new List<long>();
            Span<long> items = stackalloc long[256];
            Container rootContainer = new Container(llt.GetPage(containerId));
            var txState = llt.Transaction.GetContainerState(containerId);
            var it = txState.GetAllPages(llt).Iterate();
            it.Reset();
            while(it.MoveNext(out Int64LookupKey key, out _, out _))
            {
                var pageNum = key.Value;
                if(txState.Removals.Contains(pageNum))
                    continue;
                
                var page = llt.GetPage(pageNum);
                int offset = 0;
                int itemsLeftOnCurrentPage = 0;
                do
                {
                    int count = GetEntriesInto(containerId, ref offset, page, items, out itemsLeftOnCurrentPage);
                    list.AddRange(items[..count]);

                    //need read to the end of page
                } while (itemsLeftOnCurrentPage > 0);
            }
            return list;
        }

        public static int GetEntriesInto(long containerId, ref int offset, Page page,  Span<long> ids, out int itemsLeftOnCurrentPage)
        {
            var containerHeader = (ContainerPageHeader*)page.Pointer;
            if (containerHeader->ContainerFlags == ExtendedPageType.ContainerOverflow)
            {
                ids[0] = page.PageNumber * Constants.Storage.PageSize;
                itemsLeftOnCurrentPage = 0;
                return 1;
            }
            else if (containerHeader->ContainerFlags == ExtendedPageType.Container)
            {
                int i = offset;
                if (page.PageNumber == containerId && offset == 0)
                {
                    // skip the free list, all pages list and number of entries items
                    offset += NumberOfReservedEntries;
                    i += NumberOfReservedEntries;
                }

                var container = new Container(page);
                ushort numberOfOffsets = container.Header.NumberOfOffsets;
                var baseOffset = page.PageNumber * Constants.Storage.PageSize;
                int results = 0;
                ref var metadata = ref container.MetadataFor();
                for (; results < ids.Length && i < numberOfOffsets; i++, offset++)
                {
                    if (Unsafe.Add(ref metadata, i).IsFree)
                        continue;

                    ids[results++] = baseOffset + IndexToOffset(i);
                }

                itemsLeftOnCurrentPage = numberOfOffsets - i;
                return results;
            }

            throw new VoronErrorException("The page is not a container page");
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private Span<byte> GetItem(int pos)
        {
            ref var item = ref MetadataFor(pos);
            byte* p = _page.Pointer;
            int size = item.Get(ref p);
            return new Span<byte>(p, size);
        }

        public static void AddToFreeList(LowLevelTransaction llt, in Container rootContainer, long pageNum, long pageLevelMetadata)
        {
            var txState = llt.Transaction.GetContainerState(rootContainer._page.PageNumber);
            ref long valRef = ref CollectionsMarshal.GetValueRefOrAddDefault(txState.FreeListAdditions, pageNum, out var exists);
            if (exists)
            {
                if (valRef != pageLevelMetadata && valRef != -1)
                {
                    throw new NotSupportedException("Cannot change the page level metadata on the free list");
                }
            }

            valRef = pageLevelMetadata;
            txState.FreeListRemovals.Remove(pageNum);
            
            Debug.Assert(txState.Removals.Contains(pageNum) == false);
        }
        
        public static void RemoveFromFreeList(LowLevelTransaction llt, in Container rootContainer, long pageNum)
        {
            var txState = llt.Transaction.GetContainerState(rootContainer._page.PageNumber);
            txState.FreeListAdditions.Remove(pageNum);
            txState.FreeListRemovals.Add(pageNum);
            
            Debug.Assert(txState.Removals.Contains(pageNum) == false);
        }
        
        public static void AddToAllPagesList(LowLevelTransaction llt, in Container rootContainer, long pageNum)
        {
            var txState = llt.Transaction.GetContainerState(rootContainer._page.PageNumber);
            txState.Removals.Remove(pageNum);
            txState.Additions.Add(pageNum);
        }
        
        public static void RemoveFromAllPagesList(LowLevelTransaction llt, in Container rootContainer, long pageNum, long pageLevelMetadata)
        {
            var txState = llt.Transaction.GetContainerState(rootContainer._page.PageNumber);
            
            txState.FreeListAdditions.Remove(pageNum);
            txState.FreeListRemovals.Add(pageNum);
            txState.Additions.Remove(pageNum);
            txState.Removals.Add(pageNum);

            if (txState.LastFreePageByPageLevelMetadata.TryGetValue(pageLevelMetadata, out var page) && page == pageNum)
                txState.LastFreePageByPageLevelMetadata.Remove(pageLevelMetadata);
        }
        
        private bool HasEnoughSpaceFor(int reqSize)
        {
            // we have to take into account 4 bytes alignment
            int nextCeiling = (Header.CeilingOfOffsets + reqSize + 3) & ContainerPageHeader.Ensure4BytesAlignmentMask;
            return nextCeiling < Header.FloorOfData &&
                   // we have a max of 1K items per page, so we ensure we have at least one offset
                   Header.NumberOfOffsets < 1023;
        }

        private (int Size, int Position) GetRequiredSizeAndPosition(int size)
        {
            int reqSize = ComputeRequiredSize(size);

            ushort* metadataPtr = (ushort*)_page.DataPointer;
            var pos = 0;
            ushort numberOfOffsets = Header.NumberOfOffsets;

            if (AdvInstructionSet.IsAcceleratedVector512)
            {
                var N = Vector512<ushort>.Count;
                for (; pos + N <= numberOfOffsets; pos += N)
                {
                    var vec = Vector512.Load(metadataPtr + pos);
                    var cmp = Vector512.Equals(vec, Vector512<ushort>.Zero);
                    ulong bits = cmp.ExtractMostSignificantBits();
                    var first = BitOperations.TrailingZeroCount(bits);
                    if (first < 64)
                        return (reqSize, pos + first);
                }
            }
            
            if (AdvInstructionSet.IsAcceleratedVector256)
            {
                for (; pos + Vector256<ushort>.Count <= numberOfOffsets; pos += Vector256<ushort>.Count)
                {
                    var vec = Vector256.Load(metadataPtr + pos);
                    var cmp = Vector256.Equals(vec, Vector256<ushort>.Zero);
                    uint bits = cmp.ExtractMostSignificantBits();
                    var first = BitOperations.TrailingZeroCount(bits);
                    if(first < 32)
                        return (reqSize, pos + first);
                }
            }
            if (AdvInstructionSet.IsAcceleratedVector128)
            {
                for (; pos + Vector128<ushort>.Count <= numberOfOffsets; pos += Vector128<ushort>.Count)
                {
                    var vec = Vector128.Load(metadataPtr + pos);
                    var cmp = Vector128.Equals(vec, Vector128<ushort>.Zero);
                    uint bits = cmp.ExtractMostSignificantBits();
                    var first = BitOperations.TrailingZeroCount(bits);
                    if(first < 32)
                        return (reqSize, pos + first);
                }
            }

            ref var metadataRef = ref MetadataFor();
            for (; pos < numberOfOffsets; pos++)
            {
                // There is a delete record here, we can reuse this position.
                if (Unsafe.Add(ref metadataRef, pos).IsFree)
                    return (reqSize, pos);
            }
            
            // We reserve a new position.
            return (reqSize + sizeof(ItemMetadata), pos);
        }

        private static int ComputeRequiredSize(int size)
        {
            if (size < 30) return size;
            var isLarge = size > 256;
            return size + 1 + isLarge.ToInt32();
        }

        public static void Delete(LowLevelTransaction llt, long containerId, long id)
        {
            var (pageNum, offset) = Math.DivRem(id, Constants.Storage.PageSize);
            var page = llt.ModifyPage(pageNum);
            Container rootContainer = new Container(llt.ModifyPage(containerId));
            rootContainer.UpdateNumberOfEntries(-1);

            if (page.IsOverflow)
            {
                var numberOfOverflowPages = VirtualPagerLegacyExtensions.GetNumberOfOverflowPages(page.OverflowSize);
                rootContainer.Header.NumberOfOverflowPages -= numberOfOverflowPages;
                long pageLevelMetadata = ((ContainerPageHeader*)page.Pointer)->PageLevelMetadata;
                RemoveFromAllPagesList(llt, rootContainer, pageNum, pageLevelMetadata);
                
                for (var pageToRelease = pageNum; pageToRelease < pageNum + numberOfOverflowPages; ++pageToRelease)
                    llt.FreePage(pageToRelease);
                
                return;
            }

            var index = OffsetToIndex(offset);
            var container = new Container(page);
            Debug.Assert(container.Header.NumberOfOffsets > 0);
            ref var metadata = ref container.MetadataFor(index);
            
            if (metadata.IsFree)
                throw new VoronErrorException("Attempt to delete a container item that was ALREADY DELETED! Item " + id + " on page " + page.PageNumber);

            int totalSize = 0, usedSegments = 0;
            byte* pagePointer = page.Pointer;
            totalSize += container.SpaceUsedInItems(pagePointer, out usedSegments);
            var averageSize = usedSegments == 0 ? 0 : totalSize / usedSegments;
            metadata.Clear(pagePointer);

            // we may change the value of the entriesOffsets, but we can still use the old value
            // because it is still valid (and Size == 0 means ignore it)
            if (index + 1 == container.Header.NumberOfOffsets)
                container.Header.NumberOfOffsets--; // can shrink immediately

            if (container.HasEntries() == false) // cannot delete root page
            {
                Debug.Assert(pageNum != containerId);
                
                // don't need to consider the root page, the free list & all pages
                // entries will ensure that we never get here
                if (rootContainer.GetNextFreePage() == pageNum)
                {
                    // we delete the current free page, so we'll point to ourselves are resolve
                    // the next allocation via the free list
                    rootContainer.UpdateNextFreePage(containerId);
                }

                RemoveFromFreeList(llt, rootContainer, page.PageNumber);
                RemoveFromAllPagesList(llt, rootContainer, page.PageNumber, container.Header.PageLevelMetadata);
                
                llt.FreePage(pageNum);
                return;
            }

            int containerSpaceUsed = container.SpaceUsed();
            if (containerSpaceUsed + (Constants.Storage.PageSize/4) <= Constants.Storage.PageSize && // has at least 25% free
                containerSpaceUsed + averageSize * 2 <= Constants.Storage.PageSize) // has enough space to be on the free list? 
            {
                AddToFreeList(llt, rootContainer, page.PageNumber, container.Header.PageLevelMetadata);
            }
        }

        private long GetNextFreePage()
        {
            ref var metadata = ref MetadataFor(ContainerPageHeader.NextFreePageOffset);
            byte* pagePointer = _page.Pointer;
            Debug.Assert(metadata.IsFree == false);
            int size = metadata.Get(ref pagePointer);
            Debug.Assert(size == sizeof(long));
            return *(long*)pagePointer;
        }
        
        private void UpdateNextFreePage(long nextFreePage)
        {
            ref var metadata = ref MetadataFor(ContainerPageHeader.NextFreePageOffset);
            byte* pagePointer = _page.Pointer;
            Debug.Assert(metadata.IsFree == false);
            int size = metadata.Get(ref pagePointer);
            Debug.Assert(size == sizeof(long));
            *(long*)pagePointer = nextFreePage;
        }
        
        private void UpdateNumberOfEntries(int change)
        {
            ref var metadata = ref MetadataFor(ContainerPageHeader.NumberOfEntriesOffset);
            byte* pagePointer = _page.Pointer;
            Debug.Assert(metadata.IsFree == false);
            int size = metadata.Get(ref pagePointer);
            Debug.Assert(size == sizeof(long));
            *(long*)pagePointer += change;
        }
        
        public long GetNumberOfEntries()
        {
            ref var metadata = ref MetadataFor(ContainerPageHeader.NumberOfEntriesOffset);
            byte* pagePointer = _page.Pointer;
            Debug.Assert(metadata.IsFree == false);
            int size = metadata.Get(ref pagePointer);
            Debug.Assert(size == sizeof(long));
            return *(long*)pagePointer;
        }

        public static Span<byte> GetMutable(LowLevelTransaction llt, long id)
        {
            if (id <= 0)
                throw new InvalidOperationException("Got an invalid container id: " + id);

            var (pageNum, offset) = Math.DivRem(id, Constants.Storage.PageSize);
            var page = llt.ModifyPage(pageNum);

            if (page.IsOverflow)
            {
                Debug.Assert(page.IsOverflow);
                return page.AsSpan(PageHeader.SizeOf, page.OverflowSize);
            }

            var container = new Container(page);
            var metadata = container.MetadataFor(OffsetToIndex(offset));
            Debug.Assert(metadata.IsFree == false);
            Debug.Assert(metadata.IsFree == false, "metadata.IsFree == false");
            var pagePointer= page.Pointer;
            int size = metadata.Get(ref pagePointer);
            return new Span<byte>(pagePointer, size);
        }

        public static void Get(LowLevelTransaction llt, long id, out Item item)
        {
            if (id <= 0)
                throw new InvalidOperationException("Got an invalid container id: " + id);

            var (pageNum, offset) = Math.DivRem(id, Constants.Storage.PageSize);
            var page = llt.GetPage(pageNum);
            if (page.IsOverflow)
            {
                Debug.Assert(page.IsOverflow);
                item = new Item(page, page.DataPointer, page.OverflowSize);
                return;
            }

            var container = new Container(page);
            var itemMetadata = container.MetadataFor(OffsetToIndex(offset));
            Debug.Assert(itemMetadata.IsFree == false);
            byte* pagePointer = page.Pointer;
            var size = itemMetadata.Get(ref pagePointer);
            item = new Item(page, pagePointer, size);
        }

        public static Span<byte> GetReadOnly(LowLevelTransaction llt, long id)
        {
            if (id <= 0)
                throw new InvalidOperationException("Got an invalid container id: " + id);

            var (pageNum, offset) = Math.DivRem(id, Constants.Storage.PageSize);
            var page = llt.GetPage(pageNum);
            if (page.IsOverflow)
            {
                Debug.Assert(page.IsOverflow);
                return new Span<byte>(page.DataPointer, page.OverflowSize);
            }

            var container = new Container(page);
            var itemMetadata = container.MetadataFor(OffsetToIndex(offset));
            Debug.Assert(itemMetadata.IsFree == false);
            byte* pagePointer = page.Pointer;
            var size = itemMetadata.Get(ref pagePointer);
            return new Span<byte>(pagePointer, size);
        }
        
        public static Item Get(LowLevelTransaction llt, long id)
        {
            Get(llt, id, out Item result);
            return result;
        }

        public static Item MaybeGetFromSamePage(LowLevelTransaction llt, ref Page page, long id)
        {
            if (id <= 0)
                throw new InvalidOperationException("Got an invalid container id: " + id);

            var (pageNum, offset) = Math.DivRem(id, Constants.Storage.PageSize);
            if(!page.IsValid || pageNum != page.PageNumber)
                page = llt.GetPage(pageNum);

            if (page.IsOverflow)
            {
                return new Item(page, page.DataPointer, page.OverflowSize);
            }

            var container = new Container(page);
            container.ValidatePage();

            ItemMetadata metadata = container.MetadataFor(OffsetToIndex(offset));
            if (metadata.IsFree)
                throw new InvalidOperationException("Tried to read deleted entry: " + id);

            byte* pagePointer = page.Pointer;
            var size = metadata.Get(ref pagePointer);
            return new Item(page, pagePointer, size);

        }

        [Conditional("DEBUG")]
        private void ValidatePage()
        {
            if (_page.Flags != (PageFlags.Single | PageFlags.Other))
                throw new InvalidDataException("Page " + _page.PageNumber + " is not a container page");
            
            ref var header = ref MemoryMarshal.AsRef<ContainerPageHeader>(_page.AsSpan());

            if (header.ContainerFlags != ExtendedPageType.Container && header.ContainerFlags != ExtendedPageType.ContainerOverflow)
                throw new InvalidDataException("Page " + _page.PageNumber + " is not a container page");
        }

        public readonly struct Item
        {
            private readonly Page _page;
            private readonly byte* _ptr;
            public readonly int Length;

            public Item(Page page, byte* ptr, int size)
            {
                _page = page;
                _ptr = ptr;
                Length = size;
            }

            public byte* Address => _ptr;
            public long PageLevelMetadata => ((ContainerPageHeader*)_page.Pointer)->PageLevelMetadata;
            public Span<byte> ToSpan() => new Span<byte>(_ptr, Length);
            public UnmanagedSpan ToUnmanagedSpan() => new UnmanagedSpan(_ptr, Length);

            public Item IncrementOffset(int offset)
            {
                return new Item(_page, _ptr + offset, Length - offset);
            }
        }

        /// <summary>
        /// Assumes that ids is sorted 
        /// </summary>
        public static void GetAll(LowLevelTransaction llt, Span<long> ids, UnmanagedSpan* spans, long missingValue, PageLocator pageCache)
        {
            for (int i = 0; i < ids.Length; i++)
            {
                if (ids[i]== missingValue)
                {
                    spans[i] = default;
                    continue;
                }
                var (pageNum, offset) = Math.DivRem(ids[i], Constants.Storage.PageSize);

                if (pageCache.TryGetReadOnlyPage(pageNum, out var page) == false)
                {
                    page = llt.GetPage(pageNum);
                    pageCache.SetReadable(page);
                }

                var container = new Container(page);
                
                if (container._page.IsOverflow)
                {
                    spans[i] = new(page.DataPointer, page.OverflowSize);
                    continue;
                }

                var metadata = container.MetadataFor(OffsetToIndex(offset));
                Debug.Assert(metadata.IsFree == false);
                var p = page.Pointer;
                int size = metadata.Get(ref p);
                spans[i] = new(p, size);
            }
        }

        public static (Lookup<Int64LookupKey> allPages, Lookup<Int64LookupKey> freePages) GetPagesFor(LowLevelTransaction tx, long containerId)
        {
            var state = tx.Transaction.GetContainerState(containerId);
            return (state.GetAllPages(tx), state.GetFreePages(tx));
        }
        
        public static Lookup<Int64LookupKey>.ForwardIterator GetAllPagesIterator(LowLevelTransaction tx, long containerId)
        {
            var state = tx.Transaction.GetContainerState(containerId);
            return state.GetAllPages(tx).Iterate();
        }
    }
}

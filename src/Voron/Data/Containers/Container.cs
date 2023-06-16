using System;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.IO;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;
using Sparrow;
using Sparrow.Binary;
using Sparrow.Server;
using Voron.Data.BTrees;
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
        private static readonly Slice AllPagesTreeName;
        private static readonly Slice FreePagesTreeName;

        private const int MinimumAdditionalFreeSpaceToConsider = 64;
        private const int NumberOfReservedEntries = 4; // all pages, free pages, number of entries, next free page

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

            public bool IsFree
            {
                [MethodImpl(MethodImplOptions.AggressiveInlining | MethodImplOptions.AggressiveOptimization)]
                get => (_compactBackingStore & 0x1F) == 0;
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining|MethodImplOptions.AggressiveOptimization)]
            public int Get(ref byte* pagePointer)
            {
                // The offset it stored in bits 5..16, but it is actually 4 bytes
                // aligned, so we shift by 9 to get the raw offset
                var offset = _compactBackingStore >> 3 & 0xFFFC;
                int size = _compactBackingStore & 0x1F;
                switch (size)
                {
                    case 0: // means it is freed 
                        return 0;
                    case 30: // size is one byte  at offset
                        size = *(pagePointer + offset);
                        offset += sizeof(byte);
                        break;
                    case 31: // size is two bytes at offset
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
                int modifiedOffset = (entryOffset << 3); // lowest two bits already cleared
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
                var size = Get(ref pagePointer);
                new Span<byte>(pagePointer, size).Clear();
                _compactBackingStore = 0;
            }
        }
        
        static Container()
        {
            Debug.Assert(sizeof(ItemMetadata) == sizeof(ushort));
            using (StorageEnvironment.GetStaticContext(out var ctx))
            {
                Slice.From(ctx, "AllPagesSet", ByteStringType.Immutable, out AllPagesTreeName);
                Slice.From(ctx, "FreePagesSet", ByteStringType.Immutable, out FreePagesTreeName);
            }
        }        

        private readonly Page _page;

        public ref ContainerPageHeader Header => ref MemoryMarshal.AsRef<ContainerPageHeader>(_page.AsSpan());

        [MethodImpl(MethodImplOptions.AggressiveInlining|MethodImplOptions.AggressiveOptimization)]
        private ref ItemMetadata MetadataFor(int pos)
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

        private bool HasEntries()
        {
            ushort numberOfOffsets = Header.NumberOfOffsets;
            for (var index = 0; index < numberOfOffsets; index++)
            {
                if (MetadataFor(index).IsFree == false)
                    return true;
            }

            return false;
        }

        public int SpaceUsed()
        {
            int numberOfOffsets = Header.NumberOfOffsets;
            var size = numberOfOffsets * sizeof(ItemMetadata) + PageHeader.SizeOf;
            for (int i = 0; i < numberOfOffsets; i++)
            {
                size += MetadataFor(i).GetSize(_page.Pointer);
            }
            return size;
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
            root.Header.NumberOfPages = 1;
            root.Header.NumberOfOverflowPages = 0;
            root.Header.PageLevelMetadata = -1;
            
            root.Allocate(sizeof(TreeRootHeader), ContainerPageHeader.FreeListOffset, out var freeListTreeState);
            root.Allocate(sizeof(TreeRootHeader), ContainerPageHeader.AllPagesOffset, out var allPagesTreeState);
            root.Allocate(sizeof(long), ContainerPageHeader.NumberOfEntriesOffset, out var numberOfEntriesBuffer);
            root.Allocate(sizeof(long), ContainerPageHeader.NextFreePageOffset, out var nextFreePageBuffer);
            Unsafe.WriteUnaligned<long>(ref numberOfEntriesBuffer[0], 4L);
            Unsafe.WriteUnaligned(ref nextFreePageBuffer[0], page.PageNumber);

            // We are creating a set where we will store the free list.
            using var freePagesTree = Tree.Create(llt, llt.Transaction, FreePagesTreeName);
            using var allPagesState = Tree.Create(llt, llt.Transaction, AllPagesTreeName);

            fixed (void* pState = freeListTreeState)
            {
                freePagesTree.State.CopyTo((TreeRootHeader*)pState);
            }

            // We are adding the root to the list of all pages.
            long pageNum = Bits.SwapBytes(page.PageNumber);
            using (Slice.From(llt.Allocator, (byte*)&pageNum, sizeof(long), ByteStringType.Immutable, out var slice))
            {
                allPagesState.DirectAdd(slice, 0, out _);
            }

            fixed (void* pState = allPagesTreeState)
            {
                allPagesState.State.CopyTo((TreeRootHeader*)pState);
            }
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
                tmpHeader.FloorOfData &= 0xFFFC;// ensure that this is aligned of 4 bytes boundary
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
            // This method will return the allocated space inside the container and also the entry internal id to 
            // address this allocation in the future.
            
            if(size <= 0)
                throw new ArgumentOutOfRangeException(nameof(size));
            
            var rootContainer = new Container(llt.ModifyPage(containerId));
            rootContainer.UpdateNumberOfEntries(1);
            
            if (size > MaxSizeInsideContainerPage)
            {
                // The space to allocate is big enough to be allocated in a dedicated overflow page.
                // We will figure out how many pages we will need to store it.
                var overflowPage = llt.AllocateOverflowRawPage(size, out var numberOfPages);
                var overflowPageHeader = (ContainerPageHeader*)overflowPage.Pointer;
                overflowPageHeader->Flags |= PageFlags.Other;
                overflowPageHeader->ContainerFlags = ExtendedPageType.ContainerOverflow;
                overflowPageHeader->PageLevelMetadata = pageLevelMetadata;

                rootContainer.Header.NumberOfOverflowPages += numberOfPages;
                ModifyMetadataList(llt, rootContainer, ContainerPageHeader.AllPagesOffset, add: true, overflowPage.PageNumber);

                allocatedSpace = overflowPage.AsSpan(PageHeader.SizeOf, size);
                return overflowPage.PageNumber * Constants.Storage.PageSize; 
            }

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

            Debug.Assert(container.Header.PageLevelMetadata == -1 || container.Header.PageLevelMetadata == pageLevelMetadata);
            container.Header.PageLevelMetadata = pageLevelMetadata;
            return container.Allocate(size, pos, out allocatedSpace);
        }

        private long Allocate(int size, int pos, out Span<byte> allocatedSpace)
        {
            Debug.Assert(pos < 1024, "pos < 1024");
            var reqSize = ComputeRequiredSize(size);
            Debug.Assert(HasEnoughSpaceFor(reqSize));

            Header.FloorOfData -= (ushort)reqSize;
            Header.FloorOfData &= 0xFF_FC; // ensure 4 bytes alignment
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
            RemovePageFromContainerFreeList( rootContainer,  container);// we take it out now..., we'll add to the free list when we delete from it
            
            // We wont work as hard if we know that the entry is too big.
            bool isBigEntry = size >= (Constants.Storage.PageSize / 6);
            int tries = isBigEntry ? 4 : 128;

            // PERF: Even if this condition never happens, we need the code to ensure that we have a bounded time to find a free page.
            // This is the case where at some point we need to just give up or end up wasting more time to find a page than the time
            // we will use to create and store in disk a new one.
            int i = 0;
            for (; i < tries; i++)  
            {                
                var freeListStateSpan = rootContainer.GetItem(ContainerPageHeader.FreeListOffset);
                Tree freeList;
                fixed (void* pSate = freeListStateSpan)
                {
                    freeList = Tree.Open(llt, llt.Transaction, FreePagesTreeName, (TreeRootHeader*)pSate);
                }
                var it = freeList.Iterate(prefetch:false);
                if (it.MoveNext() == false)
                    break;

                ValueReader readerForCurrent = it.CurrentKey.CreateReader();
                long pageNum = readerForCurrent.ReadBigEndianInt64();
                var page = llt.ModifyPage(pageNum);
                var maybe = new Container(page);

                // we want to ensure that the free list doesnt get too big...
                // if we don't have space here, we should discard it from the free list
                // however we need to be sure you are not going to do so when the entries
                // are abnormally big. In those cases, the reasonable thing to do is just
                // skip it and create a new page for it but without discarding pages that
                // would be reasonably used by following requests. 
                
                // if we exclude this based on the pageLevelMetadata match, this is fine,
                // since we assume that there are *long* usage periods for each pageLevelMetadata
                // so we aren't expected to switch between them too often
                if (!isBigEntry || 
                    PageMetadataMatch(maybe, pageLevelMetadata))
                {
                    RemovePageFromContainerFreeList(rootContainer, maybe);
                }

                if (maybe.HasEnoughSpaceFor(size + MinimumAdditionalFreeSpaceToConsider) == false)
                    continue;
                
                // we register it as the next free page
                rootContainer.UpdateNextFreePage(page.PageNumber);
                
                Debug.Assert(container.Header.PageLevelMetadata == pageLevelMetadata || pageLevelMetadata == -1);
                return  maybe;
            }

            // no existing pages remaining, allocate new one
            var newPage = AllocateContainerPage(llt);
            rootContainer.Header.NumberOfPages++;
            rootContainer.UpdateNextFreePage(newPage.PageNumber);
            
            container = new Container(newPage);
            container.Header.PageLevelMetadata = pageLevelMetadata;
            
            AddPageToContainerFreeList(rootContainer, container);

            return container;

            void RemovePageFromContainerFreeList(Container parent, Container page)
            {
                page.Header.OnFreeList = false;
                ModifyMetadataList(llt, parent, ContainerPageHeader.FreeListOffset, add: false, page.Header.PageNumber);
            }

            void AddPageToContainerFreeList(Container parent, Container page)
            {
                page.Header.OnFreeList = true;
                ModifyMetadataList(llt, parent, ContainerPageHeader.AllPagesOffset, add: true, page.Header.PageNumber);
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

            var it = GetAllPagesSet(llt, containerId);
            while(it.TryMoveNext(out var pageNum))
            {
                var page = llt.GetPage(pageNum);
                int offset = 0;
                int itemsLeftOnCurrentPage = 0;
                do
                {
                    int count = GetEntriesInto(containerId, ref offset, page, items, out itemsLeftOnCurrentPage);

                    for(int i = 0; i < count; ++i)
                        list.Add(items[i]);
                    
                    //need read to the end of page
                } while (itemsLeftOnCurrentPage > 0);
            }
            return list;
        }

        public static int GetEntriesInto(long containerId, ref int offset, Page page, Span<long> ids, out int itemsLeftOnCurrentPage)
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
                for (; results < ids.Length && i < numberOfOffsets; i++, offset++)
                {
                    if (container.MetadataFor(i).IsFree)
                        continue;

                    ids[results++] = baseOffset + IndexToOffset(i);
                }

                itemsLeftOnCurrentPage = numberOfOffsets - i;
                return results;
            }

            throw new VoronErrorException("The page is not a container page");
        }

        public struct AllPagesIterator
        {
            private readonly Tree _tree;
            private readonly TreeIterator _iterator;
            private bool _hasValue;

            public AllPagesIterator(Tree tree)
            {
                _tree = tree;
                _iterator = _tree.Iterate(prefetch: false);
                _hasValue = _iterator.Seek(Slices.BeforeAllKeys);
            }

            public bool TryMoveNext(out long pageNum)
            {
                if (_hasValue == false)
                {
                    _iterator.Dispose();
                    _tree.Dispose();
                    pageNum = -1;
                    return false;
                }
                pageNum = _iterator.CurrentKey.CreateReader().ReadBigEndianInt64();
                _hasValue = _iterator.MoveNext();
                return true;
            }
        }
        
        public static AllPagesIterator GetAllPagesSet(LowLevelTransaction llt, long containerId)
        {
            var rootPage = llt.GetPage(containerId);
            var rootContainer = new Container(rootPage);
            Tree tree;
            fixed (void* pState = rootContainer.GetItem(ContainerPageHeader.AllPagesOffset))
            {
                tree = Tree.Open(llt, llt.Transaction, AllPagesTreeName, (TreeRootHeader*)pState);
            }

            return new AllPagesIterator(tree);

        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private Span<byte> GetItem(int pos)
        {
            ref var item = ref MetadataFor(pos);
            byte* p = _page.Pointer;
            int size = item.Get(ref p);
            return new Span<byte>(p, size);
        }

        private static void ModifyMetadataList(LowLevelTransaction llt, in Container rootContainer, int offset, bool add, long value)
        {
            fixed (void* pState = rootContainer.GetItem(offset))
            {
                Debug.Assert(llt.IsDirtyPage(rootContainer._page.PageNumber));
                Tree tree = Tree.Open(llt, llt.Transaction, AllPagesTreeName, (TreeRootHeader*)pState);
                value = Bits.SwapBytes(value);
                using (Slice.External(llt.Allocator, (byte*)&value, sizeof(long), ByteStringType.Immutable, out var slice))
                {
                    if (add)
                        tree.DirectAdd(slice, 0, out _);
                    else
                        tree.Delete(slice);
                }
                tree.State.CopyTo((TreeRootHeader*)pState);
            }
        }

        private bool HasEnoughSpaceFor(int reqSize)
        {
            // we have to take into account 4 bytes alignment
            int nextCeiling = (Header.CeilingOfOffsets + reqSize + 3) & 0xFFFC;
            return nextCeiling < Header.FloorOfData;
        }

        private (int Size, int Position) GetRequiredSizeAndPosition(int size)
        {
            var pos = 0;
            int reqSize = ComputeRequiredSize(size);
            ushort numberOfOffsets = Header.NumberOfOffsets;
            for (; pos < numberOfOffsets; pos++)
            {
                // There is a delete record here, we can reuse this position.
                if (MetadataFor(pos).IsFree)
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
               rootContainer.Header.NumberOfOverflowPages -= VirtualPagerLegacyExtensions.GetNumberOfOverflowPages(page.OverflowSize);
                ModifyMetadataList(llt, rootContainer, ContainerPageHeader.AllPagesOffset, add: false, pageNum);
                llt.FreePage(pageNum);
                return;
            }

            var index = OffsetToIndex(offset);
            var container = new Container(page);
            var numberOfOffsets = container.Header.NumberOfOffsets;
            Debug.Assert(numberOfOffsets > 0);
            ref var metadata = ref container.MetadataFor(index);
            
            if (metadata.IsFree)
                throw new VoronErrorException("Attempt to delete a container item that was ALREADY DELETED! Item " + id + " on page " + page.PageNumber);

            int totalSize = 0, count = 0;
            byte* pagePointer = page.Pointer;
            for (var i = 0; i < numberOfOffsets; i++)
            {
                int size = container.MetadataFor(i).GetSize(pagePointer);
                if(size == 0)
                    continue;
                count++;
                totalSize += size;
            }

            var averageSize = count == 0 ? 0 : totalSize / count;
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
                
                ModifyMetadataList(llt, rootContainer, ContainerPageHeader.AllPagesOffset, add: false, page.PageNumber);
                if (container.Header.OnFreeList)
                    ModifyMetadataList(llt, rootContainer, ContainerPageHeader.FreeListOffset, add: false, page.PageNumber);
                
                llt.FreePage(pageNum);
                return;
            }

            if (container.Header.OnFreeList)
                return;
            
            int containerSpaceUsed = container.SpaceUsed();
            if (container.Header.OnFreeList == false && // already on it, can skip. 
                containerSpaceUsed + (Constants.Storage.PageSize/4) <= Constants.Storage.PageSize && // has at least 25% free
                containerSpaceUsed + averageSize * 2 <= Constants.Storage.PageSize) // has enough space to be on the free list? 
            {
                container.Header.OnFreeList = true;
                ModifyMetadataList(llt, rootContainer, ContainerPageHeader.FreeListOffset, add: true, page.PageNumber);
            }
        }

        public static long GetNextFreePage(LowLevelTransaction llt, long containerId)
        {
             return new Container(llt.GetPage(containerId)).GetNextFreePage();
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
            var pagePointer= page.Pointer;
            int size = metadata.Get(ref pagePointer);
            return new Span<byte>(pagePointer, size);
        }

        public static Item Get(LowLevelTransaction llt, long id)
        {
            if (id <= 0)
                throw new InvalidOperationException("Got an invalid container id: " + id);
            
            var (pageNum, offset) = Math.DivRem(id, Constants.Storage.PageSize);
            var page = llt.GetPage(pageNum);
            if (page.IsOverflow)
            {
                Debug.Assert(page.IsOverflow);
                return new Item(page, page.DataPointer, page.OverflowSize);
            }

            var container = new Container(page);
            var itemMetadata = container.MetadataFor(OffsetToIndex(offset));
            Debug.Assert(itemMetadata.IsFree == false);
            byte* pagePointer = page.Pointer;
            var size = itemMetadata.Get(ref pagePointer);
            return new Item(page, pagePointer, size);
        }

        public static Item MaybeGetFromSamePage(LowLevelTransaction llt, ref Page page, long id)
        {
            if (id == 0)
                throw new InvalidOperationException("Got an invalid container id: 0");

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
                    pageCache.SetReadable(pageNum, page);
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
    }
}

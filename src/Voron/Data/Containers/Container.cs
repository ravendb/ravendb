using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Diagnostics.Contracts;
using System.IO;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
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
        private const int NumberOfReservedEntries = 3; // all pages, free pages, number of entries

        static Container()
        {
            using (StorageEnvironment.GetStaticContext(out var ctx))
            {
                Slice.From(ctx, "AllPagesSet", ByteStringType.Immutable, out AllPagesTreeName);
                Slice.From(ctx, "FreePagesSet", ByteStringType.Immutable, out FreePagesTreeName);
            }
        }        

        private readonly Page _page;

        public ref ContainerPageHeader Header => ref MemoryMarshal.AsRef<ContainerPageHeader>(_page.AsSpan());

        public Span<ItemMetadata> Offsets => _page.AsSpan<ItemMetadata>(PageHeader.SizeOf, Header.NumberOfOffsets);
        

        [StructLayout(LayoutKind.Explicit, Pack = 1, Size = 4)]
        public struct ItemMetadata
        {
            [FieldOffset(0)]
            public ushort Offset;

            [FieldOffset(2)]
            public ushort Size;

            public override string ToString()
            {
                return "Offset: " + Offset + ", Size: " + Size;
            }

            public ItemMetadata( ushort offset, ushort size )
            {
                Offset = offset;
                Size = size;
            }
        }

        private bool HasEntries(in Span<ItemMetadata> offsets)
        {
            for (var index = 0; index < offsets.Length; index++)
            {
                var item = offsets[index];
                if (item.Offset != 0)
                    return true;
            }

            return false;
        }

        public int SpaceUsed()
        {
            return SpaceUsed(Offsets);
        }

        private int SpaceUsed(in Span<ItemMetadata> offsets)
        {
            var size = Header.NumberOfOffsets * sizeof(ItemMetadata) + PageHeader.SizeOf;
            foreach (var item in offsets) 
                size += item.Size;
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
            var page = llt.AllocatePage(1);

            InitializeContainerPage(page);

            var root = new Container(page);
            root.Header.NumberOfPages = 1;
            root.Header.NumberOfOverflowPages = 0;
            root.Header.NextFreePage = page.PageNumber;

            root.Allocate(sizeof(TreeRootHeader), ContainerPageHeader.FreeListOffset, out var freeListTreeState);
            root.Allocate(sizeof(TreeRootHeader), ContainerPageHeader.AllPagesOffset, out var allPagesTreeState);
            root.Allocate(sizeof(long), ContainerPageHeader.NumberOfEntriesOffset, out var numberOfEntriesBuffer);
            ref var numberOfEntries = ref MemoryMarshal.AsRef<long>(numberOfEntriesBuffer);
            numberOfEntries = 3;

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

        private static void InitializeContainerPage(Page page)
        {
            ref var header = ref MemoryMarshal.AsRef<ContainerPageHeader>(page.AsSpan());
            header.Flags = PageFlags.Single | PageFlags.Other;
            header.ContainerFlags = ExtendedPageType.Container;
            header.FloorOfData = Constants.Storage.PageSize;
        }

        // this is computed so this will fit exactly two items of max size in a container page. Beyond that, we'll have enough
        // fragmentation that we might as well use a dedicated page.
        public const int MaxSizeInsideContainerPage = (Constants.Storage.PageSize - PageHeader.SizeOf) / 2 - sizeof(ushort) * 4;

        private void Defrag(LowLevelTransaction llt)
        {
            using var _ = llt.Environment.GetTemporaryPage(llt, out var tmp);
            var tmpSpan = tmp.AsSpan();
            _page.AsSpan(0, Header.CeilingOfOffsets).CopyTo(tmpSpan);
            tmpSpan.Slice(Header.CeilingOfOffsets).Clear();
            ref var tmpHeader = ref MemoryMarshal.AsRef<ContainerPageHeader>(tmpSpan);
            var tmpOffsets = MemoryMarshal.Cast<byte, ItemMetadata>(tmpSpan.Slice(PageHeader.SizeOf)).Slice(0, tmpHeader.NumberOfOffsets);
            tmpHeader.FloorOfData = Constants.Storage.PageSize;
            for (var i = 0; i < tmpOffsets.Length; i++)
            {
                if (tmpOffsets[i].Size == 0)
                    continue;
                tmpHeader.FloorOfData -= tmpOffsets[i].Size;
                _page.AsSpan(tmpOffsets[i].Offset, tmpOffsets[i].Size).CopyTo(tmpSpan.Slice(tmpHeader.FloorOfData));
                tmpOffsets[i].Offset = tmpHeader.FloorOfData;
            }

            while (tmpHeader.NumberOfOffsets > 0)
            {
                if (tmpOffsets[tmpHeader.NumberOfOffsets - 1].Size != 0)
                    break;
                tmpHeader.NumberOfOffsets--;
            }

            tmpSpan.CopyTo(_page.AsSpan());
        }


        public static long Allocate(LowLevelTransaction llt, long containerId, int size, out Span<byte> allocatedSpace)
        {
            // This method will return the allocated space inside the container and also the entry internal id to 
            // address this allocation in the future.
            
            if(size <= 0)
                throw new ArgumentOutOfRangeException(nameof(size));
            
            var rootContainer = new Container(llt.ModifyPage(containerId));
            ref var numberOfEntries = ref GetNumberOfEntriesRef(rootContainer);
            numberOfEntries++;
            
            if (size > MaxSizeInsideContainerPage)
            {
                // The space to allocate is big enough to be allocated in a dedicated overflow page.
                // We will figure out how many pages we will need to store it.
                var overflowPage = llt.AllocateOverflowRawPage(size, out var numberOfPages);
                var overflowPageHeader = (ContainerPageHeader*)overflowPage.Pointer;
                overflowPageHeader->Flags |= PageFlags.Other;
                overflowPageHeader->ContainerFlags = ExtendedPageType.ContainerOverflow;

                rootContainer.Header.NumberOfOverflowPages += numberOfPages;
                ModifyMetadataList(llt, rootContainer, ContainerPageHeader.AllPagesOffset, add: true, overflowPage.PageNumber);

                allocatedSpace = overflowPage.AsSpan(PageHeader.SizeOf, size);
                return overflowPage.PageNumber * Constants.Storage.PageSize; 
            }

            var activePage = llt.ModifyPage(rootContainer.Header.NextFreePage);
            var container = new Container(activePage);
            
            (var reqSize, var pos) = GetRequiredSizeAndPosition(size, container);
            if (container.HasEnoughSpaceFor(reqSize) == false)
            {
                var freedSpace = false;
                if (container.SpaceUsed(container.Offsets) < (Constants.Storage.PageSize / 2))
                {
                    container.Defrag(llt);
                    // IMPORTANT: We have to account for the *larger* size here, otherwise we may
                    // have a size based on existing item metadata, but after the defrag, need
                    // to allocate a metadata slot as well. Therefor, we *always* assume that this
                    // is requiring the additional metadata size
                    freedSpace = container.HasEnoughSpaceFor(sizeof(ItemMetadata) + size);
                }

                if (freedSpace == false)
                    container = MoveToNextPage(llt, containerId, container, size);
                
                (reqSize, pos) = GetRequiredSizeAndPosition(size, container);
            }

            if (container.HasEnoughSpaceFor(reqSize) == false)
                throw new VoronErrorException($"After checking for space and defrag, we ended up with not enough free space ({reqSize}) on page {container.Header.PageNumber}");
            
            return container.Allocate(size, pos, out allocatedSpace);
        }

        private long Allocate(int size, int pos, out Span<byte> allocatedSpace)
        {
            Debug.Assert(HasEnoughSpaceFor(size));

            Header.FloorOfData -= (ushort)size;
            if (pos == Header.NumberOfOffsets)
                Header.NumberOfOffsets++;

            allocatedSpace = _page.AsSpan(Header.FloorOfData, size);

            ref ItemMetadata item = ref Offsets[pos];
            item.Offset = Header.FloorOfData;
            item.Size = (ushort)size;

            return Header.PageNumber * Constants.Storage.PageSize + IndexToOffset(pos);
        }

        private static long IndexToOffset(int pos)
        {
            return PageHeader.SizeOf + pos * sizeof(ItemMetadata);
        }

        private static Container MoveToNextPage(LowLevelTransaction llt, long containerId, Container container, int size)
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
                if (it.Seek(Slices.BeforeAllKeys) == false)
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
                if (!isBigEntry)
                {
                    RemovePageFromContainerFreeList(rootContainer, maybe);
                }

                if (maybe.HasEnoughSpaceFor(size + MinimumAdditionalFreeSpaceToConsider) == false)
                    continue;
                
                // we register it as the next free page
                rootContainer.Header.NextFreePage = page.PageNumber;
                return  maybe;
            }

            // no existing pages remaining, allocate a new one
            
            rootContainer.Header.NumberOfPages++;
            var newPage = llt.AllocatePage(1);
            InitializeContainerPage(newPage);
            container = new Container(newPage);
            AddPageToContainerFreeList(rootContainer, container);
            ModifyMetadataList(llt, rootContainer, ContainerPageHeader.AllPagesOffset, add: true, newPage.PageNumber);

            rootContainer.Header.NextFreePage = newPage.PageNumber;

            return container;

            void RemovePageFromContainerFreeList(Container parent, Container page)
            {
                page.Header.OnFreeList = false;
                ModifyMetadataList(llt, parent, ContainerPageHeader.FreeListOffset, add: false, page.Header.PageNumber);
            }

            void AddPageToContainerFreeList(Container parent, Container page)
            {
                page.Header.OnFreeList = true;
                ModifyMetadataList(llt, parent, ContainerPageHeader.FreeListOffset, add: true, page.Header.PageNumber);
            }
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
                var entriesOffsets = container.Offsets;
                var baseOffset = page.PageNumber * Constants.Storage.PageSize;
                int results = 0;
                for (; results < ids.Length && i < entriesOffsets.Length; i++, offset++)
                {
                    if (entriesOffsets[i].Size == 0)
                        continue;

                    ids[results++] = baseOffset + IndexToOffset(i);
                }

                itemsLeftOnCurrentPage = entriesOffsets.Length - i;
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
        private Span<byte> GetItem(int offset)
        {
            ref var item = ref Offsets[offset];
            return _page.AsSpan(item.Offset, item.Size);
        }

        private static void ModifyMetadataList(LowLevelTransaction llt, in Container rootContainer, int offset, bool add, long value)
        {
            Debug.Assert(llt.IsDirtyPage(rootContainer._page.PageNumber));
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
            return Header.CeilingOfOffsets + reqSize < Header.FloorOfData;
        }

        private static (int Size, int Position) GetRequiredSizeAndPosition(int size, Container container)
        {
            var pos = 0;
            var offsets = container.Offsets;
            for (; pos < offsets.Length; pos++)
            {
                // There is a delete record here, we can reuse this position.
                if (offsets[pos].Size == 0)
                    return (size, pos);
            }
            
            // We reserve a new position.
            return (size + sizeof(ItemMetadata), pos);
        }

        public static void Delete(LowLevelTransaction llt, long containerId, long id)
        {
            var offset = (int)(id % Constants.Storage.PageSize);
            var pageNum = id / Constants.Storage.PageSize;
            var page = llt.ModifyPage(pageNum);
            Container rootContainer = new Container(llt.ModifyPage(containerId));
            ref long numberOfEntries = ref GetNumberOfEntriesRef(rootContainer);
            numberOfEntries--;

            if (offset == 0)
            {
                Debug.Assert(page.IsOverflow);
               rootContainer.Header.NumberOfOverflowPages -= VirtualPagerLegacyExtensions.GetNumberOfOverflowPages(page.OverflowSize);
                ModifyMetadataList(llt, rootContainer, ContainerPageHeader.AllPagesOffset, add: false, pageNum);
                llt.FreePage(pageNum);
                return;
            }

            var container = new Container(page);
            var entriesOffsets = container.Offsets;
            Debug.Assert(entriesOffsets.Length > 0);            
            var index = (offset - PageHeader.SizeOf) / sizeof(ItemMetadata);
            var metadata = entriesOffsets[index];
            
            if (metadata.Size == 0)
                throw new VoronErrorException("Attempt to delete a container item that was ALREADY DELETED! Item " + id + " on page " + page.PageNumber);

            int totalSize = 0, count = 0;
            for (var i = 0; i < entriesOffsets.Length; i++)
            {
                if(entriesOffsets[i].Size ==0)
                    continue;
                count++;
                totalSize += entriesOffsets[i].Size;
            }

            var averageSize = count == 0 ? 0 : totalSize / count;
            container._page.AsSpan(metadata.Offset, metadata.Size).Clear();
            entriesOffsets[index] = default;

            // we may change the value of the entriesOffsets, but we can still use the old value
            // because it is still valid (and Size == 0 means ignore it)
            if (index + 1 == container.Header.NumberOfOffsets)
                container.Header.NumberOfOffsets--; // can shrink immediately

            if (container.HasEntries(entriesOffsets) == false) // cannot delete root page
            {
                Debug.Assert(pageNum != containerId);
                
                // don't need to consider the root page, the free list & all pages
                // entries will ensure that we never get here
                if (rootContainer.Header.NextFreePage == pageNum)
                {
                    // we delete the current free page, so we'll point to ourselves are resolve
                    // the next allocation via the free list
                    rootContainer.Header.NextFreePage = containerId;
                }
                
                ModifyMetadataList(llt, rootContainer, ContainerPageHeader.AllPagesOffset, add: false, page.PageNumber);
                if (container.Header.OnFreeList)
                    ModifyMetadataList(llt, rootContainer, ContainerPageHeader.FreeListOffset, add: false, page.PageNumber);
                
                llt.FreePage(pageNum);
                return;
            }

            if (container.Header.OnFreeList)
                return;
            
            int containerSpaceUsed = container.SpaceUsed(entriesOffsets);
            if (container.Header.OnFreeList == false && // already on it, can skip. 
                containerSpaceUsed + (Constants.Storage.PageSize/4) <= Constants.Storage.PageSize && // has at least 25% free
                containerSpaceUsed + averageSize * 2 <= Constants.Storage.PageSize) // has enough space to be on the free list? 
            {
                container.Header.OnFreeList = true;
                ModifyMetadataList(llt, rootContainer, ContainerPageHeader.FreeListOffset, add: true, page.PageNumber);
            }
        }

        private static ref long GetNumberOfEntriesRef(Container rootContainer)
        {
            ref var metadata = ref rootContainer.Offsets[ContainerPageHeader.NumberOfEntriesOffset];
            Debug.Assert(metadata.Size != 0);
            return ref MemoryMarshal.AsRef<long>(rootContainer._page.AsSpan(metadata.Offset, metadata.Size));
        }
        
        public long GetNumberOfEntries()
        {
            ref var metadata = ref Offsets[ContainerPageHeader.NumberOfEntriesOffset];
            Debug.Assert(metadata.Size != 0);
            return MemoryMarshal.AsRef<long>(_page.AsSpan(metadata.Offset, metadata.Size));
        }

        public static Span<byte> GetMutable(LowLevelTransaction llt, long id)
        {
            var pageNum = id / Constants.Storage.PageSize;
            var page = llt.ModifyPage(pageNum);

            return GetInternal(id, page);
        }

        private static Span<byte> GetInternal(long id, Page page)
        {
            var offset = (int)(id % Constants.Storage.PageSize);
            if (offset == 0)
            {
                Debug.Assert(page.IsOverflow);
                return page.AsSpan(PageHeader.SizeOf, page.OverflowSize);
            }

            var container = new Container(page);
            return container.Get(offset);
        }

        public static Item Get(LowLevelTransaction llt, long id)
        {
            var pageNum = Math.DivRem(id, Constants.Storage.PageSize, out var offset);
            var page = llt.GetPage(pageNum);
            if (offset == 0)
            {
                Debug.Assert(page.IsOverflow);
                return new Item(page, PageHeader.SizeOf, page.OverflowSize);
            }

            var container = new Container(page);
            
            ItemMetadata* metadata = (ItemMetadata*)(container._page.Pointer + offset);
            Debug.Assert(metadata->Size != 0);
            return new Item(page, metadata->Offset, metadata->Size);
        }

        public static Item MaybeGetFromSamePage(LowLevelTransaction llt, ref Page page, long id)
        {
            var pageNum = Math.DivRem(id, Constants.Storage.PageSize, out var offset);
            if(!page.IsValid || pageNum != page.PageNumber)
                page = llt.GetPage(pageNum);

            int size;
            if (offset == 0) // overflow
            {
                if (page.IsOverflow == false)
                    throw new InvalidOperationException("Expected to get an overflow page " + page.PageNumber);

                size = page.OverflowSize;
                offset = PageHeader.SizeOf;
            }
            else
            {
                var container = new Container(page);
                container.ValidatePage();

                ItemMetadata* metadata = (ItemMetadata*)(container._page.Pointer + offset);
                if (metadata->Size == 0)
                    throw new InvalidOperationException("Tried to read deleted entry: " + id);

                size = metadata->Size;
                offset = metadata->Offset;
            }

            return new Item(page, (int)offset, size);
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

        private Span<byte> Get(int offset)
        {
            ItemMetadata* metadata = (ItemMetadata*)(_page.Pointer + offset);
            Debug.Assert(metadata->Size != 0);
            return _page.AsSpan(metadata->Offset, metadata->Size);
        }

        private static int OffsetToIndex(int offset)
        {
            return (offset - PageHeader.SizeOf) / sizeof(ItemMetadata);
        }

        public readonly struct Item
        {
            private readonly Page Page;
            private readonly int Offset;
            public readonly int Length;

            public Item(Page page, int offset, int size)
            {
                Page = page;
                Offset = offset;
                Length = size;
            }

            public byte* Address => Page.Pointer + Offset;

            public Span<byte> ToSpan() => new Span<byte>(Page.Pointer + Offset, Length);
            public UnmanagedSpan ToUnmanagedSpan() => new UnmanagedSpan(Page.Pointer + Offset, Length);

            public Item IncrementOffset(int offset)
            {
                return new Item(Page, Offset + offset, Length - offset);
            }
        }
    }
}

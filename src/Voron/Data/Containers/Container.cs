using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.InteropServices;
using Sparrow.Server;
using Voron.Data.Sets;
using Voron.Global;
using Voron.Impl;
using Voron.Impl.Paging;

namespace Voron.Data.Containers
{
    public readonly unsafe ref struct Container
    {
        public static readonly Slice AllPagesSetName;
        public static readonly Slice FreePagesSetName;
        
        public const int MinimumAdditionalFreeSpaceToConsider = 64;

        static Container()
        {
            using (StorageEnvironment.GetStaticContext(out var ctx))
            {
                Slice.From(ctx, "AllPagesSet", ByteStringType.Immutable, out AllPagesSetName);
                Slice.From(ctx, "FreePagesSet", ByteStringType.Immutable, out FreePagesSetName);
            }
        }
        public ref ContainerPageHeader Header => ref MemoryMarshal.AsRef<ContainerPageHeader>(Span);

        public Span<ItemMetadata> Offsets => MemoryMarshal.Cast<byte, ItemMetadata>(Span.Slice(PageHeader.SizeOf)).Slice(0, Header.NumberOfOffsets);
        public readonly Span<byte> Span;

        [StructLayout(LayoutKind.Explicit, Pack = 1, Size = 4)]
        public struct ItemMetadata
        {
            [FieldOffset(0)]
            public ushort Offset;

            [FieldOffset(2)]
            public ushort Size;

            public Span<byte> Slice(Span<byte> span) => span.Slice(Offset, Size);

            public override string ToString()
            {
                return "Offset: " + Offset + ", Size: " + Size;
            }
        }

        public bool HasEntries
        {
            get
            {
                for (var index = 0; index < Offsets.Length; index++)
                {
                    var item = Offsets[index];
                    if (item.Offset != 0)
                        return true;
                }

                return false;
            }
        }

        public int SpaceUsed
        {
            get
            {
                var size = Header.NumberOfOffsets * sizeof(ItemMetadata) + PageHeader.SizeOf;
                foreach (var item in Offsets) size += item.Size;
                return size;
            }
        }

        private Container(Span<byte> span)
        {
            Debug.Assert(span.Length == Constants.Storage.PageSize);
            Span = span;
        }

        public static long Create(LowLevelTransaction llt)
        {
            var page = llt.AllocatePage(1);
            InitializePage(page);
            var root = new Container(page.AsSpan());
            root.Header.NumberOfPages = 1;
            root.Header.NumberOfOverflowPages = 0;
            root.Header.NextFreePage = page.PageNumber;

            root.Allocate(sizeof(SetState), ContainerPageHeader.FreeListOffset, out var freeListSet);
            root.Allocate(sizeof(SetState), ContainerPageHeader.AllPagesOffset, out var allPagesSet);

            ref var freeListState = ref MemoryMarshal.AsRef<SetState>(freeListSet);
            Set.Initialize(llt, ref freeListState);
            
            ref var allPagesState = ref MemoryMarshal.AsRef<SetState>(allPagesSet);
            Set.Initialize(llt, ref allPagesState);
            Set allPages = new Set(llt, AllPagesSetName,MemoryMarshal.AsRef<SetState>(allPagesSet));
            allPages.Add(page.PageNumber);
            allPagesState = allPages.State;
            return page.PageNumber;
        }

        private static void InitializePage(Page page)
        {
            ref var header = ref MemoryMarshal.AsRef<ContainerPageHeader>(page.AsSpan());
            header.Flags = PageFlags.Single | PageFlags.Other;
            header.ContainerFlags = ExtendedPageType.Container;
            header.FloorOfData = Constants.Storage.PageSize;
        }

        // this is computed so this will fit exactly two items of max size in a container page. Beyond that, we'll have enough
        // fragmentation that we might as well use a dedicated page.
        private const int MaxSizeInsideContainerPage = (Constants.Storage.PageSize - PageHeader.SizeOf) / 2 - sizeof(ushort) * 4;

        private void Defrag(LowLevelTransaction llt)
        {
            using var _ = llt.Environment.GetTemporaryPage(llt, out var tmp);
            var tmpSpan = tmp.AsSpan();
            Span.Slice(0, Header.CeilingOfOffsets).CopyTo(tmpSpan);
            tmpSpan.Slice(Header.CeilingOfOffsets).Clear();
            ref var tmpHeader = ref MemoryMarshal.AsRef<ContainerPageHeader>(tmpSpan);
            var tmpOffsets = MemoryMarshal.Cast<byte, ItemMetadata>(tmpSpan.Slice(PageHeader.SizeOf)).Slice(0, tmpHeader.NumberOfOffsets);
            tmpHeader.FloorOfData = Constants.Storage.PageSize;
            for (var i = 0; i < tmpOffsets.Length; i++)
            {
                if (tmpOffsets[i].Size == 0)
                    continue;
                tmpHeader.FloorOfData -= tmpOffsets[i].Size;
                Span.Slice(tmpOffsets[i].Offset, tmpOffsets[i].Size).CopyTo(tmpSpan.Slice(tmpHeader.FloorOfData));
                tmpOffsets[i].Offset = tmpHeader.FloorOfData;
            }

            while (tmpHeader.NumberOfOffsets > 0)
            {
                if (tmpOffsets[tmpHeader.NumberOfOffsets - 1].Size != 0)
                    break;
                tmpHeader.NumberOfOffsets--;
            }

            tmpSpan.CopyTo(Span);
        }

        public static bool TryAdjustSize(LowLevelTransaction llt, long id, int newSize, out Span<byte> buffer)
        {
            var offset = (int)(id % Constants.Storage.PageSize);
            var pageNum = id / Constants.Storage.PageSize;
            var page = llt.GetPage(pageNum);
            var container = new Container(page.AsSpan());
            var existing = container.Get(offset);
            if (container.SpaceUsed + (newSize - existing.Length) > Constants.Storage.PageSize)
            {
                buffer = default;
                return false;
            }

            page = llt.ModifyPage(pageNum);
            container = new Container(page.AsSpan());
            using var _ = llt.Allocator.Allocate(existing.Length, out Span<byte> tmp);
            existing.CopyTo(tmp);
            existing.Clear();
            ref var metadata = ref container.Offsets[OffsetToIndex(offset)];
            if (container.HasEnoughSpaceFor(newSize) == false)
            {
                metadata.Size = 0;
                container.Defrag(llt);
            }

            container.Header.FloorOfData -= (ushort)newSize;
            buffer = container.Span.Slice(container.Header.FloorOfData, newSize);
            tmp.CopyTo(buffer);
            metadata.Offset = container.Header.FloorOfData;
            metadata.Size = (ushort)newSize;
            return true;
        }

        public static long Allocate(LowLevelTransaction llt, long containerId, int size, out Span<byte> allocatedSpace)
        {
            Container rootContainer;
            if (size > MaxSizeInsideContainerPage)
            {      
                var root = llt.ModifyPage(containerId);
                rootContainer = new Container(root.AsSpan());

                var numberOfPages = VirtualPagerLegacyExtensions.GetNumberOfOverflowPages(size);
                var overflowPage = llt.AllocatePage(numberOfPages);

                rootContainer.Header.NumberOfOverflowPages += numberOfPages;
                ModifyMetadataList(llt, rootContainer, AllPagesSetName, ContainerPageHeader.AllPagesOffset, add: true, overflowPage.PageNumber);
                
                overflowPage.OverflowSize = size;
                overflowPage.Flags |= PageFlags.Overflow | PageFlags.Other;
                allocatedSpace = overflowPage.AsSpan(numberOfPages).Slice(PageHeader.SizeOf, size);
                return overflowPage.PageNumber * Constants.Storage.PageSize;
            }

            var rootPage = llt.GetPage(containerId);
            rootContainer = new Container(rootPage.AsSpan());

            var activePage = llt.ModifyPage(rootContainer.Header.NextFreePage);
            var container = new Container(activePage.AsSpan());
            (var reqSize, var pos) = GetRequiredSizeAndPosition(size, container);

            if (container.HasEnoughSpaceFor(reqSize) == false)
            {
                container.Defrag(llt);
                if (container.HasEnoughSpaceFor(reqSize) == false) 
                    MoveToNextPage(llt, containerId, ref container, size);
                
                (reqSize, pos) = GetRequiredSizeAndPosition(size, container);
                Debug.Assert(container.HasEnoughSpaceFor(reqSize));
            }

            return container.Allocate(size, pos, out allocatedSpace);
        }

        private long Allocate(int size, int pos, out Span<byte> allocatedSpace)
        {
            Debug.Assert(HasEnoughSpaceFor(size));

            Header.FloorOfData -= (ushort)size;
            if (pos == Header.NumberOfOffsets)
                Header.NumberOfOffsets++;

            allocatedSpace = Span.Slice(Header.FloorOfData, size);
            Offsets[pos] = new ItemMetadata {Offset = Header.FloorOfData, Size = (ushort)size};
            return Header.PageNumber * Constants.Storage.PageSize + IndexToOffset(pos);
        }

        private static long IndexToOffset(int pos)
        {
            return PageHeader.SizeOf + pos * sizeof(ItemMetadata);
        }

        private static void MoveToNextPage(LowLevelTransaction llt, long containerId, ref Container container, int size)
        {
            Debug.Assert(size <= MaxSizeInsideContainerPage);

            container.Header.OnFreeList = false; // we take it out now..., we'll add to the free list when we delete from it
            var rootPage = llt.ModifyPage(containerId);
            var rootContainer = new Container(rootPage.AsSpan());
            var freeListStateSpan = rootContainer.Offsets[ContainerPageHeader.FreeListOffset].Slice(rootContainer.Span);
            var freeList = new Set(llt, FreePagesSetName, MemoryMarshal.AsRef<SetState>(freeListStateSpan));
            using (var it = freeList.Iterate())
            {
                if (it.Seek(0))
                {
                    do
                    {
                        var page = llt.GetPage(it.Current);
                        var maybe = new Container(page.AsSpan());
                        if (maybe.HasEnoughSpaceFor(size + MinimumAdditionalFreeSpaceToConsider) == false)
                            continue;

                        maybe.Header.OnFreeList = false;
                        ModifyMetadataList(llt,  rootContainer, FreePagesSetName, ContainerPageHeader.FreeListOffset, add: false, page.PageNumber);
                        rootContainer.Header.NextFreePage = page.PageNumber;
                        
                        container = maybe;
                        return;
                    } while (it.MoveNext());   
                }
            }

            // no existing pages remaining, allocate new one
            var newPage = llt.AllocatePage(1);
            InitializePage(newPage);
            rootContainer.Header.NumberOfPages++;
            rootContainer.Header.NextFreePage = newPage.PageNumber;
            container = new Container(newPage.AsSpan());
            container.Header.OnFreeList = true; // added as the next free page
            
            ModifyMetadataList(llt, rootContainer, AllPagesSetName, ContainerPageHeader.AllPagesOffset, add: true, newPage.PageNumber);
        }

        public static List<long> GetAllIds(LowLevelTransaction llt, long containerId)
        {
            var set = GetAllPagesList(llt, containerId);
            using var it = set.Iterate();
            var list = new List<long>();
            if (it.Seek(0) == false)
                return list;
            do
            {
                var page = llt.GetPage(it.Current);
                if (page.IsOverflow)
                {
                    list.Add(page.PageNumber * Constants.Storage.PageSize);
                    continue;
                }
                var container = new Container(page.AsSpan());
                int numberOfOffsets = container.Offsets.Length;
                int i = 0;
                
                if (it.Current == containerId)
                    i += 2; // skip the free list and all pages list entries
                
                for (; i < numberOfOffsets; i++)
                {
                    list.Add(page.PageNumber * Constants.Storage.PageSize + IndexToOffset(i));
                }
            } while (it.MoveNext());

            return list;
        }

        private static Set GetAllPagesList(LowLevelTransaction llt, long containerId)
        {
            var rootPage = llt.ModifyPage(containerId);
            var rootContainer = new Container(rootPage.AsSpan());
            var span = rootContainer.Offsets[ContainerPageHeader.AllPagesOffset].Slice(rootContainer.Span);
            ref var state = ref MemoryMarshal.AsRef<SetState>(span);
            var set = new Set(llt, AllPagesSetName, state);
            return set;
        }

        private static void ModifyMetadataList(LowLevelTransaction llt, in Container rootContainer, Slice name, int offset, bool add, long value)
        {
            var span = rootContainer.Offsets[offset].Slice(rootContainer.Span);
            ref var state = ref MemoryMarshal.AsRef<SetState>(span);
            var set = new Set(llt, name, state);
            if (add)
                set.Add(value);
            else
                set.Remove(value);
            state = set.State;
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
                if (offsets[pos].Size == 0)
                    // we reuse the space
                    return (size, pos);
            }

            return (size + sizeof(ItemMetadata), pos);
        }

        public static void Delete(LowLevelTransaction llt, long containerId, long id)
        {
            var offset = (int)(id % Constants.Storage.PageSize);
            var pageNum = id / Constants.Storage.PageSize;
            var page = llt.ModifyPage(pageNum);

            if (offset == 0)
            {
                Debug.Assert(page.IsOverflow);
                var root = llt.ModifyPage(containerId);
                Container rootContainer = new Container(root.AsSpan());
                rootContainer.Header.NumberOfOverflowPages -= VirtualPagerLegacyExtensions.GetNumberOfOverflowPages(page.OverflowSize);
                ModifyMetadataList(llt,  rootContainer, AllPagesSetName, ContainerPageHeader.AllPagesOffset, add: false, pageNum);
                llt.FreePage(pageNum);
                return;
            }

            var container = new Container(page.AsSpan());
            Debug.Assert(container.Offsets.Length > 0);
            var index = (offset - PageHeader.SizeOf) / sizeof(ItemMetadata);
            var metadata = container.Offsets[index];
            Debug.Assert(metadata.Size != 0);
            var totalSize = 0;
            for (var i = 0; i < container.Offsets.Length; i++)
            {
                totalSize += container.Offsets[i].Size;
            }

            var averageSize = totalSize / container.Offsets.Length;
            container.Span.Slice(metadata.Offset, metadata.Size).Clear();
            container.Offsets[index] = default;
            if (index + 1 == container.Header.NumberOfOffsets)
                container.Header.NumberOfOffsets--; // can shrink immediately

            if (container.HasEntries == false && 
                pageNum != containerId) // cannot delete root page
            {
                // don't need to consider the root page, the free list & all pages
                // entries will ensure that we never get here

                llt.FreePage(pageNum);
                var rootPage = llt.ModifyPage(containerId);
                var rootContainer = new Container(rootPage.AsSpan());
                if (rootContainer.Header.NextFreePage == pageNum)
                {
                    // we delete the current free page, so we'll point to ourselves are resolve
                    // the next allocation via the free list
                    rootContainer.Header.NextFreePage = containerId;
                }
                ModifyMetadataList(llt, rootContainer, AllPagesSetName, ContainerPageHeader.AllPagesOffset, add: false, page.PageNumber);
                if (container.Header.OnFreeList)
                    ModifyMetadataList(llt, rootContainer, FreePagesSetName, ContainerPageHeader.FreeListOffset, add: false, page.PageNumber);
            }

            if (container.Header.OnFreeList)
                return;

            if (container.Header.OnFreeList == false && // already on it, can skip. 
                container.SpaceUsed + averageSize * 2 <= Constants.Storage.PageSize) // has enough space to be on the free list? 
            {
                container.Header.OnFreeList = true;
                var rootContainer = new Container(llt.ModifyPage(containerId).AsSpan());
                ModifyMetadataList(llt, rootContainer, FreePagesSetName, ContainerPageHeader.FreeListOffset, add: true, page.PageNumber);
            }
        }

        public static ReadOnlySpan<byte> Get(LowLevelTransaction llt, long id)
        {
            var offset = (int)(id % Constants.Storage.PageSize);
            var pageNum = id / Constants.Storage.PageSize;
            var page = llt.GetPage(pageNum);

            if (offset == 0)
            {
                Debug.Assert(page.IsOverflow);
                int numberOfPages = VirtualPagerLegacyExtensions.GetNumberOfOverflowPages(page.OverflowSize);
                return page.AsSpan(numberOfPages).Slice(PageHeader.SizeOf, page.OverflowSize);
            }

            var container = new Container(page.AsSpan());

            return container.Get(offset);
        }

        private Span<byte> Get(int offset)
        {
            ref var metadata = ref Offsets[OffsetToIndex(offset)];
            return Span.Slice(metadata.Offset, metadata.Size);
        }

        private static int OffsetToIndex(int offset)
        {
            return (offset - PageHeader.SizeOf) / sizeof(ItemMetadata);
        }
    }
}

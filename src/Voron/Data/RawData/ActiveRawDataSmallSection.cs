using System;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Text;
using Sparrow;
using Voron.Data.Tables;
using Voron.Exceptions;
using Voron.Global;
using Voron.Impl;
using Voron.Impl.Paging;

namespace Voron.Data.RawData
{
    /// <summary>
    ///     Handles small values (lt 2Kb) by packing them into pages
    ///     It will allocate a 512 pages (2MB in using 4KB pages) and work with them.
    ///     It can grow up to 2,000 pages (7.8 MB in size using 4KB pages), the section size
    ///     is dependent on the size of the database file.
    ///     All attempts are made to reduce the number of times that we need to move data, even
    ///     at the cost of fragmentation.
    /// </summary>
    public unsafe class ActiveRawDataSmallSection : RawDataSection
    {
        public ActiveRawDataSmallSection(LowLevelTransaction tx, long pageNumber)
            : base(tx, pageNumber)
        {
        }

        /// <summary>
        ///     Try allocating some space in the section, defrag if needed (including moving other valid entries)
        ///     Once a section returned false for try allocation, it should be retired as an actively allocating
        ///     section, and a new one will be generated for new values.
        /// </summary>
        public bool TryAllocate(int size, out long id)
        {
            var allocatedSize = (short)size;
            size += sizeof(RawDataEntrySizes);

            // we need to have the size value here, so we add that
            if (allocatedSize <= 0)
                throw new ArgumentException($"Size must be greater than zero, but was {allocatedSize}");

            if (size > MaxItemSize || size > short.MaxValue)
                throw new ArgumentException($"Cannot allocate an item of {size} bytes in a small data section. Maximum is: {MaxItemSize}");

            //  start reading from the last used page, to skip full pages
            for (var i = _sectionHeader->LastUsedPage; i < _sectionHeader->NumberOfPages; i++)
            {
                if (AvailableSpace[i] < size)
                    continue;

                var pageHeader = PageHeaderFor(_sectionHeader->PageNumber + i + 1);
                if (pageHeader->NextAllocation + size > Constants.Storage.PageSize)
                    continue;

                // best case, we have enough space, and we don't need to defrag
                pageHeader = ModifyPage(pageHeader);
                id = (pageHeader->PageNumber) * Constants.Storage.PageSize + pageHeader->NextAllocation;
                var sizes = (RawDataEntrySizes*)((byte*)pageHeader + pageHeader->NextAllocation);
                sizes->AllocatedSize = allocatedSize;
                sizes->UsedSize = 0;
                pageHeader->NextAllocation += (ushort)size;
                pageHeader->NumberOfEntries++;
                EnsureHeaderModified();
                AvailableSpace[i] -= (ushort)size;
                _sectionHeader->NumberOfEntries++;
                _sectionHeader->LastUsedPage = i;
                _sectionHeader->AllocatedSize += size;
                return true;
            }

            // we don't have any pages that are free enough, we need to check if we 
            // need to fragment, so we will scan from the start, see if we have anything
            // worth doing, and defrag if needed
            for (ushort i = 0; i < _sectionHeader->NumberOfPages; i++)
            {
                if (AvailableSpace[i] < size)
                    continue;
                // we have space, but we need to defrag
                var pageHeader = PageHeaderFor(_sectionHeader->PageNumber + i + 1);
                pageHeader = DefragPage(pageHeader);

                id = (pageHeader->PageNumber) * Constants.Storage.PageSize + pageHeader->NextAllocation;
                ((short*)((byte*)pageHeader + pageHeader->NextAllocation))[0] = allocatedSize;
                pageHeader->NextAllocation += (ushort)size;
                pageHeader->NumberOfEntries++;
                EnsureHeaderModified();
                _sectionHeader->NumberOfEntries++;
                _sectionHeader->LastUsedPage = i;
                _sectionHeader->AllocatedSize += size;
                AvailableSpace[i] = (ushort)(Constants.Storage.PageSize - pageHeader->NextAllocation);

                return true;
            }

            // we don't have space, caller need to allocate new small section?
            id = -1;
            return false;
        }

        public string DebugDump(RawDataSmallPageHeader* pageHeader)
        {
            var sb =
                new StringBuilder(
                    $"Page {pageHeader->PageNumber}, {pageHeader->NumberOfEntries} entries, next allocation: {pageHeader->NextAllocation}")
                    .AppendLine();

            for (int i = sizeof(RawDataSmallPageHeader); i < pageHeader->NextAllocation;)
            {
                var oldSize = (RawDataEntrySizes*)((byte*)pageHeader + i);
                sb.Append($"{i} - {oldSize->AllocatedSize} / {oldSize->UsedSize} - ");

                if (oldSize->UsedSize > 0)
                {
                    var tvr = new TableValueReader((byte*)pageHeader + i + sizeof(RawDataEntrySizes),
                        oldSize->UsedSize);

                    sb.Append(tvr.Count);
                }

                sb.AppendLine();
                i += oldSize->AllocatedSize + sizeof(RawDataEntrySizes);
            }

            return sb.ToString();

        }

        private RawDataSmallPageHeader* DefragPage(RawDataSmallPageHeader* pageHeader)
        {
            pageHeader = ModifyPage(pageHeader);

            if (pageHeader->NumberOfEntries == 0)
            {
                pageHeader->NextAllocation = (ushort)sizeof(RawDataSmallPageHeader);
                Memory.Set((byte*)pageHeader + pageHeader->NextAllocation, 0,
                    Constants.Storage.PageSize - pageHeader->NextAllocation);

                return pageHeader;
            }


            TemporaryPage tmp;
            using (_tx.Environment.GetTemporaryPage(_tx, out tmp))
            {
                var maxUsedPos = pageHeader->NextAllocation;
                Memory.Copy(tmp.TempPagePointer, (byte*)pageHeader, Constants.Storage.PageSize);

                pageHeader->NextAllocation = (ushort)sizeof(RawDataSmallPageHeader);
                Memory.Set((byte*)pageHeader + pageHeader->NextAllocation, 0,
                    Constants.Storage.PageSize - pageHeader->NextAllocation);

                pageHeader->NumberOfEntries = 0;
                var pos = pageHeader->NextAllocation;
                while (pos < maxUsedPos)
                {
                    var oldSize = (RawDataEntrySizes*)(tmp.TempPagePointer + pos);

                    if (oldSize->AllocatedSize <= 0)
                        VoronUnrecoverableErrorException.Raise(_tx, $"Allocated size cannot be zero or negative, but was {oldSize->AllocatedSize} in page {pageHeader->PageNumber}");

                    if (oldSize->UsedSize < 0)
                    {
                        pos += (ushort)(oldSize->AllocatedSize + sizeof(RawDataEntrySizes));
                        continue; // this was freed
                    }
                    var prevId = (pageHeader->PageNumber) * Constants.Storage.PageSize + pos;
                    var newId = (pageHeader->PageNumber) * Constants.Storage.PageSize + pageHeader->NextAllocation;
                    if (prevId != newId)
                    {
                        OnDataMoved(prevId, newId, tmp.TempPagePointer + pos + sizeof(RawDataEntrySizes), oldSize->UsedSize);
                    }

                    var newSize = (RawDataEntrySizes*)(((byte*)pageHeader) + pageHeader->NextAllocation);
                    newSize->AllocatedSize = oldSize->AllocatedSize;
                    newSize->UsedSize = oldSize->UsedSize;
                    pageHeader->NextAllocation += (ushort)sizeof(RawDataEntrySizes);
                    pageHeader->NumberOfEntries++;
                    Memory.Copy(((byte*)pageHeader) + pageHeader->NextAllocation, tmp.TempPagePointer + pos + sizeof(RawDataEntrySizes),
                        oldSize->UsedSize);

                    pageHeader->NextAllocation += (ushort)oldSize->AllocatedSize;
                    pos += (ushort)(oldSize->AllocatedSize + sizeof(RawDataEntrySizes));
                }
            }
            return pageHeader;
        }

        [MethodImpl(MethodImplOptions.NoInlining)]
        public static ActiveRawDataSmallSection Create(LowLevelTransaction tx, string owner, byte tableType, ushort? sizeInPages = null)
        {
            Slice ownerSlice;
            Slice.From(tx.Allocator, owner, ByteStringType.Immutable, out ownerSlice);
            return Create(tx, ownerSlice, tableType, sizeInPages);
        }

        public static ActiveRawDataSmallSection Create(LowLevelTransaction tx, Slice owner, byte tableType, ushort? sizeInPages)
        {
            var dbPagesInSmallSection = GetNumberOfPagesInSmallSection(tx);
            var numberOfPagesInSmallSection = Math.Min(sizeInPages ?? dbPagesInSmallSection, dbPagesInSmallSection);
            Debug.Assert((numberOfPagesInSmallSection * 2) + ReservedHeaderSpace <= Constants.Storage.PageSize);

            var sectionStart = tx.AllocatePage(numberOfPagesInSmallSection);
            numberOfPagesInSmallSection--; // we take one page for the active section header
            Debug.Assert(numberOfPagesInSmallSection > 0);
            tx.BreakLargeAllocationToSeparatePages(sectionStart.PageNumber);

            var sectionHeader = (RawDataSmallSectionPageHeader*)sectionStart.Pointer;
            sectionHeader->RawDataFlags = RawDataPageFlags.Header;
            sectionHeader->Flags = PageFlags.RawData | PageFlags.Single;
            sectionHeader->NumberOfEntries = 0;
            sectionHeader->NumberOfPages = numberOfPagesInSmallSection;
            sectionHeader->LastUsedPage = 0;
            sectionHeader->SectionOwnerHash = Hashing.XXHash64.Calculate(owner.Content.Ptr, (ulong)owner.Content.Length);
            sectionHeader->TableType = tableType;
            
            var availablespace = (ushort*)((byte*)sectionHeader + ReservedHeaderSpace);

            for (ushort i = 0; i < numberOfPagesInSmallSection; i++)
            {
                var pageHeader = (RawDataSmallPageHeader*)(sectionStart.Pointer + (i + 1) * Constants.Storage.PageSize);
                Debug.Assert(pageHeader->PageNumber == sectionStart.PageNumber + i + 1);
                pageHeader->NumberOfEntries = 0;
                pageHeader->PageNumberInSection = i;
                pageHeader->RawDataFlags = RawDataPageFlags.Small;
                pageHeader->Flags = PageFlags.RawData | PageFlags.Single;
                pageHeader->NextAllocation = (ushort)sizeof(RawDataSmallPageHeader);
                pageHeader->SectionOwnerHash = sectionHeader->SectionOwnerHash;
                pageHeader->TableType = tableType;
                availablespace[i] = (ushort)(Constants.Storage.PageSize - sizeof(RawDataSmallPageHeader));
            }

            return new ActiveRawDataSmallSection(tx, sectionStart.PageNumber);
        }

        /// <summary>
        /// We choose the length of the section based on the overall db size.
        /// The idea is that we want to avoid pre-allocating a lot of data all at once when we are small, which
        /// can blow up our file size
        /// </summary>
        private static ushort GetNumberOfPagesInSmallSection(LowLevelTransaction tx)
        {
            // all sizes are with 8 Kb page size
            if (tx.DataPager.NumberOfAllocatedPages > 1024 * 32) // 256 MB 
            {
                // roughly 16 MB
                return (Constants.Storage.PageSize - ReservedHeaderSpace) / 2;
            }
            if (tx.DataPager.NumberOfAllocatedPages > 1024 * 16) // 64 MB
            {
                // 8 MB
                return 1024;
            }
            if (tx.DataPager.NumberOfAllocatedPages > 1024 * 8) // 32 MB
            {
                // 4 MB
                return 512;
            }
            if (tx.DataPager.NumberOfAllocatedPages > 1024 * 4) // 16 MB
            {
                // 2 MB
                return 128;
            }
            // we are less than 16 MB
            // 512 KB
            return 32;
        }

        public bool IsOwned(long id)
        {
            var posInPage = (int)(id % Constants.Storage.PageSize);
            var pageNumberInSection = (id - posInPage) / Constants.Storage.PageSize;

            // same section, obbviously owned
            if (pageNumberInSection > _sectionHeader->PageNumber &&
                pageNumberInSection <= _sectionHeader->PageNumber + _sectionHeader->NumberOfPages)
                return true;

            var pageHeader = PageHeaderFor(pageNumberInSection);
            var sectionPageNumber = pageHeader->PageNumber - pageHeader->PageNumberInSection - 1;
            var idSectionHeader = (RawDataSmallSectionPageHeader*)_tx.GetPage(sectionPageNumber).Pointer;

            return idSectionHeader->SectionOwnerHash == _sectionHeader->SectionOwnerHash;
        }

    }
}

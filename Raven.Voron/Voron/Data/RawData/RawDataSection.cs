using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Runtime.CompilerServices;
using Sparrow;
using Voron.Data.BTrees;
using Voron.Impl;
using Voron.Impl.Paging;

namespace Voron.Data.RawData
{
    public unsafe delegate void DataMovedDelegate(long previousId, long newId, byte* data);

    /// <summary>
    /// Handles small values (lt 2Kb) by packing them into pages
    /// 
    /// It will allocate a 512 pages (2MB in using 4KB pages) and work with them.
    /// It can grow up to 2,000 pages (7.8 MB in size using 4KB pages).
    /// 
    /// All attempts are made to reduce the number of times that we need to move data, even
    /// at the cost of fragmentation.  
    /// </summary>
    public unsafe class RawDataSmallSection
    {
        public long PageNumber { get; }
        private readonly LowLevelTransaction _tx;
        private RawDataSmallSectionPageHeader* _sectionHeader;
        public readonly int MaxItemSize;
        private int _pageSize;
        private HashSet<long> _dirtyPages = new HashSet<long>();

        public event DataMovedDelegate DataMoved;

        const ushort ReservedHeaderSpace = 96;



        public RawDataSmallSection(LowLevelTransaction tx, long pageNumber)
        {
            PageNumber = pageNumber;
            _tx = tx;
            _pageSize = _tx.DataPager.PageSize;

            MaxItemSize = (_pageSize - sizeof(RawDataSmallPageHeader)) / 2;

            _sectionHeader = (RawDataSmallSectionPageHeader*)_tx.GetPage(pageNumber).Pointer;
        }

        public bool TryWrite(long id, byte* data, int size)
        {
            var posInPage = (int)(id % _pageSize);
            var pageNumberInSection = (id - posInPage)/_pageSize;
            var pageHeader = PageHeaderFor(pageNumberInSection);
            if (posInPage >= pageHeader->NextAllocation)
                throw new InvalidDataException("Asked to load a past the allocated values: " + id + " from page " +
                                               pageHeader->PageNumber);

            var sizes = (short*)((byte*)pageHeader + posInPage);
            if (sizes[1] < 0)
                throw new InvalidDataException("Asked to load a value that was already freed: " + id + " from page " +
                                               pageHeader->PageNumber);

            if (sizes[0] < sizes[1])
                throw new InvalidDataException(
                    "Asked to load a value that where the allocated size is smaller than the used size: " + id +
                    " from page " +
                    pageHeader->PageNumber);

            if (sizes[0] < size)
                return false; // can't write here

           
            pageHeader = ModifyPage(pageHeader);
            var writePos = ((byte*)pageHeader + posInPage + sizeof(short) /*allocated*/+ sizeof(short) /*used*/);
            // note that we have to do this calc again, pageHeader might have changed
            ((short*)((byte*)pageHeader + posInPage))[1] = (short)size;
            Memory.Copy(writePos, data, size);
            return true;
        }

        public byte* DirectRead(long id, out int size)
        {
            var posInPage = (int)(id % _pageSize);
            var pageNumberInSection = (id - posInPage) / _pageSize;
            var pageHeader = PageHeaderFor(pageNumberInSection);
            if (posInPage >= pageHeader->NextAllocation)
                throw new InvalidDataException("Asked to load a past the allocated values: " + id + " from page " +
                                               pageHeader->PageNumber);

            var sizes = (short*)((byte*)pageHeader + posInPage);
            if (sizes[1] < 0)
                throw new InvalidDataException("Asked to load a value that was already freed: " + id + " from page " +
                                               pageHeader->PageNumber);

            if (sizes[0] < sizes[1])
                throw new InvalidDataException(
                    "Asked to load a value that where the allocated size is smaller than the used size: " + id +
                    " from page " +
                    pageHeader->PageNumber);

            size = sizes[1];
            return ((byte*)pageHeader + posInPage + sizeof(short) /*allocated*/+ sizeof(short) /*used*/);
        }

        /// <summary>
        /// Try allocating some space in the section, defrag if needed (including moving other valid entries)
        /// Once a section returned false for try allocation, it should be retired as an actively allocating
        /// section, and a new one will be generated for new values.
        /// </summary>
        public bool TryAllocate(int size, out long id)
        {
            var allocatedSize = (short)size;
            size += sizeof(short) /*allocated size */+ sizeof(short) /*actual size*/;
            // we need to have the size value here, so we add that

            if (size > MaxItemSize || size > short.MaxValue)
                throw new ArgumentException("Cannot allocate an item of " + size +
                                            " bytes in a small data section. Maximum is: " + MaxItemSize);

            //  start reading from the last used page, to skip full pages
            for (var i = _sectionHeader->LastUsedPage; i < _sectionHeader->NumberOfPages; i++)
            {
                var pageHeader = PageHeaderFor(_sectionHeader->PageNumber + i + 1);
                if (AvailableSpace[i] < size ||
                    pageHeader->NextAllocation + size > _pageSize)
                    continue;

                // best case, we have enough space, and we don't need to defrag
                pageHeader = ModifyPage(pageHeader);
                id = (pageHeader->PageNumber) * _pageSize + pageHeader->NextAllocation;
                ((short*) ((byte*) pageHeader + pageHeader->NextAllocation))[0] = allocatedSize;
                pageHeader->NextAllocation += (ushort)size;
                pageHeader->NumberOfEntries++;
                EnsureHeaderModified();
                _sectionHeader->NumberOfEntriesInSection++;
                _sectionHeader->LastUsedPage = i;
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
                id = (pageHeader->PageNumber) * _pageSize + pageHeader->NextAllocation;
                ((short*)((byte*)pageHeader + pageHeader->NextAllocation))[0] = allocatedSize;
                pageHeader->NextAllocation += (ushort)size;
                pageHeader->NumberOfEntries++;
                EnsureHeaderModified();
                _sectionHeader->NumberOfEntriesInSection++;
                _sectionHeader->LastUsedPage = i;
                return true;
            }

            // we don't have space, caller need to allocate new small section?
            id = -1;
            return false;
        }

        public ushort* AvailableSpace
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get
            {
                return (ushort*)((byte*)_sectionHeader + ReservedHeaderSpace);
            }
        }

        private unsafe RawDataSmallPageHeader* DefragPage(RawDataSmallPageHeader* pageHeader)
        {
            pageHeader = ModifyPage(pageHeader);

            TemporaryPage tmp;
            using (_tx.Environment.GetTemporaryPage(_tx, out tmp))
            {
                var maxUsedPos = pageHeader->NextAllocation;
                Memory.Copy(tmp.TempPagePointer, (byte*)pageHeader, _pageSize);
                pageHeader->NextAllocation = (ushort)(
                    _sectionHeader->PageNumber == pageHeader->PageNumber
                        ? sizeof(RawDataSmallSectionPageHeader)
                        : sizeof(RawDataSmallPageHeader)
                    );
                Memory.Set((byte*)pageHeader + pageHeader->NextAllocation, 0,
                    _pageSize - pageHeader->NextAllocation);

                pageHeader->NumberOfEntries = 0;
                var pos = pageHeader->NextAllocation;
                while (pos < maxUsedPos)
                {
                    var sizes = ((short*)(tmp.TempPagePointer + pos));
                    var allocatedSize = sizes[0];
                    if (allocatedSize < 0)
                        throw new InvalidDataException("Allocated size cannot be negative, but was " + allocatedSize +
                                                       " in page " + pageHeader->PageNumber);
                    var usedSize = sizes[1]; // used size
                    if (usedSize < 0)
                    {
                        pos += (ushort)allocatedSize;
                        continue; // this was freed
                    }

                    if (DataMoved != null)
                    {
                        var prevId = (pageHeader->PageNumber) * _pageSize + pos;
                        var newId = (pageHeader->PageNumber) * _pageSize + pageHeader->NextAllocation;
                        if (prevId != newId)
                        {
                            DataMoved(prevId, newId, tmp.TempPagePointer + pos);
                        }
                    }

                    sizes = (short*)(((byte*)pageHeader) + pageHeader->NextAllocation);
                    sizes[0] = usedSize; // allocated
                    sizes[1] = usedSize; // used
                    pageHeader->NextAllocation += sizeof(short) + sizeof(short);

                    Memory.Copy(((byte*)pageHeader) + pageHeader->NextAllocation, tmp.TempPagePointer + pos,
                        usedSize);

                    pageHeader->NextAllocation += (ushort)usedSize;
                    pos += (ushort)allocatedSize;
                }
            }
            return pageHeader;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void EnsureHeaderModified()
        {
            if (_dirtyPages.Add(_sectionHeader->PageNumber) == false)
                return;
            var page = _tx.ModifyPage(_sectionHeader->PageNumber);
            _sectionHeader = (RawDataSmallSectionPageHeader*)page.Pointer;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private RawDataSmallPageHeader* ModifyPage(RawDataSmallPageHeader* pageHeader)
        {
            if (_dirtyPages.Add(pageHeader->PageNumber) == false)
                return pageHeader;
            var page = _tx.ModifyPage(pageHeader->PageNumber);
            return (RawDataSmallPageHeader*)page.Pointer;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private unsafe RawDataSmallPageHeader* PageHeaderFor(long pageNumber)
        {
            return (RawDataSmallPageHeader*)(_tx.GetPage(pageNumber).Pointer);
        }

        public static RawDataSmallSection Create(LowLevelTransaction tx)
        {
            ushort numberOfPagesInSmallSection = 512;
            if (tx.DataPager.NumberOfAllocatedPages > 1024 * 32)
            {
                numberOfPagesInSmallSection = (ushort)(tx.DataPager.PageSize - ReservedHeaderSpace);
            }
            else if (tx.DataPager.NumberOfAllocatedPages > 1024 * 16)
            {
                numberOfPagesInSmallSection = 1024;
            }
            Debug.Assert(numberOfPagesInSmallSection <= tx.DataPager.PageSize - ReservedHeaderSpace);

            var sectionStart = tx.AllocatePage(numberOfPagesInSmallSection + 1);
            tx.BreakLargeAllocationToSeparatePages(sectionStart.PageNumber);

            var sectionHeader = (RawDataSmallSectionPageHeader*)sectionStart.Pointer;
            sectionHeader->RawDataFlags = RawDataPageFlags.Header;
            sectionHeader->Flags = PageFlags.RawData | PageFlags.Single;
            sectionHeader->NumberOfEntries = 0;
            sectionHeader->NumberOfEntriesInSection = 0;
            sectionHeader->NumberOfPages = numberOfPagesInSmallSection;
            sectionHeader->LastUsedPage = 0;

            var availablespace = (ushort*)((byte*)sectionHeader + ReservedHeaderSpace);

            for (int i = 0; i < numberOfPagesInSmallSection; i++)
            {
                var pageHeader = (RawDataSmallPageHeader*)(sectionStart.Pointer + (i + 1) * tx.DataPager.PageSize);
                Debug.Assert(pageHeader->PageNumber == sectionStart.PageNumber + i + 1);
                pageHeader->NumberOfEntries = 0;
                pageHeader->RawDataFlags = RawDataPageFlags.Small;
                pageHeader->Flags = PageFlags.RawData | PageFlags.Single;
                pageHeader->NextAllocation = (ushort)sizeof(RawDataSmallPageHeader);
                availablespace[i] = (ushort)(tx.DataPager.PageSize - sizeof(RawDataSmallPageHeader));
            }

            return new RawDataSmallSection(tx, sectionStart.PageNumber);
        }
    }
}
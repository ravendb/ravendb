using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
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
    /// It will allocate a 128 pages (512Kb in using 4KB pages) and work with them.
    /// 
    /// All attempts are made to reduce the number of times that we need to move data, even
    /// at the cost of fragmentation.  
    /// </summary>
    public unsafe class RawDataSmallSection
    {
        private readonly LowLevelTransaction _tx;
        private readonly long _pageNumber;
        private RawDataSmallSectionPageHeader* _sectionHeader;
        public readonly int MaxItemSize;
        private int _pageSize;
        private HashSet<long> _dirtyPages = new HashSet<long>();

        public event DataMovedDelegate DataMoved;


        private RawDataSmallSection(LowLevelTransaction tx, long pageNumber)
        {
            _tx = tx;
            _pageNumber = pageNumber;
            _pageSize = _tx.DataPager.PageSize;

            MaxItemSize = (_pageSize - sizeof (RawDataSmallPageHeader))/2;

            _sectionHeader = (RawDataSmallSectionPageHeader*) _tx.GetPage(pageNumber).Pointer;
        }

        public bool TryWrite(long id, byte* data, int size)
        {
            var posInPage = (int) (id%_pageSize);
            var pageNumberInSection = (int) (id - _sectionHeader->PageNumber) - posInPage;
            var pageHeader = PageHeaderFor(pageNumberInSection);
            if (posInPage >= pageHeader->NextAllocation)
                throw new InvalidDataException("Asked to load a past the allocated values: " + id + " from page " +
                                               pageHeader->PageNumber);

            var sizes = (short*) ((byte*) pageHeader + posInPage);
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

            if (sizes[1] > size)
            {

            }

            pageHeader = ModifyPage(pageHeader);
            var writePos = ((byte*) pageHeader + posInPage + sizeof (short) /*allocated*/+ sizeof (short) /*used*/);
            // note that we have to do this calc again, pageHeader might have changed
            ((short*) ((byte*) pageHeader + posInPage))[1] = (short) size;
            Memory.Copy(writePos, data, size);
            return true;
        }

        public byte* DirectRead(long id, out int size)
        {
            var posInPage = (int) (id%_pageSize);
            var pageNumberInSection = (int) (id - _sectionHeader->PageNumber) - posInPage;
            var pageHeader = PageHeaderFor(pageNumberInSection);
            if (posInPage >= pageHeader->NextAllocation)
                throw new InvalidDataException("Asked to load a past the allocated values: " + id + " from page " +
                                               pageHeader->PageNumber);

            var sizes = (short*) ((byte*) pageHeader + posInPage);
            if (sizes[1] < 0)
                throw new InvalidDataException("Asked to load a value that was already freed: " + id + " from page " +
                                               pageHeader->PageNumber);

            if (sizes[0] < sizes[1])
                throw new InvalidDataException(
                    "Asked to load a value that where the allocated size is smaller than the used size: " + id +
                    " from page " +
                    pageHeader->PageNumber);

            size = sizes[1];
            return ((byte*) pageHeader + posInPage + sizeof (short) /*allocated*/+ sizeof (short) /*used*/);
        }

        public bool TryAllocate(int size, out long id)
        {
            size += sizeof (short) /*allocated size */+ sizeof (short) /*actual size*/;
                // we need to have the size value here, so we add that

            if (size > MaxItemSize)
                throw new ArgumentException("Cannot allocate an item of " + size +
                                            " bytes in a small data section. Maximum is: " + MaxItemSize);

            for (int i = 0; i < RawDataSmallSectionPageHeader.NumberOfPagesInSmallSection; i++)
            {
                var pageHeader = PageHeaderFor(i);
                if (_sectionHeader->AvailableSpace[i] < size ||
                    pageHeader->NextAllocation + size > _pageSize)
                    continue;

                // best case, we have enough space, and we don't need to defrag
                pageHeader = ModifyPage(pageHeader);
                id = (pageHeader->PageNumber)*_pageSize + pageHeader->NextAllocation;
                pageHeader->NextAllocation += (ushort) size;
                pageHeader->NumberOfEntries++;
                ModifyHeader(_sectionHeader)->NumberOfEntriesInSection++;
                return true;
            }

            for (int i = 0; i < RawDataSmallSectionPageHeader.NumberOfPagesInSmallSection; i++)
            {
                if (_sectionHeader->AvailableSpace[i] < size)
                    continue;
                // we have space, but we need to defrag
                var pageHeader = PageHeaderFor(i);
                pageHeader = DefragPage(pageHeader);
                id = (_sectionHeader->PageNumber)*_pageSize + pageHeader->NextAllocation;
                pageHeader->NextAllocation += (ushort) size;
                pageHeader->NumberOfEntries++;
                ModifyHeader(_sectionHeader)->NumberOfEntriesInSection++;
                return true;
            }

            // we don't have space, caller need to allocate new small section?
            id = -1;
            return false;
        }

        private unsafe RawDataSmallPageHeader* DefragPage(RawDataSmallPageHeader* pageHeader)
        {
            pageHeader = ModifyPage(pageHeader);

            TemporaryPage tmp;
            using (_tx.Environment.GetTemporaryPage(_tx, out tmp))
            {
                var maxUsedPos = pageHeader->NextAllocation;
                Memory.Copy(tmp.TempPagePointer, (byte*) pageHeader, _pageSize);
                pageHeader->NextAllocation = (ushort) (
                    _sectionHeader->PageNumber == pageHeader->PageNumber
                        ? sizeof (RawDataSmallSectionPageHeader)
                        : sizeof (RawDataSmallPageHeader)
                    );
                Memory.Set((byte*) pageHeader + pageHeader->NextAllocation, 0,
                    _pageSize - pageHeader->NextAllocation);

                pageHeader->NumberOfEntries = 0;
                var pos = pageHeader->NextAllocation;
                while (pos < maxUsedPos)
                {
                    var sizes = ((short*) (tmp.TempPagePointer + pos));
                    var allocatedSize = sizes[0];
                    if (allocatedSize < 0)
                        throw new InvalidDataException("Allocated size cannot be negative, but was " + allocatedSize +
                                                       " in page " + pageHeader->PageNumber);
                    var usedSize = sizes[1]; // used size
                    if (usedSize < 0)
                    {
                        pos += (ushort) allocatedSize;
                        continue; // this was freed
                    }

                    if (DataMoved != null)
                    {
                        var prevId = (pageHeader->PageNumber)*_pageSize + pos;
                        var newId = (pageHeader->PageNumber)*_pageSize + pageHeader->NextAllocation;
                        if (prevId != newId)
                        {
                            DataMoved(prevId, newId, tmp.TempPagePointer + pos);
                        }
                    }

                    sizes = (short*) (((byte*) pageHeader) + pageHeader->NextAllocation);
                    sizes[0] = usedSize; // allocated
                    sizes[1] = usedSize; // used
                    pageHeader->NextAllocation += sizeof (short) + sizeof (short);

                    Memory.Copy(((byte*) pageHeader) + pageHeader->NextAllocation, tmp.TempPagePointer + pos,
                        usedSize);

                    pageHeader->NextAllocation += (ushort) usedSize;
                    pos += (ushort) allocatedSize;
                }
            }
            return pageHeader;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private RawDataSmallSectionPageHeader* ModifyHeader(RawDataSmallSectionPageHeader* sectionHeader)
        {
            if (_dirtyPages.Add(sectionHeader->PageNumber) == false)
                return sectionHeader;
            var page = _tx.ModifyPage(sectionHeader->PageNumber);
            return (RawDataSmallSectionPageHeader*) page.Pointer;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private RawDataSmallPageHeader* ModifyPage(RawDataSmallPageHeader* pageHeader)
        {
            if (_dirtyPages.Add(pageHeader->PageNumber) == false)
                return pageHeader;
            var page = _tx.ModifyPage(pageHeader->PageNumber);
            return (RawDataSmallPageHeader*) page.Pointer;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private unsafe RawDataSmallPageHeader* PageHeaderFor(int i)
        {
            return (RawDataSmallPageHeader*) (_tx.GetPage(_sectionHeader->PageNumber + i).Pointer);
        }

        public static RawDataSmallSection Create(LowLevelTransaction tx)
        {
            var sectionStart = tx.AllocatePage(RawDataSmallSectionPageHeader.NumberOfPagesInSmallSection);
            var sectionHeader = (RawDataSmallSectionPageHeader*) sectionStart.Pointer;
            sectionHeader->Flags = PageFlags.RawData | PageFlags.Single;
            sectionHeader->NumberOfEntries = 0;
            sectionHeader->NumberOfEntriesInSection = 0;
            sectionHeader->RawDataFlags = RawDataPageFlags.Header;
            sectionHeader->AvailableSpace[0] = (ushort) (tx.DataPager.PageSize - sizeof (RawDataSmallSectionPageHeader));
            sectionHeader->NextAllocation = (ushort) (sizeof (RawDataSmallSectionPageHeader));

            for (int i = 1; i < RawDataSmallSectionPageHeader.NumberOfPagesInSmallSection; i++)
            {
                var pageHeader = (RawDataSmallPageHeader*) (sectionStart.Pointer + i*tx.DataPager.PageSize);
                pageHeader->PageNumber = sectionStart.PageNumber + i;
                pageHeader->NumberOfEntries = 0;
                pageHeader->RawDataFlags = RawDataPageFlags.Small;
                pageHeader->Flags = PageFlags.RawData | PageFlags.Single;
                pageHeader->NextAllocation = (ushort) sizeof (RawDataSmallPageHeader);
                sectionHeader->AvailableSpace[i] = (ushort) (tx.DataPager.PageSize - sizeof (RawDataSmallPageHeader));
            }

            return new RawDataSmallSection(tx, sectionStart.PageNumber);
        }
    }
}
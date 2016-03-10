using System.Collections.Generic;
using System.IO;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Sparrow;
using Voron.Impl;

namespace Voron.Data.RawData
{
    public unsafe class RawDataSection
    {
        protected const ushort ReservedHeaderSpace = 96;
        private readonly HashSet<long> _dirtyPages = new HashSet<long>();
        protected readonly int _pageSize;
        protected readonly LowLevelTransaction _tx;
        public readonly int MaxItemSize;
        protected RawDataSmallSectionPageHeader* _sectionHeader;

        [StructLayout(LayoutKind.Sequential)]
        protected struct RawDataEntrySizes
        {
            public short AllocatedSize;
            public short UsedSize;
        }

        public RawDataSection(LowLevelTransaction tx, long pageNumber)
        {
            PageNumber = pageNumber;
            _tx = tx;
            _pageSize = _tx.DataPager.PageSize;

            MaxItemSize = (_pageSize - sizeof(RawDataSmallPageHeader)) / 2;

            _sectionHeader = (RawDataSmallSectionPageHeader*)_tx.GetPage(pageNumber).Pointer;
        }

        public long PageNumber { get; }


        public int AllocatedSize => _sectionHeader->AllocatedSize;

        public int Size => _sectionHeader->NumberOfPages * _pageSize;


        public ushort* AvailableSpace
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get
            {
                return (ushort*)((byte*)_sectionHeader + ReservedHeaderSpace);
            }
        }

        public List<long> GetAllIdsInSectionContaining(long id)
        {
            if (Contains(id) == false)
            {
                var posInPage = (int)(id % _pageSize);
                var pageNumberInSection = (id - posInPage) / _pageSize;
                var pageHeaderForId = PageHeaderFor(pageNumberInSection);

                // this is in another section, cannot free it directly, so we'll forward to the right section
                var sectionPageNumber = pageHeaderForId->PageNumber - pageHeaderForId->PageNumberInSection;
                return new RawDataSection(_tx, sectionPageNumber).GetAllIdsInSectionContaining(id);
            }

            var ids = new List<long>(_sectionHeader->NumberOfEntries);
            for (int i = 0; i < _sectionHeader->NumberOfPages; i++)
            {
                var pageHeader = PageHeaderFor(_sectionHeader->PageNumber + i + 1);
                var offset = sizeof(RawDataSmallPageHeader);
                while (offset < _pageSize)
                {
                    var sizes = (short*)((byte*)pageHeader + offset);
                    if (sizes[1] != -1)
                    {
                        ids.Add((pageHeader->PageNumber * _pageSize) + offset);
                    }
                    offset += sizeof(short) * 2 + sizes[0];
                }
            }
            return ids;
        }

        public int NumberOfEntries => _sectionHeader->NumberOfEntries;

        public int OverheadSize
            => _pageSize /* header page*/+
               _sectionHeader->NumberOfEntries * (sizeof(ushort) * 2) /*per entry*/+
               _sectionHeader->NumberOfPages * sizeof(RawDataSmallPageHeader);

        public double Density
        {
            get
            {
                var total = 0;
                for (var i = 0; i < _sectionHeader->NumberOfPages; i++)
                {
                    total += AvailableSpace[i];
                }
                return 1 - (total / (double)(_sectionHeader->NumberOfPages * _pageSize));
            }
        }

        public bool Contains(long id)
        {
            var posInPage = (int)(id % _pageSize);
            var pageNumberInSection = (id - posInPage) / _pageSize;

            return (pageNumberInSection > _sectionHeader->PageNumber &&
                    pageNumberInSection <= _sectionHeader->PageNumber + _sectionHeader->NumberOfPages);
        }

        public bool TryWrite(long id, byte* data, int size)
        {
            byte* writePos;
            if (!TryWriteDirect(id, size, out writePos))
                return false;
            Memory.Copy(writePos, data, size);
            return true;
        }

        public bool TryWriteDirect(long id, int size, out byte* writePos)
        {
            var posInPage = (int)(id % _pageSize);
            var pageNumberInSection = (id - posInPage) / _pageSize;
            var pageHeader = PageHeaderFor(pageNumberInSection);

            if (posInPage >= pageHeader->NextAllocation)
                throw new InvalidDataException($"Asked to load a past the allocated values: {id} from page {pageHeader->PageNumber}");

            var sizes = (RawDataEntrySizes*)((byte*)pageHeader + posInPage);
            if (sizes->UsedSize < 0)
                throw new InvalidDataException($"Asked to load a value that was already freed: {id} from page {pageHeader->PageNumber}");

            if (sizes->AllocatedSize < sizes->UsedSize)
                throw new InvalidDataException(
                    "Asked to load a value that where the allocated size is smaller than the used size: " + id +
                    " from page " +
                    pageHeader->PageNumber);

            if (sizes->AllocatedSize < size)
            {
                writePos = (byte*)0;
                return false; // can't write here
            }


            pageHeader = ModifyPage(pageHeader);
            writePos = ((byte*)pageHeader + posInPage + sizeof(short) /*allocated*/+ sizeof(short) /*used*/);
            // note that we have to do this calc again, pageHeader might have changed
            ((short*)((byte*)pageHeader + posInPage))[1] = (short)size;
            return true;
        }

        public byte* DirectRead(long id, out int size)
        {
            return DirectRead(_tx, id, out size);
        }

        public static byte* DirectRead(LowLevelTransaction tx, long id, out int size)
        {
            var posInPage = (int)(id % tx.DataPager.PageSize);
            var pageNumberInSection = (id - posInPage) / tx.DataPager.PageSize;
            var pageHeader = PageHeaderFor(tx, pageNumberInSection);

            if (posInPage >= pageHeader->NextAllocation)
                throw new InvalidDataException(
                    $"Asked to load a past the allocated values: {id} from page {pageHeader->PageNumber}");

            var sizes = (short*)((byte*)pageHeader + posInPage);
            if (sizes[1] < 0)
                throw new InvalidDataException(
                    $"Asked to load a value that was already freed: {id} from page {pageHeader->PageNumber}");

            if (sizes[0] < sizes[1])
                throw new InvalidDataException(
                    $"Asked to load a value that where the allocated size is smaller than the used size: {id} from page {pageHeader->PageNumber}");

            size = sizes[1];
            return ((byte*)pageHeader + posInPage + sizeof(short) /*allocated*/+ sizeof(short) /*used*/);
        }

        public long GetSectionPageNumber(long id)
        {
            var posInPage = (int)(id % _pageSize);
            var pageNumberInSection = (id - posInPage) / _pageSize;
            var pageHeader = PageHeaderFor(pageNumberInSection);
            var sectionPageNumber = pageHeader->PageNumber - pageHeader->PageNumberInSection;
            return sectionPageNumber;
        }


        public void DeleteSection(long sectionPageNumber)
        {
            if (sectionPageNumber != _sectionHeader->PageNumber)
            {
                // this is in another section, cannot delete it directly, so we'll forward to the right section
                new RawDataSection(_tx, sectionPageNumber).DeleteSection(sectionPageNumber);
                return;
            }

            for (int i = 0; i < _sectionHeader->NumberOfPages; i++)
            {
                _tx.FreePage(_sectionHeader->PageNumber + i + 1);
            }
            _tx.FreePage(_sectionHeader->PageNumber);
        }

        public double Free(long id)
        {
            var posInPage = (int)(id % _pageSize);
            var pageNumberInSection = (id - posInPage) / _pageSize;
            var pageHeader = PageHeaderFor(pageNumberInSection);

            if (Contains(id) == false)
            {
                // this is in another section, cannot free it directly, so we'll forward to the right section
                var sectionPageNumber = pageHeader->PageNumber - pageHeader->PageNumberInSection - 1;
                var actualSection = new RawDataSection(_tx, sectionPageNumber);
                if(actualSection.Contains(id)==false)
                    throw new InvalidDataException($"Cannot delete {id} because the raw data section starting in {sectionPageNumber} with size {actualSection.AllocatedSize} doesn't own it. Possible data corruption?");
                return actualSection.Free(id);
            }

            pageHeader = ModifyPage(pageHeader);
            if (posInPage >= pageHeader->NextAllocation)
                throw new InvalidDataException($"Asked to load a past the allocated values: {id} from page {pageHeader->PageNumber}");

            var sizes = (short*)((byte*)pageHeader + posInPage);
            if (sizes[1] < 0)
                throw new InvalidDataException($"Asked to free a value that was already freed: {id} from page {pageHeader->PageNumber}");

            sizes[1] = -1;
            pageHeader->NumberOfEntries--;

            EnsureHeaderModified();
            _sectionHeader->NumberOfEntries--;
            var sizeFreed = sizes[0] + (sizeof(short) * 2);
            _sectionHeader->AllocatedSize -= sizeFreed;
            AvailableSpace[pageHeader->PageNumberInSection] += (ushort)sizeFreed;

            return Density;
        }

        public event DataMovedDelegate DataMoved;

        public override string ToString()
        {
            return $"PageNumber: {PageNumber}; " +
                   $"AllocatedSize: {AllocatedSize:#,#;;0}; " +
                   $"Size: {Size:#,#;;0}; " +
                   $"Entries: {NumberOfEntries:#,#;;0}; " +
                   $"Overhead: {OverheadSize:#,#;;0}; " +
                   $"Density: {Density:P}";
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        protected void EnsureHeaderModified()
        {
            if (_dirtyPages.Add(_sectionHeader->PageNumber) == false)
                return;
            var page = _tx.ModifyPage(_sectionHeader->PageNumber);
            _sectionHeader = (RawDataSmallSectionPageHeader*)page.Pointer;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        protected RawDataSmallPageHeader* ModifyPage(RawDataSmallPageHeader* pageHeader)
        {
            if (_dirtyPages.Add(pageHeader->PageNumber) == false)
                return pageHeader;
            var page = _tx.ModifyPage(pageHeader->PageNumber);
            return (RawDataSmallPageHeader*)page.Pointer;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        protected RawDataSmallPageHeader* PageHeaderFor(long pageNumber)
        {
            return PageHeaderFor(_tx, pageNumber);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        protected static RawDataSmallPageHeader* PageHeaderFor(LowLevelTransaction tx, long pageNumber)
        {
            return (RawDataSmallPageHeader*)(tx.GetPage(pageNumber).Pointer);
        }

        protected virtual void OnDataMoved(long previousid, long newid, byte* data, int size)
        {
            DataMoved?.Invoke(previousid, newid, data, size);
        }
    }
}
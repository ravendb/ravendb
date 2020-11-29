using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Threading;
using Sparrow;
using Voron.Data.Tables;
using Voron.Exceptions;
using Voron.Global;
using Voron.Impl;

namespace Voron.Data.RawData
{
    public unsafe class RawDataSection : IDisposable
    {
        protected const ushort ReservedHeaderSpace = 96;


        protected readonly LowLevelTransaction _llt;

        public const int MaxItemSize = (Constants.Storage.PageSize - RawDataSmallPageHeader.SizeOf) / 2;

        protected RawDataSmallSectionPageHeader* _sectionHeader;

        [StructLayout(LayoutKind.Sequential)]
        public struct RawDataEntrySizes
        {
            public short AllocatedSize;
            public short UsedSize_Buffer;

            public const short CompressedFlagMask = unchecked((short)(1 << 15));
            public const short ValueOnlyMask = ~CompressedFlagMask;

            public void SetFreed()
            {
                UsedSize_Buffer = -1;
            }

            public bool IsFreed => UsedSize_Buffer == -1;

            public bool IsCompressed
            {
                get => (UsedSize_Buffer & CompressedFlagMask) != 0;
                set
                {
                    if (value)
                        UsedSize_Buffer |= CompressedFlagMask;
                    else
                        UsedSize_Buffer &= ~CompressedFlagMask;
                }
            }

            public short UsedSize
            {
                get
                {
                    Debug.Assert(IsFreed == false, "Should not try to get size of freed instance");
                    return (short)(UsedSize_Buffer & ValueOnlyMask);
                }
                set => UsedSize_Buffer = (short)(value & ValueOnlyMask);
            }
        }

        public RawDataSection(LowLevelTransaction tx, long pageNumber)
        {
            PageNumber = pageNumber;
            _llt = tx;

            _sectionHeader = (RawDataSmallSectionPageHeader*)_llt.GetPage(pageNumber).Pointer;
        }

        public long PageNumber { get; }


        public int AllocatedSize => _sectionHeader->AllocatedSize;

        public ulong SectionOwnerHash => _sectionHeader->SectionOwnerHash;

        public int Size => _sectionHeader->NumberOfPages * Constants.Storage.PageSize;

        public int NumberOfPages => _sectionHeader->NumberOfPages;

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
                var posInPage = (int)(id % Constants.Storage.PageSize);
                var pageNumberInSection = (id - posInPage) / Constants.Storage.PageSize;
                var pageHeaderForId = PageHeaderFor(_llt, pageNumberInSection);

                // this is in another section, cannot free it directly, so we'll forward to the right section
                var sectionPageNumber = pageHeaderForId->PageNumber - pageHeaderForId->PageNumberInSection - 1;
                var actualSection = new RawDataSection(_llt, sectionPageNumber);
                if (actualSection._sectionHeader->SectionOwnerHash != _sectionHeader->SectionOwnerHash)
                {
                    VoronUnrecoverableErrorException.Raise(_llt,
                        $"Cannot get all ids in section containing {id} because the raw data section starting in {sectionPageNumber} belongs to a different owner");
                }
                return actualSection.GetAllIdsInSectionContaining(id);
            }

            return GetAllIdsInSection();
        }

        public List<long> GetAllIdsInSection()
        {
            var ids = new List<long>(_sectionHeader->NumberOfEntries);
            for (int i = 0; i < _sectionHeader->NumberOfPages && ids.Count < _sectionHeader->NumberOfEntries; i++)
            {
                FillAllIdsInPage(_sectionHeader->PageNumber + i, ids);
            }

            return ids;
        }

        public List<long> GetAllIdsInSamePageAs(long id)
        {
            var ids = new List<long>();
            FillAllIdsInPage((id / Constants.Storage.PageSize) - 1, ids);
            return ids;
        }

        public void FillAllIdsInPage(long pageNumber, List<long> ids)
        {
            var pageHeader = PageHeaderFor(_llt, pageNumber + 1);
            var offset = sizeof(RawDataSmallPageHeader);
            while (offset + sizeof(RawDataEntrySizes) < Constants.Storage.PageSize)
            {
                var sizes = (RawDataEntrySizes*)((byte*)pageHeader + offset);
                if (sizes->IsFreed == false)
                {
                    var currentId = (pageHeader->PageNumber * Constants.Storage.PageSize) + offset;

                    var posInPage = (int)(currentId % Constants.Storage.PageSize);

                    if (posInPage >= pageHeader->NextAllocation)
                        break;

                    ids.Add(currentId);

                    if (ids.Count == _sectionHeader->NumberOfEntries)
                        break;
                }
                offset += sizeof(short) * 2 + sizes->AllocatedSize;
            }
        }

        public int NumberOfEntries => _sectionHeader->NumberOfEntries;

        public int OverheadSize
            => Constants.Storage.PageSize /* header page*/+
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
                return 1 - (total / (double)(_sectionHeader->NumberOfPages * Constants.Storage.PageSize));
            }
        }

        public bool Contains(long id)
        {
            var posInPage = (int)(id % Constants.Storage.PageSize);
            var pageNumberInSection = (id - posInPage) / Constants.Storage.PageSize;

            return (pageNumberInSection > _sectionHeader->PageNumber &&
                    pageNumberInSection <= _sectionHeader->PageNumber + _sectionHeader->NumberOfPages);
        }

        public bool TryWrite(long id, byte* data, int size, bool compressed)
        {
            if (!TryWriteDirect(id, size, compressed, out var writePos))
                return false;
            Memory.Copy(writePos, data, size);
            return true;
        }

        public bool TryWriteDirect(long id, int size, bool compressed, out byte* writePos)
        {
            if (_llt.Flags == TransactionFlags.Read)
                ThrowReadOnlyTransaction(id);

            var posInPage = (int)(id % Constants.Storage.PageSize);
            var pageNumberInSection = (id - posInPage) / Constants.Storage.PageSize;
            var pageHeader = PageHeaderFor(_llt, pageNumberInSection);

            if (posInPage >= pageHeader->NextAllocation)
                VoronUnrecoverableErrorException.Raise(_llt, $"Asked to load a past the allocated values: {id} from page {pageHeader->PageNumber}");

            var sizes = (RawDataEntrySizes*)((byte*)pageHeader + posInPage);
            if (sizes->IsFreed)
                VoronUnrecoverableErrorException.Raise(_llt, $"Asked to load a value that was already freed: {id} from page {pageHeader->PageNumber}");

            if (sizes->AllocatedSize < sizes->UsedSize)
                VoronUnrecoverableErrorException.Raise(_llt,
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
            var entry = ((RawDataEntrySizes*)((byte*)pageHeader + posInPage));
            entry->UsedSize_Buffer = (short)size;
            entry->IsCompressed = compressed;

            return true;
        }

        public byte* DirectRead(long id, out int size, out bool compressed)
        {
            return DirectRead(_llt, id, out size, out compressed);
        }

        public static byte* DirectRead(LowLevelTransaction tx, long id, out int size, out bool compressed)
        {
            RawDataEntrySizes* sizes = GetRawDataEntrySizeFor(tx, id);

            size = sizes->UsedSize;
            compressed = sizes->IsCompressed;
            return (byte*)sizes + sizeof(RawDataEntrySizes);
        }

        public static RawDataEntrySizes* GetRawDataEntrySizeFor(LowLevelTransaction tx, long id)
        {
            var posInPage = (int)(id % Constants.Storage.PageSize);
            var pageNumberInSection = (id - posInPage) / Constants.Storage.PageSize;
            var pageHeader = PageHeaderFor(tx, pageNumberInSection);

            if (posInPage >= pageHeader->NextAllocation)
            {
                if (posInPage == 0)
                    VoronUnrecoverableErrorException.Raise(tx,
                        $"Asked to load a large value from a raw data section page {pageHeader->PageNumber}, this is a bug");

                VoronUnrecoverableErrorException.Raise(tx,
                    $"Asked to load a past the allocated values: {id} from page {pageHeader->PageNumber}");
            }

            var sizes = (RawDataEntrySizes*)((byte*)pageHeader + posInPage);
            if (sizes->IsFreed)
                VoronUnrecoverableErrorException.Raise(tx,
                    $"Asked to load a value that was already freed: {id} from page {pageHeader->PageNumber}");

            if (sizes->AllocatedSize < sizes->UsedSize)
                VoronUnrecoverableErrorException.Raise(tx,
                    $"Asked to load a value that where the allocated size is smaller than the used size: {id} from page {pageHeader->PageNumber}");

            return sizes;
        }

        public static long GetSectionPageNumber(LowLevelTransaction tx, long id)
        {
            var posInPage = (int)(id % Constants.Storage.PageSize);
            var pageNumberInSection = (id - posInPage) / Constants.Storage.PageSize;
            var pageHeader = PageHeaderFor(tx, pageNumberInSection);
            var sectionPageNumber = pageHeader->PageNumber - pageHeader->PageNumberInSection - 1;
            return sectionPageNumber;
        }


        public void DeleteSection(long sectionPageNumber)
        {
            if (_llt.Flags == TransactionFlags.Read)
                ThrowReadOnlyTransaction(sectionPageNumber);

            if (sectionPageNumber != _sectionHeader->PageNumber)
            {
                // this is in another section, cannot delete it directly, so we'll forward to the right section
                var actualSection = new RawDataSection(_llt, sectionPageNumber);
                if (actualSection._sectionHeader->SectionOwnerHash != _sectionHeader->SectionOwnerHash)
                {
                    VoronUnrecoverableErrorException.Raise(_llt,
                        $"Cannot delete section because the raw data section starting in {sectionPageNumber} belongs to a different owner");
                }
                actualSection.DeleteSection(sectionPageNumber);
                return;
            }

            for (int i = 0; i < _sectionHeader->NumberOfPages; i++)
            {
                _llt.FreePage(_sectionHeader->PageNumber + i + 1);
            }
            _llt.FreePage(_sectionHeader->PageNumber);
        }

        public double Free(long id)
        {
            if (_llt.Flags == TransactionFlags.Read)
                ThrowReadOnlyTransaction(id);

            var posInPage = (int)(id % Constants.Storage.PageSize);
            var pageNumberInSection = (id - posInPage) / Constants.Storage.PageSize;
            var pageHeader = PageHeaderFor(_llt, pageNumberInSection);

            if (Contains(id) == false)
            {
                // this is in another section, cannot free it directly, so we'll forward to the right section
                var sectionPageNumber = pageHeader->PageNumber - pageHeader->PageNumberInSection - 1;
                var actualSection = new RawDataSection(_llt, sectionPageNumber);
                if (actualSection.Contains(id) == false)
                    VoronUnrecoverableErrorException.Raise(_llt, $"Cannot delete {id} because the raw data section starting in {sectionPageNumber} with size {actualSection.AllocatedSize} doesn't own it. Possible data corruption?");

                if (actualSection._sectionHeader->SectionOwnerHash != _sectionHeader->SectionOwnerHash)
                {
                    VoronUnrecoverableErrorException.Raise(_llt,
                        $"Cannot delete {id} because the raw data section starting in {sectionPageNumber} belongs to a different owner");
                }

                return actualSection.Free(id);
            }

            pageHeader = ModifyPage(pageHeader);
            if (posInPage >= pageHeader->NextAllocation)
                VoronUnrecoverableErrorException.Raise(_llt, $"Asked to load a past the allocated values: {id} from page {pageHeader->PageNumber}");

            var sizes = (RawDataEntrySizes*)((byte*)pageHeader + posInPage);
            if (sizes->IsFreed)
                VoronUnrecoverableErrorException.Raise(_llt, $"Asked to free a value that was already freed: {id} from page {pageHeader->PageNumber}");

            sizes->SetFreed();
            Memory.Set((byte*)pageHeader + posInPage + sizeof(RawDataEntrySizes), 0, sizes->AllocatedSize);
            pageHeader->NumberOfEntries--;

            EnsureHeaderModified();
            _sectionHeader->NumberOfEntries--;
            var sizeFreed = sizes->AllocatedSize + (sizeof(short) * 2);
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

        public void Dispose()
        {
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        protected void EnsureHeaderModified()
        {
            var page = _llt.ModifyPage(_sectionHeader->PageNumber);
            _sectionHeader = (RawDataSmallSectionPageHeader*)page.Pointer;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        protected RawDataSmallPageHeader* ModifyPage(RawDataSmallPageHeader* pageHeader)
        {
            var page = _llt.ModifyPage(pageHeader->PageNumber);
            return (RawDataSmallPageHeader*)page.Pointer;
        }

        private static void ThrowReadOnlyTransaction(long id)
        {
            throw new InvalidOperationException($"Attempted to modify page {id} in a read only transaction");
        }

        protected static void ThrowInvalidPage(long id)
        {
            throw new InvalidOperationException($"Page {id} is not a raw data section page");
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        protected static RawDataSmallPageHeader* PageHeaderFor(LowLevelTransaction tx, long pageNumber)
        {
            var pageHeader = (RawDataSmallPageHeader*)(tx.GetPage(pageNumber).Pointer);
            if ((pageHeader->Flags & PageFlags.RawData) != PageFlags.RawData)
                ThrowInvalidPage(pageNumber);
            return pageHeader;
        }

        protected virtual void OnDataMoved(long previousId, long newid, byte* data, int size, bool compressed)
        {
            var onDataMoved = DataMoved;
            if (onDataMoved == null)
                throw new InvalidOperationException("Trying to move data, but no one is listening to the move!");
            onDataMoved(previousId, newid, data, size, compressed);
        }

        internal void FreeRawDataSectionPages()
        {
            var rawDataSmallPageHeader = PageHeaderFor(_llt, PageNumber);
            var rawDataSectionPageHeader = (RawDataSmallSectionPageHeader*)rawDataSmallPageHeader;

            for (var i = 0; i < rawDataSectionPageHeader->NumberOfPages; i++)
            {
                _llt.FreePage(rawDataSmallPageHeader->PageNumber + i);
            }
        }
    }
}

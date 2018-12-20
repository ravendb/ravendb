using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Sparrow;
using Voron.Exceptions;
using Voron.Global;
using Voron.Impl;

namespace Voron.Data.RawData
{
    public unsafe class RawDataSection : IDisposable
    {
        protected const ushort ReservedHeaderSpace = 96;


        protected readonly LowLevelTransaction _tx;

        public const int MaxItemSize = (Constants.Storage.PageSize - RawDataSmallPageHeader.SizeOf) / 2;

        protected RawDataSmallSectionPageHeader* _sectionHeader;

        [StructLayout(LayoutKind.Sequential)]
        public struct RawDataEntrySizes
        {
            public short AllocatedSize;
            public short UsedSize;
        }

        public RawDataSection(LowLevelTransaction tx, long pageNumber)
        {
            PageNumber = pageNumber;
            _tx = tx;


            _sectionHeader = (RawDataSmallSectionPageHeader*)_tx.GetPage(pageNumber).Pointer;
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
                var pageHeaderForId = PageHeaderFor(pageNumberInSection);

                // this is in another section, cannot free it directly, so we'll forward to the right section
                var sectionPageNumber = pageHeaderForId->PageNumber - pageHeaderForId->PageNumberInSection - 1;
                var actualSection = new RawDataSection(_tx, sectionPageNumber);
                if (actualSection._sectionHeader->SectionOwnerHash != _sectionHeader->SectionOwnerHash)
                {
                    VoronUnrecoverableErrorException.Raise(_tx,
                        $"Cannot get all ids in section containing {id} because the raw data section starting in {sectionPageNumber} belongs to a different owner");
                }
                return actualSection.GetAllIdsInSectionContaining(id);
            }

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
            var pageHeader = PageHeaderFor(pageNumber + 1);
            var offset = sizeof(RawDataSmallPageHeader);
            while (offset + sizeof(RawDataEntrySizes) < Constants.Storage.PageSize)
            {
                var sizes = (RawDataEntrySizes*)((byte*)pageHeader + offset);
                if (sizes->UsedSize != -1)
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
            if (_tx.Flags == TransactionFlags.Read)
                ThrowReadOnlyTransaction(id);

            var posInPage = (int)(id % Constants.Storage.PageSize);
            var pageNumberInSection = (id - posInPage) / Constants.Storage.PageSize;
            var pageHeader = PageHeaderFor(pageNumberInSection);

            if (posInPage >= pageHeader->NextAllocation)
                VoronUnrecoverableErrorException.Raise(_tx, $"Asked to load a past the allocated values: {id} from page {pageHeader->PageNumber}");

            var sizes = (RawDataEntrySizes*)((byte*)pageHeader + posInPage);
            if (sizes->UsedSize < 0)
                VoronUnrecoverableErrorException.Raise(_tx, $"Asked to load a value that was already freed: {id} from page {pageHeader->PageNumber}");

            if (sizes->AllocatedSize < sizes->UsedSize)
                VoronUnrecoverableErrorException.Raise(_tx,
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
            ((RawDataEntrySizes*)((byte*)pageHeader + posInPage))->UsedSize = (short)size;

            return true;
        }

        public byte* DirectRead(long id, out int size)
        {
            return DirectRead(_tx, id, out size);
        }

        public static byte* DirectRead(LowLevelTransaction tx, long id, out int size)
        {
            RawDataEntrySizes* sizes = GetRawDataEntrySizeFor(tx, id);

            size = sizes->UsedSize;
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
            if (sizes->UsedSize < 0)
                VoronUnrecoverableErrorException.Raise(tx,
                    $"Asked to load a value that was already freed: {id} from page {pageHeader->PageNumber}");

            if (sizes->AllocatedSize < sizes->UsedSize)
                VoronUnrecoverableErrorException.Raise(tx,
                    $"Asked to load a value that where the allocated size is smaller than the used size: {id} from page {pageHeader->PageNumber}");

            return sizes;
        }

        public long GetSectionPageNumber(long id)
        {
            var posInPage = (int)(id % Constants.Storage.PageSize);
            var pageNumberInSection = (id - posInPage) / Constants.Storage.PageSize;
            var pageHeader = PageHeaderFor(pageNumberInSection);
            var sectionPageNumber = pageHeader->PageNumber - pageHeader->PageNumberInSection - 1;
            return sectionPageNumber;
        }


        public void DeleteSection(long sectionPageNumber)
        {
            if (_tx.Flags == TransactionFlags.Read)
                ThrowReadOnlyTransaction(sectionPageNumber);

            if (sectionPageNumber != _sectionHeader->PageNumber)
            {
                // this is in another section, cannot delete it directly, so we'll forward to the right section
                var actualSection = new RawDataSection(_tx, sectionPageNumber);
                if (actualSection._sectionHeader->SectionOwnerHash != _sectionHeader->SectionOwnerHash)
                {
                    VoronUnrecoverableErrorException.Raise(_tx,
                        $"Cannot delete section because the raw data section starting in {sectionPageNumber} belongs to a different owner");
                }
                actualSection.DeleteSection(sectionPageNumber);
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
            if (_tx.Flags == TransactionFlags.Read)
                ThrowReadOnlyTransaction(id);

            var posInPage = (int)(id % Constants.Storage.PageSize);
            var pageNumberInSection = (id - posInPage) / Constants.Storage.PageSize;
            var pageHeader = PageHeaderFor(pageNumberInSection);

            if (Contains(id) == false)
            {
                // this is in another section, cannot free it directly, so we'll forward to the right section
                var sectionPageNumber = pageHeader->PageNumber - pageHeader->PageNumberInSection - 1;
                var actualSection = new RawDataSection(_tx, sectionPageNumber);
                if (actualSection.Contains(id) == false)
                    VoronUnrecoverableErrorException.Raise(_tx, $"Cannot delete {id} because the raw data section starting in {sectionPageNumber} with size {actualSection.AllocatedSize} doesn't own it. Possible data corruption?");

                if (actualSection._sectionHeader->SectionOwnerHash != _sectionHeader->SectionOwnerHash)
                {
                    VoronUnrecoverableErrorException.Raise(_tx,
                        $"Cannot delete {id} because the raw data section starting in {sectionPageNumber} belongs to a different owner");
                }

                return actualSection.Free(id);
            }

            pageHeader = ModifyPage(pageHeader);
            if (posInPage >= pageHeader->NextAllocation)
                VoronUnrecoverableErrorException.Raise(_tx, $"Asked to load a past the allocated values: {id} from page {pageHeader->PageNumber}");

            var sizes = (RawDataEntrySizes*)((byte*)pageHeader + posInPage);
            if (sizes->UsedSize < 0)
                VoronUnrecoverableErrorException.Raise(_tx, $"Asked to free a value that was already freed: {id} from page {pageHeader->PageNumber}");

            sizes->UsedSize = -1;
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
            var page = _tx.ModifyPage(_sectionHeader->PageNumber);
            _sectionHeader = (RawDataSmallSectionPageHeader*)page.Pointer;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        protected RawDataSmallPageHeader* ModifyPage(RawDataSmallPageHeader* pageHeader)
        {
            var page = _tx.ModifyPage(pageHeader->PageNumber);
            return (RawDataSmallPageHeader*)page.Pointer;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        protected RawDataSmallPageHeader* PageHeaderFor(long pageNumber)
        {
            var pageHeader = PageHeaderFor(_tx, pageNumber);

            if ((pageHeader->Flags & PageFlags.RawData) != PageFlags.RawData)
                ThrowInvalidPage(pageNumber);
            return pageHeader;
        }

        private static void ThrowReadOnlyTransaction(long id)
        {
            throw new InvalidOperationException($"Attempted to modify page {id} in a read only transaction");
        }

        private static void ThrowInvalidPage(long id)
        {
            throw new InvalidOperationException($"Page {id} is not a raw data section page");
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        protected static RawDataSmallPageHeader* PageHeaderFor(LowLevelTransaction tx, long pageNumber)
        {
            return (RawDataSmallPageHeader*)(tx.GetPage(pageNumber).Pointer);
        }

        protected virtual void OnDataMoved(long previousid, long newid, byte* data, int size)
        {
            var onDataMoved = DataMoved;
            if (onDataMoved == null)
                throw new InvalidOperationException("Trying to move data, but no one is listening to the move!");
            onDataMoved(previousid, newid, data, size);
        }
    }
}

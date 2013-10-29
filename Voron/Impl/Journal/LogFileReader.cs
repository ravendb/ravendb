using System.Collections.Generic;
using System.Collections.Immutable;
using System.IO;
using Voron.Util;

namespace Voron.Impl.Journal
{
    public unsafe class LogFileReader
    {
        private IVirtualPager _pager;
        private long _lastSyncedPage;
        private long _writePage;
        private ImmutableDictionary<long,long> _pageTranslationTable;

        public ImmutableDictionary<long, long> PageTranslationTable
        {
            get { return _pageTranslationTable; }
        }

        public long WritePage
        {
            get { return _writePage; }
        }

        public long LastSyncedPage
        {
            get { return _lastSyncedPage; }
        }

        public LogFileReader(IVirtualPager pager, ImmutableDictionary<long,long> pageTranslationTable)
        {
            _pager = pager;
            _pageTranslationTable = pageTranslationTable;
        }

        public TransactionHeader* RecoverAndValidate(long startReadingPage, TransactionHeader* previous)
        {
            TransactionHeader* lastReadHeader = previous;

            var readPosition = startReadingPage;

            var transactionPageTranslation = new Dictionary<long, long>();

            while (readPosition < _pager.NumberOfAllocatedPages)
            {
                var current = (TransactionHeader*)_pager.Read(readPosition).Base;

                if (current->HeaderMarker != Constants.TransactionHeaderMarker)
                    break;

                ValidateHeader(current, lastReadHeader);

                if (current->TxMarker.HasFlag(TransactionMarker.Commit) == false)
                {
                    readPosition += current->PageCount + current->OverflowPageCount;
                    continue;
                }

                lastReadHeader = current;

                readPosition++;

                uint crc = 0;

                for (var i = 0; i < current->PageCount; i++)
                {
                    var page = _pager.Read(readPosition);

                    transactionPageTranslation[page.PageNumber] = readPosition;

                    if (page.IsOverflow)
                    {
                        var numOfPages = _pager.GetNumberOfOverflowPages(page.OverflowSize);
                        readPosition += numOfPages;
                        crc = Crc.Extend(crc, page.Base, 0, numOfPages * _pager.PageSize);
                    }
                    else
                    {
                        readPosition++;
                        crc = Crc.Extend(crc, page.Base, 0, _pager.PageSize);
                    }

                    _lastSyncedPage = readPosition - 1;
                    _writePage = _lastSyncedPage + 1;
                }

                if (crc != current->Crc)
                {
                    throw new InvalidDataException("Checksum mismatch"); //TODO this is temporary, ini the future this condition will just mean that transaction was not committed
                }

            }
            _pageTranslationTable = _pageTranslationTable.SetItems(transactionPageTranslation);

            return lastReadHeader;
        }


        private void ValidateHeader(TransactionHeader* current, TransactionHeader* previous)
        {
            if (current->TransactionId < 0)
                throw new InvalidDataException("Transaction id cannot be less than 0 (Tx: " + current->TransactionId);
            if (current->TxMarker.HasFlag(TransactionMarker.Start) == false)
                throw new InvalidDataException("Transaction must have Start marker");
            if (current->LastPageNumber < 0)
                throw new InvalidDataException("Last page number after committed transaction must be greater than 0");
            if (current->PageCount > 0 && current->Crc == 0)
                throw new InvalidDataException("Transaction checksum can't be equal to 0");

            if (previous == null)
                return;

            if (previous->TxMarker.HasFlag(TransactionMarker.Split))
            {
                if (current->TxMarker.HasFlag(TransactionMarker.Split) == false)
                    throw new InvalidDataException("Previous transaction have a split marker, so the current one should have it too");

                if (current->TransactionId == previous->TransactionId)
                    throw new InvalidDataException("Split transaction should have the same id in the log. Expected id: " +
                                                   previous->TransactionId + ", got: " + current->TransactionId);
            }
            else
            {
                if (current->TransactionId != 1 && // 1 is a first storage transaction which does not increment transaction counter after commit
                    current->TransactionId - previous->TransactionId != 1)
                    throw new InvalidDataException("Unexpected transaction id. Expected: " + (previous->TransactionId + 1) + ", got:" +
                                                   current->TransactionId);
            }

        }
    }
}
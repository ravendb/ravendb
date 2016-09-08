using Sparrow;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using Sparrow.Compression;
using Sparrow.Utils;
using Voron.Global;
using Voron.Impl.Paging;

namespace Voron.Impl.Journal
{
    public unsafe class JournalReader
    {
        private readonly AbstractPager _journalPager;
        private readonly AbstractPager _dataPager;
        private readonly AbstractPager _recoveryPager;

        private readonly long _lastSyncedTransactionId;
        private long _readingPage;
        private DiffApplier _diffApplier = new DiffApplier();


        public bool RequireHeaderUpdate { get; private set; }

        public long NextWritePage
        {
            get { return _readingPage; }
        }

        public JournalReader(AbstractPager journalPager, AbstractPager dataPager, AbstractPager recoveryPager,
            long lastSyncedTransactionId, TransactionHeader* previous)
        {
            RequireHeaderUpdate = false;
            _journalPager = journalPager;
            _dataPager = dataPager;
            _recoveryPager = recoveryPager;
            _lastSyncedTransactionId = lastSyncedTransactionId;
            _readingPage = 0;
            LastTransactionHeader = previous;
        }

        public TransactionHeader* LastTransactionHeader { get; private set; }

        public long? MaxPageToRead { get; set; }

        public bool ReadOneTransactionToDataFile(StorageEnvironmentOptions options, bool checkCrc = true)
        {
            if (_readingPage >= _journalPager.NumberOfAllocatedPages)
                return false;

            if (MaxPageToRead != null && _readingPage >= MaxPageToRead.Value)
                return false;

            TransactionHeader* current;
            if (!TryReadAndValidateHeader(options, out current))
                return false;


            var transactionSize = GetNumberOfPagesFromSize(options, current->CompressedSize + sizeof(TransactionHeader));

            if (current->TransactionId <= _lastSyncedTransactionId)
            {
                _readingPage += transactionSize;
                LastTransactionHeader = current;
                return true; // skipping
            }

            if (checkCrc && !ValidatePagesHash(options, current))
                return false;

            _readingPage += transactionSize;
            var numberOfPages = _recoveryPager.GetNumberOfOverflowPages(current->UncompressedSize);
            _recoveryPager.EnsureContinuous(0, numberOfPages);
            var outputPage = _recoveryPager.AcquirePagePointer(null, 0);
            UnmanagedMemory.Set(outputPage, 0, numberOfPages * options.PageSize);

            try
            {
                LZ4.Decode64((byte*)current + sizeof(TransactionHeader), current->CompressedSize, outputPage,
                    current->UncompressedSize, true);
            }
            catch (Exception e)
            {
                options.InvokeRecoveryError(this, "Could not de-compress, invalid data", e);
                RequireHeaderUpdate = true;

                return false;
            }

            var pageInfoPtr = (TransactionHeaderPageInfo*)outputPage;
            
            var totalRead = sizeof(TransactionHeaderPageInfo)*current->PageCount;
            for (var i = 0; i < current->PageCount; i++)
            {
                if(totalRead > current->UncompressedSize)
                    throw new InvalidDataException($"Attempted to read position {totalRead} from transaction data while the transaction is size {current->UncompressedSize}");
                Debug.Assert(_journalPager.Disposed == false);
                Debug.Assert(_recoveryPager.Disposed == false);
                _dataPager.EnsureContinuous(pageInfoPtr[i].PageNumber,
                    GetNumberOfPagesFromSize(options, pageInfoPtr[i].Size));
                var pagePtr = _dataPager.AcquirePagePointer(null, pageInfoPtr[i].PageNumber);

                var diffPageNumber = *(long*) (outputPage + totalRead);
                if (pageInfoPtr[i].PageNumber != diffPageNumber)
                    throw new InvalidDataException($"Expected a diff for page {pageInfoPtr[i].PageNumber} but got one for {diffPageNumber}");
                totalRead += sizeof(long);
                if (pageInfoPtr[i].DiffSize == 0)
                {
                    Memory.Copy(pagePtr, outputPage + totalRead, pageInfoPtr[i].Size);
                    totalRead += pageInfoPtr[i].Size;
                }
                else
                {
                    _diffApplier.Destination = pagePtr;
                    _diffApplier.Diff = outputPage + totalRead;
                    _diffApplier.Size = pageInfoPtr[i].Size;
                    _diffApplier.DiffSize = pageInfoPtr[i].DiffSize;
                    _diffApplier.Apply();
                    totalRead += pageInfoPtr[i].DiffSize;
                }
            }

            LastTransactionHeader = current;

            return true;
        }

        internal static int GetNumberOfPagesFromSize(StorageEnvironmentOptions options, int size)
        {
            return (size / options.PageSize) + (size % options.PageSize == 0 ? 0 : 1);
        }

        public void RecoverAndValidate(StorageEnvironmentOptions options)
        {
            while (ReadOneTransactionToDataFile(options))
            {
            }
        }

        public void SetStartPage(long value)
        {
            _readingPage = value;
        }

        private bool TryReadAndValidateHeader(StorageEnvironmentOptions options, out TransactionHeader* current)
        {
            current = (TransactionHeader*)_journalPager.Read(null, _readingPage).Base;

            if (current->HeaderMarker != Constants.TransactionHeaderMarker)
            {
                // not a transaction page, 

                // if the header marker is zero, we are probably in the area at the end of the log file, and have no additional log records
                // to read from it. This can happen if the next transaction was too big to fit in the current log file. We stop reading
                // this log file and move to the next one. 

                RequireHeaderUpdate = current->HeaderMarker != 0;
                if (RequireHeaderUpdate)
                {
                    options.InvokeRecoveryError(this, "Transaction " + current->TransactionId + " header marker was set to garbage value, file is probably corrupted", null);
                }

                return false;
            }

            ValidateHeader(current, LastTransactionHeader);

            if ((current->TxMarker & TransactionMarker.Commit) != TransactionMarker.Commit)
            {
                // uncommitted transaction, probably
                RequireHeaderUpdate = true;
                options.InvokeRecoveryError(this, "Transaction " + current->TransactionId + " was not committed", null);
                return false;
            }

            return true;
        }

        private void ValidateHeader(TransactionHeader* current, TransactionHeader* previous)
        {
            if (current->TransactionId < 0)
                throw new InvalidDataException("Transaction id cannot be less than 0 (llt: " + current->TransactionId +
                                               " )");
            if (current->TxMarker.HasFlag(TransactionMarker.Commit) && current->LastPageNumber < 0)
                throw new InvalidDataException("Last page number after committed transaction must be greater than 0");
            if (current->TxMarker.HasFlag(TransactionMarker.Commit) && current->PageCount > 0 && current->Hash == 0)
                throw new InvalidDataException("Committed and not empty transaction hash can't be equal to 0");
            if (current->CompressedSize <= 0)
                throw new InvalidDataException("Compression error in transaction.");

            if (previous == null)
                return;

            if (current->TransactionId != 1 &&
                // 1 is a first storage transaction which does not increment transaction counter after commit
                current->TransactionId - previous->TransactionId != 1)
                throw new InvalidDataException("Unexpected transaction id. Expected: " + (previous->TransactionId + 1) +
                                               ", got:" + current->TransactionId);
        }

        private bool ValidatePagesHash(StorageEnvironmentOptions options, TransactionHeader* current)
        {
            // The location of the data is the base pointer, plus the space reserved for the transaction header if uncompressed. 
            byte* dataPtr = _journalPager.AcquirePagePointer(null, _readingPage) + sizeof(TransactionHeader);

            ulong hash = Hashing.XXHash64.Calculate(dataPtr, current->CompressedSize);
            if (hash != current->Hash)
            {
                RequireHeaderUpdate = true;
                options.InvokeRecoveryError(this, "Invalid hash signature for transaction: " + current->ToString(), null);

                return false;
            }
            return true;
        }

        public override string ToString()
        {
            return _journalPager.ToString();
        }
    }
}

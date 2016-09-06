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

        public bool ReadOneTransaction(StorageEnvironmentOptions options, bool checkCrc = true)
        {
            if (_readingPage >= _journalPager.NumberOfAllocatedPages)
                return false;

            if (MaxPageToRead != null && _readingPage >= MaxPageToRead.Value)
                return false;

            TransactionHeader* current;
            if (!TryReadAndValidateHeader(options, out current))
                return false;

            // Uncompressed transactions will respect boundaries between the TransactionHeader page and the data, so we move to the data page.
            if (current->Compressed == false)                 
                _readingPage++;

            var transactionSize = GetNumberOfPagesFromSize(options, current->Compressed ? current->CompressedSize + sizeof(TransactionHeader) : current->UncompressedSize);

            if (current->TransactionId <= _lastSyncedTransactionId)
            {
                LastTransactionHeader = current;
                _readingPage += transactionSize;
                return true; // skipping
            }

            if (checkCrc && !ValidatePagesHash(options, current))
                return false;

            byte* outputPage;
            if (current->Compressed)
            {
                var numberOfPages = _recoveryPager.GetNumberOfOverflowPages(current->UncompressedSize);
                _recoveryPager.EnsureContinuous(0, numberOfPages);
                outputPage = _recoveryPager.AcquirePagePointer(null, 0);
                UnmanagedMemory.Set(outputPage, 0, numberOfPages * options.PageSize);

                try
                {
                    LZ4.Decode64(outputPage, current->CompressedSize, outputPage, current->UncompressedSize, true);
                }
                catch (Exception e)
                {
                    options.InvokeRecoveryError(this, "Could not de-compress, invalid data", e);
                    RequireHeaderUpdate = true;

                    return false;
                }
            }
            else
            {
                options.InvokeRecoveryError(this, "Got an invalid uncompressed transaction", null);
                RequireHeaderUpdate = true;
                return false;
            }

            var pageInfoPtr = (TransactionHeaderPageInfo *)outputPage;
            var diffedDataPtr = (byte*)outputPage + sizeof(TransactionHeaderPageInfo) * current->PageCount;
            var diffApplier = new DiffApplier();
            _dataPager.EnsureContinuous(current->NextPageNumber, 1);

            for (var i = 0; i < current->PageCount; i++)
            {
                Debug.Assert(_journalPager.Disposed == false);
                Debug.Assert(_recoveryPager.Disposed == false);
                var pagePtr = _dataPager.AcquirePagePointer(null, pageInfoPtr[i].PageNumber);
                switch (pageInfoPtr[i].Type)
                {
                    case JournalPageType.Diff:
                        diffApplier.Destination = pagePtr;
                        diffApplier.Diff = diffedDataPtr;
                        diffApplier.DiffSize = pageInfoPtr[i].Size;
                        diffApplier.Apply();
                        break;
                    case JournalPageType.Full:
                        Memory.Copy(pagePtr, diffedDataPtr, pageInfoPtr[i].Size);
                        break;
                    default:
                        throw new ArgumentOutOfRangeException("Type","Could not understand page journal type " + pageInfoPtr[i].Type);
                }
                diffedDataPtr += pageInfoPtr[i].Size;
            }

            LastTransactionHeader = current;

            return true;
        }

        internal static int GetNumberOfPagesFromSize(StorageEnvironmentOptions options, int size)
        {
            return (size/options.PageSize) + (size%options.PageSize == 0 ? 0 : 1);
        }

        public void RecoverAndValidate(StorageEnvironmentOptions options)
        {
            while (ReadOneTransaction(options))
            {
            }
        }

        public void SetStartPage(long value)
        {
            _readingPage = value;
        }

        private bool TryReadAndValidateHeader(StorageEnvironmentOptions options, out TransactionHeader* current)
        {
            current = (TransactionHeader*) _journalPager.Read(null, _readingPage).Base;

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
                throw new InvalidDataException("Transaction id cannot be less than 0 (llt: " + current->TransactionId + " )");
            if (current->TxMarker.HasFlag(TransactionMarker.Commit) && current->LastPageNumber < 0)
                throw new InvalidDataException("Last page number after committed transaction must be greater than 0");
            if (current->TxMarker.HasFlag(TransactionMarker.Commit) && current->PageCount > 0 && current->Hash == 0)
                throw new InvalidDataException("Committed and not empty transaction hash can't be equal to 0");
            if (current->Compressed)
            {
                if (current->CompressedSize <= 0)
                    throw new InvalidDataException("Compression error in transaction.");
            }

            if (previous == null)
                return;

            if (current->TransactionId != 1 &&
                // 1 is a first storage transaction which does not increment transaction counter after commit
                current->TransactionId - previous->TransactionId != 1)
                throw new InvalidDataException("Unexpected transaction id. Expected: " + (previous->TransactionId + 1) + ", got:" + current->TransactionId);
        }

        private bool ValidatePagesHash(StorageEnvironmentOptions options, TransactionHeader* current)
        {
            // The location of the data is the base pointer, plus the space reserved for the transaction header if uncompressed. 
            byte* dataPtr = _journalPager.AcquirePagePointer(null, _readingPage) + (current->Compressed ? sizeof(TransactionHeader) : 0);

            ulong hash = Hashing.XXHash64.Calculate(dataPtr, current->Compressed ? current->CompressedSize : current->UncompressedSize);
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

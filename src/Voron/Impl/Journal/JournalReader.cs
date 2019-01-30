using Sparrow;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using Sparrow.Compression;
using Sparrow.Utils;
using Voron.Data;
using Voron.Exceptions;
using Voron.Global;
using Voron.Impl.Paging;

namespace Voron.Impl.Journal
{
    public sealed unsafe class JournalReader : IPagerLevelTransactionState
    {
        private readonly AbstractPager _journalPager;
        private readonly AbstractPager _dataPager;
        private readonly AbstractPager _recoveryPager;
        private readonly HashSet<long> _modifiedPages;
        private readonly JournalInfo _journalInfo;
        private long _readAt4Kb;
        private readonly DiffApplier _diffApplier = new DiffApplier();
        private readonly long _journalPagerNumberOfAllocated4Kb;
        private readonly List<EncryptionBuffer> _encryptionBuffers;

        public bool RequireHeaderUpdate { get; private set; }

        public long Next4Kb => _readAt4Kb;

        public JournalReader(AbstractPager journalPager, AbstractPager dataPager, AbstractPager recoveryPager, HashSet<long> modifiedPages, JournalInfo journalInfo, TransactionHeader* previous)
        {
            RequireHeaderUpdate = false;
            _journalPager = journalPager;
            _dataPager = dataPager;
            _recoveryPager = recoveryPager;
            _modifiedPages = modifiedPages;
            _journalInfo = journalInfo;
            _readAt4Kb = 0;
            LastTransactionHeader = previous;
            _journalPagerNumberOfAllocated4Kb = 
                _journalPager.TotalAllocationSize /(4*Constants.Size.Kilobyte);

            if (journalPager.Options.EncryptionEnabled)
                _encryptionBuffers = new List<EncryptionBuffer>();
        }

        public TransactionHeader* LastTransactionHeader { get; private set; }

        public bool ReadOneTransactionToDataFile(StorageEnvironmentOptions options)
        {
            if (_readAt4Kb >= _journalPagerNumberOfAllocated4Kb)
                return false;

            TransactionHeader* current;
            if (TryReadAndValidateHeader(options, out current) == false)
            {
                var lastValid4Kb = _readAt4Kb;
                _readAt4Kb++;
                while (_readAt4Kb < _journalPagerNumberOfAllocated4Kb)
                {
                    if (TryReadAndValidateHeader(options, out current))
                    {
                        RequireHeaderUpdate = true;
                        break;
                    }
                    _readAt4Kb++;
                }

                _readAt4Kb = lastValid4Kb;
                return false;
            }
            bool performDecompression = current->CompressedSize != -1;

            var size = current->CompressedSize != -1 ? current->CompressedSize : current->UncompressedSize;

            var transactionSizeIn4Kb =
                (size + sizeof(TransactionHeader)) / (4*Constants.Size.Kilobyte) +
                ((size + sizeof(TransactionHeader)) % (4*Constants.Size.Kilobyte) == 0 ? 0 : 1);


            if (current->TransactionId <= _journalInfo.LastSyncedTransactionId)
            {
                _readAt4Kb += transactionSizeIn4Kb;
                LastTransactionHeader = current;
                return true; // skipping
            }

            _readAt4Kb += transactionSizeIn4Kb;
            
            TransactionHeaderPageInfo* pageInfoPtr;
            byte* outputPage;
            if (performDecompression)
            {
                var numberOfPages = GetNumberOfPagesFor(current->UncompressedSize);
                _recoveryPager.EnsureContinuous(0, numberOfPages);
                _recoveryPager.EnsureMapped(this, 0, numberOfPages);
                outputPage = _recoveryPager.AcquirePagePointer(this, 0);
                Memory.Set(outputPage, 0, (long)numberOfPages * Constants.Storage.PageSize);

                try
                {
                    LZ4.Decode64LongBuffers((byte*)current + sizeof(TransactionHeader), current->CompressedSize, outputPage,
                        current->UncompressedSize, true);
                }
                catch (Exception e)
                {
                    options.InvokeRecoveryError(this, "Could not de-compress, invalid data", e);
                    RequireHeaderUpdate = true;

                    return false;
                }
                pageInfoPtr = (TransactionHeaderPageInfo*)outputPage;
            }
            else
            {
                var numberOfPages = GetNumberOfPagesFor(current->UncompressedSize);
                _recoveryPager.EnsureContinuous(0, numberOfPages);
                _recoveryPager.EnsureMapped(this, 0, numberOfPages);
                outputPage = _recoveryPager.AcquirePagePointer(this, 0);
                Memory.Set(outputPage, 0, (long)numberOfPages * Constants.Storage.PageSize);
                Memory.Copy(outputPage, (byte*)current + sizeof(TransactionHeader), current->UncompressedSize);
                pageInfoPtr = (TransactionHeaderPageInfo*)outputPage;
            }

            long totalRead = sizeof(TransactionHeaderPageInfo) * current->PageCount;
            if (totalRead > current->UncompressedSize)
                throw new InvalidDataException($"Attempted to read position {totalRead} from transaction data while the transaction is size {current->UncompressedSize}");

            for (var i = 0; i < current->PageCount; i++)
            {
                if(pageInfoPtr[i].PageNumber > current->LastPageNumber)
                    throw new InvalidDataException($"Transaction {current->TransactionId} contains reference to page {pageInfoPtr[i].PageNumber} which is after the last allocated page {current->LastPageNumber}");
            }

            for (var i = 0; i < current->PageCount; i++)
            {
                if (totalRead > current->UncompressedSize)
                    throw new InvalidDataException($"Attempted to read position {totalRead} from transaction data while the transaction is size {current->UncompressedSize}");

                Debug.Assert(_journalPager.Disposed == false);
                if (performDecompression)
                    Debug.Assert(_recoveryPager.Disposed == false);

                var numberOfPagesOnDestination = GetNumberOfPagesFor(pageInfoPtr[i].Size);
                _dataPager.EnsureContinuous(pageInfoPtr[i].PageNumber, numberOfPagesOnDestination);
                _dataPager.EnsureMapped(this, pageInfoPtr[i].PageNumber, numberOfPagesOnDestination);

                // We are going to overwrite the page, so we don't care about its current content
                var pagePtr = _dataPager.AcquirePagePointerForNewPage(this, pageInfoPtr[i].PageNumber, numberOfPagesOnDestination);
                
                var pageNumber = *(long*)(outputPage + totalRead);
                if (pageInfoPtr[i].PageNumber != pageNumber)
                    throw new InvalidDataException($"Expected a diff for page {pageInfoPtr[i].PageNumber} but got one for {pageNumber}");
                totalRead += sizeof(long);

                _modifiedPages.Add(pageNumber);

                for (var j = 1; j < numberOfPagesOnDestination; j++)
                {
                    _modifiedPages.Remove(pageNumber + j);
                }

                _dataPager.UnprotectPageRange(pagePtr, (ulong)pageInfoPtr[i].Size);
 
                if (pageInfoPtr[i].DiffSize == 0)
                {
                    if (pageInfoPtr[i].Size == 0)
                    {
                        // diff contained no changes
                        continue;
                    }

                    var journalPagePtr = outputPage + totalRead;

                    if (options.EncryptionEnabled == false)
                    {
                        var pageHeader = (PageHeader*)journalPagePtr;

                        var checksum = StorageEnvironment.CalculatePageChecksum((byte*)pageHeader, pageNumber, out var expectedChecksum);
                        if (checksum != expectedChecksum) 
                            ThrowInvalidChecksumOnPageFromJournal(pageNumber, current, expectedChecksum, checksum, pageHeader);
                    }

                    Memory.Copy(pagePtr, journalPagePtr, pageInfoPtr[i].Size);
                    totalRead += pageInfoPtr[i].Size;

                    if (options.EncryptionEnabled)
                    {
                        var pageHeader = (PageHeader*)pagePtr;

                        if ((pageHeader->Flags & PageFlags.Overflow) == PageFlags.Overflow)
                        {
                            // need to mark overlapped buffers as invalid for commit

                            var encryptionBuffers = ((IPagerLevelTransactionState)this).CryptoPagerTransactionState[_dataPager].LoadedBuffers;

                            var numberOfPages = VirtualPagerLegacyExtensions.GetNumberOfOverflowPages(pageHeader->OverflowSize);

                            for (var j = 1; j < numberOfPages; j++)
                            {
                                if (encryptionBuffers.TryGetValue(pageNumber + j, out var buffer))
                                {
                                    buffer.SkipOnTxCommit = true;
                                }
                            }
                        }
                    }
                }
                else
                {
                    _diffApplier.Destination = pagePtr;
                    _diffApplier.Diff = outputPage + totalRead;
                    _diffApplier.Size = pageInfoPtr[i].Size;
                    _diffApplier.DiffSize = pageInfoPtr[i].DiffSize;
                    _diffApplier.Apply(pageInfoPtr[i].IsNewDiff);
                    totalRead += pageInfoPtr[i].DiffSize;
                }

                _dataPager.ProtectPageRange(pagePtr, (ulong)pageInfoPtr[i].Size);
            }

            LastTransactionHeader = current;

            return true;
        }

        private void ThrowInvalidChecksumOnPageFromJournal(long pageNumber, TransactionHeader* current, ulong expectedChecksum, ulong checksum, PageHeader* pageHeader)
        {
            var message =
                $"Invalid checksum for page {pageNumber} in transaction {current->TransactionId}, journal file {_journalPager} might be corrupted, expected hash to be {expectedChecksum} but was {checksum}." +
                $"Data from journal has not been applied to data file {_dataPager} yet. ";

            message += $"Page flags: {pageHeader->Flags}. ";

            if ((pageHeader->Flags & PageFlags.Overflow) == PageFlags.Overflow)
                message += $"Overflow size: {pageHeader->OverflowSize}. ";


            throw new InvalidDataException(message);
        }

        public int RecoverAndValidate(StorageEnvironmentOptions options, TransactionHeader[] transactionHeaders)
        {
            int index = 0;
            while (ReadOneTransactionToDataFile(options))
            {
                transactionHeaders[index++] = *LastTransactionHeader;
            }
            ZeroRecoveryBufferIfNeeded(this, options);

            return index;
        }

        public void ZeroRecoveryBufferIfNeeded(IPagerLevelTransactionState tx, StorageEnvironmentOptions options)
        {
            if (options.EncryptionEnabled == false)
                return;
            var recoveryBufferSize = _recoveryPager.NumberOfAllocatedPages * Constants.Storage.PageSize;
            _recoveryPager.EnsureMapped(tx, 0, checked((int)_recoveryPager.NumberOfAllocatedPages));
            var pagePointer = _recoveryPager.AcquirePagePointer(tx, 0);
            Sodium.sodium_memzero(pagePointer, (UIntPtr)recoveryBufferSize);
        }

        public void SetStartPage(long value)
        {
            _readAt4Kb = value;
        }

        private void DecryptTransaction(byte* page, StorageEnvironmentOptions options)
        {
            var txHeader = (TransactionHeader*)page;
            var num = txHeader->TransactionId;

            if ((txHeader->Flags & TransactionPersistenceModeFlags.Encrypted) != TransactionPersistenceModeFlags.Encrypted)
                throw new InvalidOperationException($"Unable to decrypt transaction {num}, not encrypted");

            var subKeyLen = Sodium.crypto_aead_xchacha20poly1305_ietf_keybytes();
            var subKey = stackalloc byte[(int)subKeyLen ];
            fixed (byte* mk = options.MasterKey)
            fixed (byte* ctx = WriteAheadJournal.Context)
            {
                if (Sodium.crypto_kdf_derive_from_key(subKey, subKeyLen, (ulong)num, ctx, mk) != 0)
                    throw new InvalidOperationException("Unable to generate derived key");
            }

            var size = txHeader->CompressedSize != -1 ? txHeader->CompressedSize : txHeader->UncompressedSize;

            var rc = Sodium.crypto_aead_xchacha20poly1305_ietf_decrypt_detached(
                page + TransactionHeader.SizeOf,
                null,
                page + TransactionHeader.SizeOf,
                (ulong)size,
                page + TransactionHeader.MacOffset,
                page,
                (ulong)(TransactionHeader.SizeOf - TransactionHeader.NonceOffset),
                page + TransactionHeader.NonceOffset,
                subKey
            );

            if (rc != 0)
                throw new InvalidOperationException($"Unable to decrypt transaction {num}, rc={rc}");
        }

        private bool TryReadAndValidateHeader(StorageEnvironmentOptions options, out TransactionHeader* current)
        {
            if (_readAt4Kb > _journalPagerNumberOfAllocated4Kb)
            {
                current = null;
                return false; // end of jouranl
            }

            const int pageTo4KbRatio = Constants.Storage.PageSize / (4 * Constants.Size.Kilobyte);
            var pageNumber = _readAt4Kb / pageTo4KbRatio;
            var positionInsidePage = (_readAt4Kb % pageTo4KbRatio) * (4 * Constants.Size.Kilobyte);

            current = (TransactionHeader*)
                (_journalPager.AcquirePagePointer(this, pageNumber) + positionInsidePage);

            // due to the reuse of journals we no longer can assume we have zeros in the end of the journal
            // we might have there random garbage or old transactions we can ignore, so we have the following scenarios:
            // * TxId <= current Id      ::  we can ignore old transaction of the reused journal and continue
            // * TxId == current Id + 1  ::  valid, but if hash is invalid. Transaction hasn't been committed
            // * TxId >  current Id + 1  ::  if hash is invalid we can ignore reused/random, but if hash valid then we might missed TXs 

            if (current->HeaderMarker != Constants.TransactionHeaderMarker)
            {  
                // not a transaction page, 

                // if the header marker is zero or garbage, we are probably in the area at the end of the log file, and have no additional log records
                // to read from it. This can happen if the next transaction was too big to fit in the current log file. We stop reading
                // this log file and move to the next one, or it might have happened becuase of reuse of journal file 

                // note : we might encounter a "valid" TransactionHeaderMarker which is still garbage, so we will test that later on

                RequireHeaderUpdate = false;
                return false;
            }

            if (current->TransactionId < 0)
                return false;

            current = EnsureTransactionMapped(current, pageNumber, positionInsidePage);

            bool hashIsValid;
            if (options.EncryptionEnabled)
            {
                // We use temp buffers to hold the transaction before decrypting, and release the buffers afterwards.
                var pagesSize = current->CompressedSize != -1 ? current->CompressedSize : current->UncompressedSize;
                var size = (4 * Constants.Size.Kilobyte) * GetNumberOf4KbFor(sizeof(TransactionHeader) + pagesSize);

                var ptr = NativeMemory.Allocate4KbAlignedMemory(size, out var thread);
                var buffer = new EncryptionBuffer
                {
                    Pointer = ptr,
                    Size = size,
                    AllocatingThread = thread
                };

                _encryptionBuffers.Add(buffer);
                Memory.Copy(buffer.Pointer, (byte*)current, size);
                current = (TransactionHeader*)buffer.Pointer;

                try
                {
                    DecryptTransaction((byte*)current, options);
                    hashIsValid = true;
                }
                catch (InvalidOperationException ex)
                {
                    RequireHeaderUpdate = true;
                    options.InvokeRecoveryError(this, "Transaction " + current->TransactionId + " was not committed", ex);
                    return false;
                }
            }
            else
            {
                hashIsValid = ValidatePagesHash(options, current);
            }

            long lastTxId;

            if (LastTransactionHeader != null)
            {
                lastTxId = LastTransactionHeader->TransactionId;
            }
            else
            {
                // this is first transaction being processed in the recovery process

                if (_journalInfo.LastSyncedTransactionId == -1 || current->TransactionId <= _journalInfo.LastSyncedTransactionId)
                    return hashIsValid;

                lastTxId = _journalInfo.LastSyncedTransactionId;
            }

            var txIdDiff = current->TransactionId - lastTxId;

            // 1 is a first storage transaction which does not increment transaction counter after commit
            if (current->TransactionId != 1)
            {
                if (txIdDiff < 0)
                    return false;

                if (txIdDiff > 1 || txIdDiff == 0)
                {
                    if (hashIsValid)
                    {
                        // TxId is bigger then the last one by nore the '1' but has valid hash which mean we lost transactions in the middle

                        if (LastTransactionHeader != null)
                        {
                            throw new InvalidJournalException(
                                $"Transaction has valid(!) hash with invalid transaction id {current->TransactionId}, the last valid transaction id is {LastTransactionHeader->TransactionId}." +
                                $" Journal file {_journalPager.FileName} might be corrupted", _journalInfo);
                        }

                        throw new InvalidJournalException(
                            $"The last synced transaction id was {_journalInfo.LastSyncedTransactionId} (in journal: {_journalInfo.LastSyncedJournal}) but the first transaction being read in the recovery process is {current->TransactionId} (transaction has valid hash). " +
                            $"Some journals are missing. Current journal file {_journalPager.FileName}.", _journalInfo);
                    }
                }

                // if (txIdDiff == 1) :
                if (current->LastPageNumber <= 0)
                    throw new InvalidDataException("Last page number after committed transaction must be greater than 0");
            }

            if (hashIsValid == false)
            {
                RequireHeaderUpdate = true;
                return false;
            }

            return true;
        }

        private TransactionHeader* EnsureTransactionMapped(TransactionHeader* current, long pageNumber, long positionInsidePage)
        {
            var size = current->CompressedSize != -1 ? current->CompressedSize : current->UncompressedSize;
            var numberOfPages = GetNumberOfPagesFor(positionInsidePage + sizeof(TransactionHeader) + size);
            _journalPager.EnsureMapped(this, pageNumber, numberOfPages);

            var pageHeader = _journalPager.AcquirePagePointer(this, pageNumber)
                             + positionInsidePage;

            return (TransactionHeader*)pageHeader;
        }
        
        private bool ValidatePagesHash(StorageEnvironmentOptions options, TransactionHeader* current)
        {
            byte* dataPtr = (byte*)current + sizeof(TransactionHeader);

            var size = current->CompressedSize != -1 ? current->CompressedSize : current->UncompressedSize;
            if (size < 0)
            {
                RequireHeaderUpdate = true;
                // negative size is not supported
                options.InvokeRecoveryError(this, $"Compresses size {current->CompressedSize} is negative", null);
                return false;
            }
            if (size >
                (_journalPagerNumberOfAllocated4Kb - _readAt4Kb) * 4 * Constants.Size.Kilobyte)
            {
                // we can't read past the end of the journal
                RequireHeaderUpdate = true;
                var compressLabel = (current->CompressedSize != -1) ? "Compressed" : "Uncompressed";
                options.InvokeRecoveryError(this, $"Size {size} ({compressLabel}) is too big for the journal size {_journalPagerNumberOfAllocated4Kb * 4 * Constants.Size.Kilobyte}", null);
                return false;
            }

            ulong hash = Hashing.XXHash64.Calculate(dataPtr, (ulong)size, (ulong)current->TransactionId);
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

        public void Dispose()
        {
            if (_encryptionBuffers != null) // Encryption enabled
            { 
                foreach (var buffer in _encryptionBuffers)
                    NativeMemory.Free4KbAlignedMemory(buffer.Pointer, buffer.Size, buffer.AllocatingThread);
                BeforeCommitFinalization?.Invoke(this);
            }
            OnDispose?.Invoke(this);
        }

        private static int GetNumberOfPagesFor(long size)
        {
            return checked((int)(size / Constants.Storage.PageSize) + (size % Constants.Storage.PageSize == 0 ? 0 : 1));
        }

        private static int GetNumberOf4KbFor(long size)
        {
            return checked((int)(size / (4*Constants.Size.Kilobyte)) + (size % (4 * Constants.Size.Kilobyte) == 0 ? 0 : 1));
        }

        Dictionary<AbstractPager, TransactionState> IPagerLevelTransactionState.PagerTransactionState32Bits { get; set; }

        Dictionary<AbstractPager, CryptoTransactionState> IPagerLevelTransactionState.CryptoPagerTransactionState { get; set; }

        public event Action<IPagerLevelTransactionState> OnDispose;
        public event Action<IPagerLevelTransactionState> BeforeCommitFinalization;

        void IPagerLevelTransactionState.EnsurePagerStateReference(PagerState state)
        {
            //nothing to do
        }

        StorageEnvironment IPagerLevelTransactionState.Environment => null;

        // JournalReader actually writes to the data file
        bool IPagerLevelTransactionState.IsWriteTransaction => true;

        public long NumberOfAllocated4Kb => _journalPagerNumberOfAllocated4Kb;
    }
}

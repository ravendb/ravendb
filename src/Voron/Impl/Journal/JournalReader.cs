using Sparrow;
using System;
using System.Buffers;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using Sparrow.Compression;
using Sparrow.Server.Platform;
using Sparrow.Server.Utils;
using Voron.Exceptions;
using Voron.Impl.Paging;
using Constants = Voron.Global.Constants;
using System.Linq;
using Sparrow.Platform;
using Voron.Impl.FileHeaders;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Win32.SafeHandles;
using Sparrow.Json;
using Sparrow.Logging;
using Sparrow.Server.Logging;
using Voron.Logging;
using Voron.Util.Settings;
using static Voron.StorageEnvironmentOptions;

namespace Voron.Impl.Journal
{
    public sealed unsafe class JournalReader
    {
        private readonly StorageEnvironment _environment;
        private readonly long _journalNumber;
        private readonly Pager _journalPager;
        private readonly Pager.State _journalPagerState;
        private readonly Pager _dataPager;
        private readonly Pager _recoveryPager;
        private readonly HashSet<long> _modifiedPages;
        private readonly JournalInfo _journalInfo;
        private readonly FileHeader _currentFileHeader;
        private long _readAt4Kb;
        private long _next4Kb;
        private readonly DiffApplier _diffApplier = new();
        private readonly long _journalPagerNumberOfAllocated4Kb;
        private readonly List<Pager.EncryptionBuffer> _encryptionBuffers;
        private TransactionHeader* _firstValidTransactionHeader = null;

        private long? _firstSkippedTx;
        private long? _lastSkippedTx;

        public bool RequireHeaderUpdate { get; private set; }

        public long Next4Kb => _next4Kb;

        public JournalReader(StorageEnvironment environment, long journalNumber, Pager journalPager, Pager.State journalPagerState, Pager dataPager, Pager recoveryPager,
            HashSet<long> modifiedPages, JournalInfo journalInfo, FileHeader currentFileHeader, TransactionHeader* previous)
        {
            RequireHeaderUpdate = false;
            _environment = environment;
            _journalNumber = journalNumber;
            _journalPager = journalPager;
            _journalPagerState = journalPagerState;
            _dataPager = dataPager;
            _recoveryPager = recoveryPager;
            _modifiedPages = modifiedPages;
            _journalInfo = journalInfo;
            _currentFileHeader = currentFileHeader;
            _readAt4Kb = 0;
            LastTransactionHeader = previous;
            _journalPagerNumberOfAllocated4Kb = _journalPagerState.TotalAllocatedSize / (4 * Constants.Size.Kilobyte);
            JournalId = _currentFileHeader.JournalId;
            if (journalPager.Options.Encryption.IsEnabled)
                _encryptionBuffers = new List<Pager.EncryptionBuffer>();
            _log = RavenLogManager.Instance.GetLoggerForVoron<StorageEnvironment>(_environment.Options, _journalPager.FileName);
        }

        public TransactionHeader* LastTransactionHeader { get; private set; }

        public bool ReadOneTransactionToDataFile(ref Pager.State dataPagerState, ref Pager.State recoveryPagerState, ref Pager.PagerTransactionState txState,
            SafeFileHandle fileHandle, StorageEnvironmentOptions options)
        {
            if (_readAt4Kb >= _journalPagerNumberOfAllocated4Kb)
                return false;

            if (TryReadAndValidateHeader(options, ref txState, out TransactionHeader* current) == false)
            {
                return false;
            }

            if (IsAlreadySyncTransaction(current->TransactionId))
            {
                SkipCurrentTransaction(current);
                return true;
            }

            var performDecompression = current->CompressedSize != -1;

            var transactionSizeIn4Kb = GetTransactionSizeIn4Kb(current);

            _readAt4Kb += transactionSizeIn4Kb;

            var numberOfPages = GetNumberOfPagesFor(current->UncompressedSize);
            _recoveryPager.EnsureContinuous(ref recoveryPagerState, 0, numberOfPages);
            _recoveryPager.EnsureMapped(recoveryPagerState, ref txState, 0, numberOfPages);
            var outputPage = _recoveryPager.MakeWritable(recoveryPagerState,
                _recoveryPager.AcquireRawPagePointer(recoveryPagerState, ref txState, 0)
            );
            Memory.Set(outputPage, 0, (long)numberOfPages * Constants.Storage.PageSize);

            TransactionHeaderPageInfo* pageInfoPtr;
            if (performDecompression)
            {
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
                Memory.Copy(outputPage, (byte*)current + sizeof(TransactionHeader), current->UncompressedSize);
                pageInfoPtr = (TransactionHeaderPageInfo*)outputPage;
            }

            long totalRead = sizeof(TransactionHeaderPageInfo) * current->PageCount;
            if (totalRead > current->UncompressedSize)
                throw new InvalidDataException($"Attempted to read position {totalRead} from transaction data while the transaction is size {current->UncompressedSize}");

            for (var i = 0; i < current->PageCount; i++)
            {
                if (pageInfoPtr[i].PageNumber > current->LastPageNumber)
                    throw new InvalidDataException(
                        $"Transaction {current->TransactionId} contains reference to page {pageInfoPtr[i].PageNumber} which is after the last allocated page {current->LastPageNumber}");
            }

            long bufferSize = 0;
            byte* currentBuffer = null;
            try
            {
                for (var i = 0; i < current->PageCount; i++)
                {
                    if (totalRead > current->UncompressedSize)
                        throw new InvalidDataException(
                            $"Attempted to read position {totalRead} from transaction data while the transaction is size {current->UncompressedSize}");

                    Debug.Assert(_journalPagerState.Disposed == false);
                    Debug.Assert(performDecompression == false || recoveryPagerState.Disposed == false);

                    var numberOfPagesOnDestination = GetNumberOfPagesFor(pageInfoPtr[i].Size);
                    _dataPager.EnsureContinuous(ref dataPagerState, pageInfoPtr[i].PageNumber, numberOfPagesOnDestination);

                    var pageNumber = *(long*)(outputPage + totalRead);
                    if (pageInfoPtr[i].PageNumber != pageNumber)
                        throw new InvalidDataException($"Expected a diff for page {pageInfoPtr[i].PageNumber} but got one for {pageNumber}");
                    totalRead += sizeof(long);

                    _modifiedPages.Add(pageNumber);

                    for (var j = 1; j < numberOfPagesOnDestination; j++)
                    {
                        _modifiedPages.Remove(pageNumber + j);
                    }

                    long pageSize = (long)numberOfPagesOnDestination * Constants.Storage.PageSize;
                    if (pageSize > bufferSize)
                    {
                        currentBuffer = (byte*)NativeMemory.Realloc(currentBuffer, (nuint)pageSize);
                    }

                    if (pageInfoPtr[i].DiffSize == 0)
                    {
                        if (pageInfoPtr[i].Size == 0)
                        {
                            // diff contained no changes
                            continue;
                        }

                        var journalPagePtr = outputPage + totalRead;

                        if (options.Encryption.IsEnabled == false)
                        {
                            var pageHeader = (PageHeader*)journalPagePtr;
                            var checksum = StorageEnvironment.CalculatePageChecksum((byte*)pageHeader, pageNumber, out var expectedChecksum);
                            if (checksum != expectedChecksum)
                                ThrowInvalidChecksumOnPageFromJournal(pageNumber, current, expectedChecksum, checksum, pageHeader);
                        }

                        Memory.Copy(currentBuffer, journalPagePtr, pageInfoPtr[i].Size);

                        totalRead += pageInfoPtr[i].Size;
                    }
                    else
                    {
                        ReadPageFromFile(fileHandle, currentBuffer, pageSize, pageNumber);
                        _diffApplier.Destination = currentBuffer;
                        _diffApplier.Diff = outputPage + totalRead;
                        _diffApplier.Size = pageInfoPtr[i].Size;
                        _diffApplier.DiffSize = pageInfoPtr[i].DiffSize;
                        _diffApplier.Apply(pageInfoPtr[i].IsNewDiff);
                        totalRead += pageInfoPtr[i].DiffSize;
                    }

                    if (options.Encryption.IsEnabled)
                    {
                        Pager.Crypto.EncryptPage(options.Encryption.MasterKey, (PageHeader*)currentBuffer);

                    }

                    WritePageToFile(fileHandle, currentBuffer, pageSize, pageNumber);
                }
            }
            finally
            {
                NativeMemory.Free(currentBuffer);
            }

            LastTransactionHeader = current;

            return true;
        }

        private static void WritePageToFile(SafeFileHandle fileHandle, byte* currentBuffer, long pageSize, long pageNumber)
        {
            while (pageSize > 0)
            {
                int sizeToWrite = (int)Math.Min(pageSize, int.MaxValue);
                RandomAccess.Write(fileHandle, new Span<byte>(currentBuffer, sizeToWrite), pageNumber * Constants.Storage.PageSize);
                pageSize -= sizeToWrite;
                currentBuffer += sizeToWrite;
            }
        }

        private static void ReadPageFromFile(SafeFileHandle fileHandle, byte* currentBuffer, long pageSize, long pageNumber)
        {
            while (pageSize > 0)
            {
                int size = (int)Math.Min(pageSize, int.MaxValue);
                RandomAccess.Read(fileHandle, new Span<byte>(currentBuffer, size), pageNumber * Constants.Storage.PageSize);
                pageSize -= size;
                currentBuffer += size;
            }

        }

        private void SkipCurrentTransaction(TransactionHeader* current)
        {
            var transactionSizeIn4Kb = GetTransactionSizeIn4Kb(current);

            _readAt4Kb += transactionSizeIn4Kb;

            if (LastTransactionHeader == null || LastTransactionHeader->TransactionId < current->TransactionId) // precaution
            {
                if (current->TransactionId > _journalInfo.LastSyncedTransactionId)
                    LastTransactionHeader = current;
            }

            if (_firstSkippedTx == null)
                _firstSkippedTx = current->TransactionId;
            else
                _lastSkippedTx = current->TransactionId;
        }

        private bool IsAlreadySyncTransaction(long transactionId)
        {
            return _journalInfo.LastSyncedTransactionId != -1 && transactionId <= _journalInfo.LastSyncedTransactionId;
        }

        private static long GetTransactionSizeIn4Kb(TransactionHeader* current)
        {
            var size = current->CompressedSize != -1 ? current->CompressedSize : current->UncompressedSize;

            var transactionSizeIn4Kb =
                (size + sizeof(TransactionHeader)) / (4 * Constants.Size.Kilobyte) +
                ((size + sizeof(TransactionHeader)) % (4 * Constants.Size.Kilobyte) == 0 ? 0 : 1);
            return transactionSizeIn4Kb;
        }

        [DoesNotReturn]
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

        public List<TransactionHeader> RecoverAndValidate(ref Pager.State dataPagerState, ref Pager.State recoveryPagerState, ref Pager.PagerTransactionState txState,
            StorageEnvironmentOptions options)
        {
            var transactionHeaders = new List<TransactionHeader>();
            var rc = Pal.rvn_pager_get_file_handle(dataPagerState.Handle, out var fileHandle, out int errorCode);
            if (rc != PalFlags.FailCodes.Success)
                PalHelper.ThrowLastError(rc, errorCode, "Failed to get a file handle to " + _dataPager.FileName);
            using var _ = fileHandle;
            while (ReadOneTransactionToDataFile(ref dataPagerState, ref recoveryPagerState, ref txState, fileHandle, options))
            {
                Debug.Assert(transactionHeaders.Count == 0 || LastTransactionHeader->TransactionId > transactionHeaders.Last().TransactionId, 
                    $"LastTransactionHeader->TransactionId: {LastTransactionHeader->TransactionId}, transactionHeaders.Last().TransactionId: {transactionHeaders.Last().TransactionId}");

                if (LastTransactionHeader != null)
                    transactionHeaders.Add(*LastTransactionHeader);
            }

            ZeroRecoveryBufferIfNeeded(recoveryPagerState, ref txState, options);

            return transactionHeaders;
        }

        public void ZeroRecoveryBufferIfNeeded(Pager.State recoveryPagerState, ref Pager.PagerTransactionState txState, StorageEnvironmentOptions options)
        {
            if (options.Encryption.IsEnabled == false)
                return;
            var recoveryBufferSize = recoveryPagerState.NumberOfAllocatedPages * Constants.Storage.PageSize;
            _recoveryPager.EnsureMapped(recoveryPagerState, ref txState, 0, checked((int)recoveryPagerState.NumberOfAllocatedPages));
            var pagePointer = _recoveryPager.MakeWritable(recoveryPagerState, 
                _recoveryPager.AcquireRawPagePointer(recoveryPagerState, ref txState, 0)
            );
            Sodium.sodium_memzero(pagePointer, (UIntPtr)recoveryBufferSize);
        }

        private void DecryptTransaction(byte* page, StorageEnvironmentOptions options)
        {
            var txHeader = (TransactionHeader*)page;
            var num = txHeader->TransactionId;

            if ((txHeader->Flags & TransactionPersistenceModeFlags.Encrypted) != TransactionPersistenceModeFlags.Encrypted)
                throw new InvalidOperationException($"Unable to decrypt transaction {num}, not encrypted");

            var subKeyLen = Sodium.crypto_aead_xchacha20poly1305_ietf_keybytes();
            var subKey = stackalloc byte[(int)subKeyLen];
            fixed (byte* mk = options.Encryption.MasterKey)
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
        
        private bool TryValidateTransaction(StorageEnvironmentOptions options, ref Pager.PagerTransactionState txState, out TransactionHeader* current)
        {
            const int pageTo4KbRatio = Constants.Storage.PageSize / (4 * Constants.Size.Kilobyte);
            var pageNumber = _readAt4Kb / pageTo4KbRatio;
            var positionInsidePage = (_readAt4Kb % pageTo4KbRatio) * (4 * Constants.Size.Kilobyte);

            current = (TransactionHeader*)(_journalPager.AcquirePagePointer(_journalPagerState, ref txState, pageNumber) + positionInsidePage);
            if (current->HeaderMarker != Constants.TransactionHeaderMarker)
                return false;

            long actualTransactionSize = sizeof(TransactionHeader) + 
                              (current->CompressedSize != -1 ? current->CompressedSize : current->UncompressedSize);
          
            if (_readAt4Kb * (4*Constants.Size.Kilobyte) + actualTransactionSize > _journalPagerState.TotalAllocatedSize)
                return false;

            // Note that journals are measured in units of 4KB while Voron pages are 8KB.
            // So we may read "half a page", that is fine, since the OS is always 4KB pages
            // and we explicitly checked the size above
            var requiredPages = GetNumberOfPagesFor(positionInsidePage + actualTransactionSize);
            _journalPager.EnsureMapped(_journalPagerState, ref txState, pageNumber, requiredPages);

            var pageHeader = _journalPager.AcquirePagePointer(_journalPagerState, ref txState, pageNumber)
                             + positionInsidePage;

            current = (TransactionHeader*)pageHeader;
            if (options.Encryption.IsEnabled is false)
            {
                if ((current->Flags & TransactionPersistenceModeFlags.Encrypted) == TransactionPersistenceModeFlags.Encrypted)
                    throw new InvalidOperationException(
                        "Encountered an encrypted transaction when opening a non encrypted storage. Did you forget to provide the encryption key?");

                bool hashIsValid = ValidatePagesHash(options, current);
                if (hashIsValid == false && CanIgnoreDataIntegrityErrorBecauseTxWasSynced(current->TransactionId, options))
                {
                    options.InvokeIntegrityErrorOfAlreadySyncedData(this,
                        $"Invalid hash of data of first transaction which has been already synced (tx id: {current->TransactionId}, last synced tx: {_journalInfo.LastSyncedTransactionId}, journal: {_journalNumber}). " +
                        "Safely continuing the startup recovery process.", null);

                    return true;
                }
                return hashIsValid;
            }

            // We use temp buffers to hold the transaction before decrypting, and release the buffers afterwards.
            var pagesSize = current->CompressedSize != -1 ? current->CompressedSize : current->UncompressedSize;
            var sizeIn4Kbs = (4 * Constants.Size.Kilobyte) * GetNumberOf4KbFor(sizeof(TransactionHeader) + pagesSize);

            var ptr = PlatformSpecific.NativeMemory.Allocate4KbAlignedMemory(sizeIn4Kbs, out var thread);
            var buffer = new Pager.EncryptionBuffer(options.Encryption.EncryptionBuffersPool, thread, ptr, sizeIn4Kbs);

            _encryptionBuffers.Add(buffer);
            Memory.Copy(buffer.Pointer, (byte*)current, sizeIn4Kbs);
            current = (TransactionHeader*)buffer.Pointer;

            try
            {
                DecryptTransaction((byte*)current, options);
                return true;
            }
            catch (InvalidOperationException ex)
            {
                if (CanIgnoreDataIntegrityErrorBecauseTxWasSynced(current->TransactionId, options))
                {
                    options.InvokeIntegrityErrorOfAlreadySyncedData(this,
                        $"Unable to decrypt data of transaction which has been already synced (tx id: {current->TransactionId}, last synced tx: {_journalInfo.LastSyncedTransactionId}, journal: {_journalNumber}). " +
                        "Safely continuing the startup recovery process.",
                        ex);

                    return true;
                }

                RequireHeaderUpdate = true;
                options.InvokeRecoveryError(this, $"Could not decrypt transaction {current->TransactionId}. It could be not committed", ex);

                return false;
            }
        }

        private void VerifyTransactionSequence(StorageEnvironmentOptions options, TransactionHeader* current)
        {
            long lastTxId;

            if (LastTransactionHeader != null)
            {
                lastTxId = LastTransactionHeader->TransactionId;
            }
            else
            {
                // this is first transaction being processed in the recovery process

                if (_journalInfo.LastSyncedTransactionId == -1 || current->TransactionId <= _journalInfo.LastSyncedTransactionId)
                {
                    AssertValidLastPageNumber(current);

                    if (_firstValidTransactionHeader == null)
                        _firstValidTransactionHeader = current;

                    return;
                }

                lastTxId = _journalInfo.LastSyncedTransactionId;
            }
            
            var txIdDiff = current->TransactionId - lastTxId;

            if (current->TransactionId != 1)
            {
                if (txIdDiff != 1)
                {
                    if (CanIgnoreDataIntegrityErrorBecauseTxWasSynced(current->TransactionId -1 , options))
                    {
                        // when running in ignore data integrity errors mode then we could skip corrupted but already sync data
                        // so it's expected in this case that txIdDiff > 1, let it continue to work then
                        options.InvokeIntegrityErrorOfAlreadySyncedData(this,
                            $"Encountered integrity error of transaction data which has been already synced (tx id: {current->TransactionId}, last synced tx: {_journalInfo.LastSyncedTransactionId}, journal: {_journalNumber}). Tx diff is: {txIdDiff}. " +
                            $"Safely continuing the startup recovery process. Debug details - file header {_currentFileHeader}", null);

                        return;
                    }

                    if (LastTransactionHeader != null)
                    {
                        throw new InvalidJournalException(
                            $"Transaction has valid(!) hash with invalid transaction id {current->TransactionId}, the last valid transaction id is {LastTransactionHeader->TransactionId}. Tx diff is: {txIdDiff}{AddSkipTxInfoDetails()}." +
                            $" Journal file {_journalPager.FileName} might be corrupted or some journals are missing. Debug details - file header {_currentFileHeader}",
                            _journalInfo);
                    }

                    throw new InvalidJournalException(
                        $"The last synced transaction id was {_journalInfo.LastSyncedTransactionId} (in journal: {_journalInfo.LastSyncedJournal}) but the first transaction being read in the recovery process is {current->TransactionId} in journal {_journalPager.FileName} (transaction has valid hash). Tx diff is: {txIdDiff}{AddSkipTxInfoDetails()}. " +
                        $"Some journals might be missing. Debug details - file header {_currentFileHeader}", _journalInfo);
                }

            }
            
            AssertValidLastPageNumber(current);

            if (_firstValidTransactionHeader == null)
                _firstValidTransactionHeader = current;

            return;

            void AssertValidLastPageNumber(TransactionHeader* transactionHeader)
            {
                if (transactionHeader->LastPageNumber <= 0)
                {
                    if (CanIgnoreDataIntegrityErrorBecauseTxWasSynced(transactionHeader->TransactionId, options))
                    {
                        options.InvokeIntegrityErrorOfAlreadySyncedData(this,
                            $"Invalid last page number ({transactionHeader->LastPageNumber}) in the header of transaction which has been already synced (tx id: {transactionHeader->TransactionId}, last synced tx: {_journalInfo.LastSyncedTransactionId}, journal: {_journalNumber}). " +
                            $"Safely continuing the startup recovery process. Debug details - file header {_currentFileHeader}", null);
                    }
                    else
                    {
                        throw new InvalidDataException(
                            $"Last page number after committed transaction must be greater than 0. Debug details - file header {_currentFileHeader}");
                    }
                }
            }
        }

        public readonly HashSet<Guid> RecoveredJournalIds = [];
        
        private bool TryReadAndValidateHeader(StorageEnvironmentOptions options, ref Pager.PagerTransactionState txState, out TransactionHeader* current)
        {
            for (; _readAt4Kb < _journalPagerNumberOfAllocated4Kb; _readAt4Kb++)
            {
                if (TryValidateTransaction(options, ref txState, out current) is false)
                {
                    Debug.Assert(current != null, "current != null");

                    if (VerifyNoUnexpectedValidTransactionsAfter(options, ref txState))
                    {
                        // we found a _valid_ transaction (and all the invalid ones were
                        // already synced, so we can safely ignore them), so we'll go 
                        // forward from there
                        _readAt4Kb--;
                        continue;
                    }

                    return false;
                }

                RecoveredJournalIds.Add(current->JournalId);

                _next4Kb = _readAt4Kb + GetTransactionSizeIn4Kb(current);

                if (JournalId == Guid.Empty)
                {
                    JournalId = current->JournalId;
                }

                if (current->Flags == TransactionPersistenceModeFlags.LinkedJournalsRecord &&
                    _environment.Options.RootJournal is null) // this only applies to the _root_, not to branches
                {
                    ProcessLinkedJournalsRecord(current);
                    _readAt4Kb += GetTransactionSizeIn4Kb(current) - 1;
                    continue;
                }
                
                if ((current->JournalId == JournalId) is false &&
                    current->JournalId != Guid.Empty) // this may be legacy
                {
                    // not our env, skip processing it
                    _readAt4Kb += GetTransactionSizeIn4Kb(current) - 1;
                    continue;
                }

                if (current->JournalId == Guid.Empty)
                {
                    if (Legacy_IsOldTransactionFromRecycledJournal(current))
                    {
                        _readAt4Kb += GetTransactionSizeIn4Kb(current) - 1;
                        continue;
                    }
                    // Here we are dealing with a valid transaction (in terms of tx id)
                    // that has zeroed DatabaseId, probably a legacy transaction for the 
                    // current database (non-shared journal mode), allowing it
                }

                VerifyTransactionSequence(options, current);
                
                LastTransactionHeader = current;
                return true;
            }

            current = null;
            return false;
        }


        private void ProcessLinkedJournalsRecord(TransactionHeader* current)
        {
            if (current->TransactionId != WriteAheadJournal.LinkedJournalsRecord.TransactionIdMarker)
            {
                throw new InvalidOperationException(
                    $"LinkedJournal record was expected to have proper TransactionId marker ({WriteAheadJournal.LinkedJournalsRecord.TransactionIdMarker}) " +
                    $"but was: {current->TransactionId}, file corruption?");
            }

            ReadOnlySpan<byte> buffer = new(current + 1, checked((int)current->UncompressedSize));
            int numberOfLinks = current->PageCount;
            for (int i = 0; i < numberOfLinks; i++)
            {
                var journalId = new Guid(buffer[..sizeof(Guid)]);
                buffer = buffer[sizeof(Guid)..];

                var nextSep = buffer.IndexOf((byte)0);
                if (nextSep == -1)
                    throw new InvalidOperationException("Unable to find null terminator for the linked journal path, got: " + Encoding.UTF8.GetString(buffer));
                
                var link = buffer[..nextSep];
                buffer = buffer[(nextSep + 1)..];
                var relativePath = Encoding.UTF8.GetString(link);
                string dest = Path.GetFullPath(relativePath, _journalPager.FileName);
                var dirInfo = new DirectoryInfo(dest);
                
                var basePath = dirInfo.Parent?.Parent?.FullName;
                if (basePath == null)
                    continue;

                if (_environment.Options.TryGetJournalId(basePath, out var envJournalId) == false)
                    continue;

                if (envJournalId != journalId)
                    continue;

                var rc = Pal.rvn_ensure_hard_link_non_durable(_journalPager.FileName, dest, out var errorCode);
                if (rc == PalFlags.FailCodes.Success) 
                    continue;
                
                // error handling here is complex, because we need to account for users manually removing / copying / moving the 
                // branch directory directly. That may break the existing links, but we cannot assume much about this. So we'll 
                // be optimistic about it and move on. This code is only meant to apply if we had a machine level failures and need
                // to re-wire the hard links that may have been dropped because it is too expensive to make them durable at the
                // link creation time
                if (_log.IsWarnEnabled is false) 
                    continue;
                    
                var msg = PalHelper.CreateErrorMessage(rc, errorCode, $"Failed to ensure hard link from '{_journalPager.FileName}' to: '{dest}', this can be because of manual manipulation of the storage environment directories.", out _);
                _log.Warn(msg);
            }
        }

        private bool VerifyNoUnexpectedValidTransactionsAfter(StorageEnvironmentOptions options, ref Pager.PagerTransactionState txState)
        {
            using var _ = options.DisableOnRecoveryErrorHandler();
            using var __ = options.DisableOnIntegrityErrorOfAlreadySyncedDataHandler();
            
            // now need to verify if there are any _valid_ transactions after we found an invalid one
            var original4KbPosition = _readAt4Kb;
            for (; _readAt4Kb < _journalPagerNumberOfAllocated4Kb; _readAt4Kb++)
            {
                if (TryValidateTransaction(options, ref txState, out var current) is false)
                    continue;
                
                if (current->JournalId == JournalId ||
                    current->JournalId == Guid.Empty && Legacy_IsOldTransactionFromRecycledJournal(current))
                {
                    // This is a valid transaction, but if all the transactions *up to it* are sync-ed, then we know that we can 
                    // ignore corrupted journal, the data is already on the data file
                    if (CanIgnoreDataIntegrityErrorBecauseTxWasSynced(current->TransactionId - 1, options))
                        return true;
                }
                else
                {
                    // not my transaction, so can skip it
                    _readAt4Kb += GetTransactionSizeIn4Kb(current) - 1;
                    continue;
                }
                
                RequireHeaderUpdate = true;
                ThrowUnexpectedValidTransaction(current);
            }
            return false;

            void ThrowUnexpectedValidTransaction(TransactionHeader* current)
            {
                var message =
                    $"Got invalid transaction at position {original4KbPosition * 4 * Constants.Size.Kilobyte} when reading journal {_journalPager}. ";

                if (LastTransactionHeader != null)
                    message += $"Last read transaction was:{Environment.NewLine}{LastTransactionHeader->ToString()}.{Environment.NewLine}";

                message +=
                    $"Although further reading found a valid transaction at position {_readAt4Kb * 4 * Constants.Size.Kilobyte}:{Environment.NewLine}{current->ToString()}.{Environment.NewLine}" +
                    "Journal file is likely to be corrupted.";

                throw new InvalidJournalException(message, _journalInfo);
            }
        }

        public Guid JournalId;
        private RavenLogger _log;

        private bool CanIgnoreDataIntegrityErrorBecauseTxWasSynced(long transactionId, StorageEnvironmentOptions options)
        {
            // if we have a journal which contains transactions that has been synced and this is the case for current transaction 
            // then we can continue the recovery regardless encountered errors

            return options.IgnoreDataIntegrityErrorsOfAlreadySyncedTransactions &&
                   IsAlreadySyncTransaction(transactionId);
        }
        
        private bool Legacy_IsOldTransactionFromRecycledJournal(TransactionHeader* currentTx)
        {
            if (_firstValidTransactionHeader != null && currentTx->TransactionId < _firstValidTransactionHeader->TransactionId)
                return true;

            if (LastTransactionHeader != null && currentTx->TransactionId < LastTransactionHeader->TransactionId)
                return true;

            return false;
        }

        private bool ValidatePagesHash(StorageEnvironmentOptions options, TransactionHeader* current)
        {
            byte* dataPtr = (byte*)current + sizeof(TransactionHeader);

            var size = current->CompressedSize != -1 ? current->CompressedSize : current->UncompressedSize;
            if (size < 0)
            {
                if (CanIgnoreDataIntegrityErrorBecauseTxWasSynced(current->TransactionId, options) == false)
                {
                    RequireHeaderUpdate = true;
                    // negative size is not supported
                    options.InvokeRecoveryError(this, $"Compresses size {current->CompressedSize} is negative", null);
                }

                return false;
            }

            if (size > (_journalPagerNumberOfAllocated4Kb - _readAt4Kb) * 4 * Constants.Size.Kilobyte)
            {
                if (CanIgnoreDataIntegrityErrorBecauseTxWasSynced(current->TransactionId, options) == false)
                {
                    // we can't read past the end of the journal
                    RequireHeaderUpdate = true;
                    var compressLabel = (current->CompressedSize != -1) ? "Compressed" : "Uncompressed";
                    options.InvokeRecoveryError(this,
                        $"Size {size} ({compressLabel}) is too big for the journal size {_journalPagerNumberOfAllocated4Kb * 4 * Constants.Size.Kilobyte}", null);
                }

                return false;
            }

            ulong hash = Hashing.XXHash64.Calculate(dataPtr, (ulong)size, (ulong)current->TransactionId);
            if (hash != current->Hash)
            {
                if (CanIgnoreDataIntegrityErrorBecauseTxWasSynced(current->TransactionId, options) == false)
                {
                    RequireHeaderUpdate = true;
                    options.InvokeRecoveryError(this, "Invalid hash signature for transaction: " + current->ToString(), null);
                }

                return false;
            }

            return true;
        }

        public override string ToString()
        {
            return _journalPager.ToString();
        }

        public void Complete(ref Pager.State state, ref Pager.PagerTransactionState txState)
        {
            if (_encryptionBuffers != null) // Encryption enabled
            {
                foreach (var buffer in _encryptionBuffers)
                    PlatformSpecific.NativeMemory.Free4KbAlignedMemory(buffer.Pointer, buffer.Size, buffer.AllocatingThread);

                if (txState.ForCrypto?.TryGetValue(_dataPager, out var cryptoState) == true)
                {
                    // we need to iterate from the end in order to filter out pages that was overwritten by later transaction
                    var sortedState = cryptoState.OrderByDescending(x => x.Key);

                    var overflowDetector = new RecoveryOverflowDetector();

                    foreach (var buffer in sortedState)
                    {
                        if (buffer.Value.SkipOnTxCommit)
                            continue;

                        if (buffer.Value.Modified == false)
                            continue; // No modification

                        var pageHeader = (PageHeader*)buffer.Value.Pointer;
                        var numberOfPages = Paging.Paging.GetNumberOfPages(pageHeader);

                        long modifiedPage = buffer.Key;

                        if (overflowDetector.IsOverlappingAnotherPage(modifiedPage, numberOfPages))
                        {
                            // if page is overlapping an already seen page it means this one was freed, we must skip it on tx commit
                            cryptoState[modifiedPage].SkipOnTxCommit = true;
                            continue;
                        }

                        overflowDetector.SetPageChecked(modifiedPage);
                    }
                }
            }

            txState.InvokeBeforeCommitFinalization(_environment, ref state, ref txState);
            txState.InvokeDispose(_environment, ref state, ref txState);
        }

        private static int GetNumberOfPagesFor(long size)
        {
            return checked((int)(size / Constants.Storage.PageSize) + (size % Constants.Storage.PageSize == 0 ? 0 : 1));
        }

        private static long GetNumberOf4KbFor(long size)
        {
            return checked(size / (4 * Constants.Size.Kilobyte) + (size % (4 * Constants.Size.Kilobyte) == 0 ? 0 : 1));
        }

        private string AddSkipTxInfoDetails()
        {
            var details = string.Empty;

            if (_firstSkippedTx != null)
                details += $"first skipped tx - {_firstSkippedTx}";

            if (_lastSkippedTx != null)
                details += $", last skipped tx - {_lastSkippedTx}";

            return details != string.Empty ? $" ({details})" : string.Empty;
        }
    }
}

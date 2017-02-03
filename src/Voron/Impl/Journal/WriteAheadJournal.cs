// -----------------------------------------------------------------------
//  <copyright file="WriteAheadJournal.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using Sparrow;
using Sparrow.Binary;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading;
using Sparrow.Compression;
using Sparrow.Logging;
using Sparrow.Utils;
using Voron.Data;
using Voron.Exceptions;
using Voron.Impl.FileHeaders;
using Voron.Impl.Paging;
using Voron.Util;
using Voron.Global;

namespace Voron.Impl.Journal
{
    public unsafe class WriteAheadJournal : IDisposable
    {
        private static readonly TimeSpan Infinity = TimeSpan.FromMilliseconds(-1);

        private readonly StorageEnvironment _env;
        private readonly AbstractPager _dataPager;

        private long _currentJournalFileSize;
        private DateTime _lastFile;

        private long _journalIndex = -1;

        private bool _disposed;

        private readonly JournalApplicator _journalApplicator;
        private readonly ModifyHeaderAction _updateLogInfo;

        private ImmutableAppendOnlyList<JournalFile> _files = ImmutableAppendOnlyList<JournalFile>.Empty;
        internal JournalFile CurrentFile;

        private readonly HeaderAccessor _headerAccessor;
        private AbstractPager _compressionPager;
        private long _compressionPagerCounter;

        private LazyTransactionBuffer _lazyTransactionBuffer;
        private readonly DiffPages _diffPage = new DiffPages();
        private readonly Logger _logger;
        private List<JournalSnapshot> _snapshotCache;
        public bool HasDataInLazyTxBuffer() => _lazyTransactionBuffer?.HasDataInBuffer() ?? false;

        private readonly object _writeLock = new object();
        private int _maxNumberOfPagesRequiredForCompressionBuffer;

        public WriteAheadJournal(StorageEnvironment env)
        {
            _env = env;
            _logger = LoggingSource.Instance.GetLogger<WriteAheadJournal>(Path.GetFileName(env.ToString()));
            _dataPager = _env.Options.DataPager;
            _currentJournalFileSize = env.Options.InitialLogFileSize;
            _headerAccessor = env.HeaderAccessor;
            _updateLogInfo = header =>
            {
                var journalFilesCount = _files.Count;
                var currentJournal = journalFilesCount > 0 ? _journalIndex : -1;
                header->Journal.CurrentJournal = currentJournal;
                header->Journal.JournalFilesCount = journalFilesCount;
                header->IncrementalBackup.LastCreatedJournal = _journalIndex;
            };

            _compressionPager = CreateCompressionPager(_env.Options.InitialFileSize ?? _env.Options.InitialLogFileSize);
            _journalApplicator = new JournalApplicator(this);
        }

        public ImmutableAppendOnlyList<JournalFile> Files => _files;

        public JournalApplicator Applicator => _journalApplicator;

        public bool HasLazyTransactions { get; set; }

        private JournalFile NextFile(int numberOf4kbs = 1)
        {
            var now = DateTime.UtcNow;
            if ((now - _lastFile).TotalSeconds < 90)
            {
                _currentJournalFileSize = Math.Min(_env.Options.MaxLogFileSize, _currentJournalFileSize * 2);
            }
            var actualLogSize = _currentJournalFileSize;
            long minRequiredSize = numberOf4kbs * 4 * Constants.Size.Kilobyte;
            if (_currentJournalFileSize < minRequiredSize)
            {
                _currentJournalFileSize = Bits.NextPowerOf2(minRequiredSize);
                actualLogSize = _currentJournalFileSize;
            }
       
            var journalPager = _env.Options.CreateJournalWriter(_journalIndex+1, actualLogSize);

            // we modify the in memory state _after_ we created the file, because we have to make sure that 
            // we have created it successfully first. 
            _journalIndex++; 

            _lastFile = now;

            var journal = new JournalFile(_env, journalPager, _journalIndex);
            journal.AddRef(); // one reference added by a creator - write ahead log

            _files = _files.Append(journal);

            _headerAccessor.Modify(_updateLogInfo);

            return journal;
        }

        public bool RecoverDatabase(TransactionHeader* txHeader)
        {
            // note, we don't need to do any concurrency here, happens as a single threaded
            // fashion on db startup
            var requireHeaderUpdate = false;

            var logInfo = _headerAccessor.Get(ptr => ptr->Journal);

            if (logInfo.JournalFilesCount == 0)
            {
                _journalIndex = logInfo.LastSyncedJournal;
                return false;
            }

            var oldestLogFileStillInUse = logInfo.CurrentJournal - logInfo.JournalFilesCount + 1;
            if (_env.Options.IncrementalBackupEnabled == false)
            {
                // we want to check that we cleanup old log files if they aren't needed
                // this is more just to be safe than anything else, they shouldn't be there.
                var unusedfiles = oldestLogFileStillInUse;
                while (true)
                {
                    unusedfiles--;
                    if (_env.Options.TryDeleteJournal(unusedfiles) == false)
                        break;
                }

            }

            var lastSyncedTransactionId = logInfo.LastSyncedTransactionId;

            var journalFiles = new List<JournalFile>();
            long lastSyncedTxId = -1;
            long lastSyncedJournal = logInfo.LastSyncedJournal;
            for (var journalNumber = oldestLogFileStillInUse; journalNumber <= logInfo.CurrentJournal; journalNumber++)
            {
                var initialSize = _env.Options.InitialFileSize ?? _env.Options.InitialLogFileSize;
                var journalRecoveryName = StorageEnvironmentOptions.JournalRecoveryName(journalNumber);
                using (var recoveryPager = _env.Options.CreateScratchPager(journalRecoveryName, initialSize))
                using (var pager = _env.Options.OpenJournalPager(journalNumber))
                {
                    RecoverCurrentJournalSize(pager);

                    var transactionHeader = txHeader->TransactionId == 0 ? null : txHeader;
                    using (
                        var journalReader = new JournalReader(pager, _dataPager, recoveryPager, lastSyncedTransactionId,
                            transactionHeader))
                    {
                        journalReader.RecoverAndValidate(_env.Options);

                        var lastReadHeaderPtr = journalReader.LastTransactionHeader;

                        if (lastReadHeaderPtr != null)
                        {
                            *txHeader = *lastReadHeaderPtr;
                            lastSyncedTxId = txHeader->TransactionId;
                            lastSyncedJournal = journalNumber;
                        }

                        pager.Dispose(); // need to close it before we open the journal writer

                        if (lastSyncedTxId != -1 && (journalReader.RequireHeaderUpdate || journalNumber == logInfo.CurrentJournal))
                        {
                            var jrnlWriter = _env.Options.CreateJournalWriter(journalNumber,
                                pager.NumberOfAllocatedPages * Constants.Storage.PageSize);
                            var jrnlFile = new JournalFile(_env, jrnlWriter, journalNumber);
                            jrnlFile.InitFrom(journalReader);
                            jrnlFile.AddRef(); // creator reference - write ahead log

                            journalFiles.Add(jrnlFile);
                        }

                        if (journalReader.RequireHeaderUpdate) //this should prevent further loading of transactions
                        {
                            requireHeaderUpdate = true;
                            break;
                        }
                    }

                }
            }

            _files = _files.AppendRange(journalFiles);

            if (lastSyncedTxId < 0)
                VoronUnrecoverableErrorException.Raise(_env,
                    "First transaction initializing the structure of Voron database is corrupted. Cannot access internal database metadata. Create a new database to recover.");

            Debug.Assert(lastSyncedTxId >= 0);
            Debug.Assert(lastSyncedJournal >= 0);

            _journalIndex = lastSyncedJournal;

            _headerAccessor.Modify(
                header =>
                {
                    header->Journal.CurrentJournal = lastSyncedJournal;
                    header->Journal.JournalFilesCount = _files.Count;
                    header->IncrementalBackup.LastCreatedJournal = _journalIndex;
                });

            CleanupInvalidJournalFiles(lastSyncedJournal);
            CleanupUnusedJournalFiles(oldestLogFileStillInUse, lastSyncedJournal);

            if (_files.Count > 0)
            {
                var lastFile = _files.Last();
                if (lastFile.Available4Kbs >= 2)
                    // it must have at least one page for the next transaction header and one 4kb for data
                    CurrentFile = lastFile;
            }

            return requireHeaderUpdate;
        }

        private void CleanupUnusedJournalFiles(long oldestLogFileStillInUse, long lastSyncedJournal)
        {
            var logFile = oldestLogFileStillInUse;
            while (logFile < lastSyncedJournal)
            {
                _env.Options.TryDeleteJournal(logFile);
                logFile++;
            }
        }

        private void CleanupInvalidJournalFiles(long lastSyncedJournal)
        {
            // we want to check that we cleanup newer log files, since everything from
            // the current file is considered corrupted
            var badJournalFiles = lastSyncedJournal;
            while (true)
            {
                badJournalFiles++;
                if (_env.Options.TryDeleteJournal(badJournalFiles) == false)
                {
                    break;
                }
            }
        }

        private void RecoverCurrentJournalSize(AbstractPager pager)
        {
            var journalSize = Bits.NextPowerOf2(pager.NumberOfAllocatedPages * Constants.Storage.PageSize);
            if (journalSize >= _env.Options.MaxLogFileSize) // can't set for more than the max log file size
                return;

            // this set the size of the _next_ journal file size
            _currentJournalFileSize = Math.Min(journalSize, _env.Options.MaxLogFileSize);
        }


        public Page? ReadPage(LowLevelTransaction tx, long pageNumber, Dictionary<int, PagerState> scratchPagerStates, LowLevelTransaction.PagerRef pagerRef)
        {
            // read transactions have to read from journal snapshots
            if (tx.Flags == TransactionFlags.Read)
            {
                // read log snapshots from the back to get the most recent version of a page
                for (var i = tx.JournalSnapshots.Count - 1; i >= 0; i--)
                {
                    PagePosition value;
                    if (tx.JournalSnapshots[i].PageTranslationTable.TryGetValue(tx, pageNumber, out value))
                    {
                        var page = _env.ScratchBufferPool.ReadPage(tx, value.ScratchNumber, value.ScratchPos, scratchPagerStates[value.ScratchNumber], pagerRef);

                        Debug.Assert(page.PageNumber == pageNumber);

                        return page;
                    }
                }

                return null;
            }

            // write transactions can read directly from journals that they got when they started up
            var files = tx.JournalFiles;
            for (var i = files.Count - 1; i >= 0; i--)
            {
                PagePosition value;
                if (files[i].PageTranslationTable.TryGetValue(tx, pageNumber, out value))
                {
                    var page = _env.ScratchBufferPool.ReadPage(tx, value.ScratchNumber, value.ScratchPos, pagerRef: pagerRef);

                    Debug.Assert(page.PageNumber == pageNumber);

                    return page;
                }
            }

            return null;
        }


        public void Dispose()
        {
            if (_disposed)
                return;
            _disposed = true;

            // we cannot dispose the journal until we are done with all of the pending writes
            if (_lazyTransactionBuffer != null)
            {
                _lazyTransactionBuffer.WriteBufferToFile(CurrentFile, null);
                _lazyTransactionBuffer.Dispose();
            }
            _compressionPager.Dispose();

            _journalApplicator.Dispose();
            if (_env.Options.OwnsPagers)
            {
                foreach (var logFile in _files)
                {
                    logFile.Dispose();
                }

            }
            else
            {
                foreach (var logFile in _files)
                {
                    GC.SuppressFinalize(logFile);
                }

            }

            _files = ImmutableAppendOnlyList<JournalFile>.Empty;
        }

        public JournalInfo GetCurrentJournalInfo()
        {
            return _headerAccessor.Get(ptr => ptr->Journal);
        }

        public List<JournalSnapshot> GetSnapshots()
        {
            return _snapshotCache;
        }

        public void UpdateCacheForJournalSnapshots()
        {
            var items = new List<JournalSnapshot>(_files.Count);
            // ReSharper disable once LoopCanBeConvertedToQuery
            foreach (var journalFile in _files)
            {
                var journalSnapshot = journalFile.GetSnapshot();
                // we have to hold a reference to the journals for the lifetime of the cache
                // this call is prevented from running concurrently with GetSnapshots()
                journalSnapshot.FileInstance.AddRef();
                items.Add(journalSnapshot);
            }

            ValidateNoDuplicateJournals(items);

            var old = _snapshotCache;
            _snapshotCache = items;
            if (old == null)
                return;

            foreach (var journalSnapshot in old)
            {
                journalSnapshot.FileInstance.Release();// free the old cache reference
            }
        }

        [Conditional("DEBUG")]
        private static void ValidateNoDuplicateJournals(List<JournalSnapshot> items)
        {
            for (int i = 0; i < items.Count; i++)
            {
                for (int j = i + 1; j < items.Count; j++)
                {
                    if (items[i].Number == items[j].Number)
                    {
                        throw new InvalidOperationException("Cannot add a snapshot of log file with number " + items[i].Number +
                                                            " to the transaction, because it already exists in a snapshot collection");
                    }
                }
            }
        }

        public void Clear(LowLevelTransaction tx)
        {
            if (tx.Flags != TransactionFlags.ReadWrite)
                throw new InvalidOperationException("Clearing of write ahead journal should be called only from a write transaction");

            foreach (var journalFile in _files)
            {
                journalFile.Release();
            }
            _files = ImmutableAppendOnlyList<JournalFile>.Empty;
            CurrentFile = null;
        }



        public class JournalSyncEventArgs : EventArgs
        {
            public long OldestTransactionId { get; private set; }

            public JournalSyncEventArgs(long oldestTransactionId)
            {
                OldestTransactionId = oldestTransactionId;
            }
        }

        public class JournalApplicator : IDisposable
        {
            private readonly Dictionary<long, JournalFile> _journalsToDelete = new Dictionary<long, JournalFile>();
            private readonly object _flushingLock = new object();
            private readonly object _fsyncLock = new object();
            private readonly WriteAheadJournal _waj;

            private long _lastFlushedTransactionId;
            private long _lastFlushedJournalId;
            private long _lastSyncJournalId;
            private JournalFile _lastFlushedJournal;
            private bool _ignoreLockAlreadyTaken;
            private long _totalWrittenButUnsyncedBytes;
            private DateTime _lastSyncTime;

            public long TotalWrittenButUnsyncedBytes => Volatile.Read(ref _totalWrittenButUnsyncedBytes);

            public JournalApplicator(WriteAheadJournal waj)
            {
                _waj = waj;
            }


            public void ApplyLogsToDataFile(CancellationToken token, TimeSpan timeToWait)
            {
                if (token.IsCancellationRequested)
                    return;

                if (Monitor.IsEntered(_flushingLock) && _ignoreLockAlreadyTaken == false)
                    throw new InvalidJournalFlushRequestException("Applying journals to the data file has been already requested on the same thread");

                bool lockTaken = false;
                try
                {
                    Monitor.TryEnter(_flushingLock, timeToWait, ref lockTaken);

                    if (lockTaken == false)
                    {
                        if (timeToWait == TimeSpan.Zero)
                            // someone else is flushing, and we were explicitly told that we don't care about this
                            // so there is no point in throwing
                            return;

                        throw new TimeoutException(
                            $"Could not acquire the write lock in {timeToWait.TotalSeconds} seconds");
                    }

                    if (_waj._env.Disposed)
                        return;


                    var jrnls = GetJournalSnapshots();

                    if (jrnls.Count == 0)
                        return; // nothing to do

                    Debug.Assert(jrnls.First().Number >= _lastFlushedJournalId);

                    var pagesToWrite = new Dictionary<long, PagePosition>();

                    long lastProcessedJournal = -1;
                    long previousJournalMaxTransactionId = -1;

                    long lastFlushedTransactionId = -1;

                    long oldestActiveTransaction = _waj._env.ActiveTransactions.OldestTransaction;

                    foreach (var journalFile in jrnls)
                    {
                        if (journalFile.Number < _lastFlushedJournalId)
                            continue;
                        var currentJournalMaxTransactionId = -1L;

                        var maxTransactionId = journalFile.LastTransaction;
                        if (oldestActiveTransaction != 0)
                            maxTransactionId = Math.Min(oldestActiveTransaction - 1, maxTransactionId);

                        foreach (var modifedPagesInTx in journalFile.PageTranslationTable.GetModifiedPagesForTransactionRange(
                            _lastFlushedTransactionId, maxTransactionId))
                        {
                            foreach (var pagePosition in modifedPagesInTx)
                            {
                                if (pagePosition.Value.IsFreedPageMarker)
                                {
                                    // Avoid the case where an older journal file had written a page that was freed in a different journal
                                    pagesToWrite.Remove(pagePosition.Key);
                                    continue;
                                }

                                if (journalFile.Number == _lastFlushedJournalId &&
                                    pagePosition.Value.TransactionId <= _lastFlushedTransactionId)
                                    continue;

                                currentJournalMaxTransactionId = Math.Max(currentJournalMaxTransactionId,
                                    pagePosition.Value.TransactionId);

                                if (currentJournalMaxTransactionId < previousJournalMaxTransactionId)
                                    ThrowReadByeondOldestActiveTransaction(currentJournalMaxTransactionId, previousJournalMaxTransactionId, oldestActiveTransaction);


                                lastProcessedJournal = journalFile.Number;
                                pagesToWrite[pagePosition.Key] = pagePosition.Value;

                                lastFlushedTransactionId = currentJournalMaxTransactionId;
                            }
                        }

                        if (currentJournalMaxTransactionId == -1L)
                            continue;

                        previousJournalMaxTransactionId = currentJournalMaxTransactionId;
                    }

                    if (pagesToWrite.Count == 0)
                    {
                        return;
                    }

                    try
                    {
                        ApplyPagesToDataFileFromScratch(pagesToWrite);
                    }
                    catch (OutOfMemoryException e)
                    {
                        if (_waj._logger.IsOperationsEnabled)
                        {
                            _waj._logger.Operations("Could not allocate enough space to apply pages to data file", e);
                        }
                        // on 32 bits systems, we likely run out of address space, nothing that we can do, this should
                        // be handled by the 32 bits pager.
                        return;
                    }
                    catch (DiskFullException diskFullEx)
                    {
                        if (_waj._logger.IsOperationsEnabled)
                        {
                            _waj._logger.Operations("The disk is full!", diskFullEx);
                        }
                        _waj._env.HandleDataDiskFullException(diskFullEx);
                        return;
                    }

                    var unusedJournals = GetUnusedJournalFiles(jrnls, lastProcessedJournal, lastFlushedTransactionId);

                    _waj._env.FlushInProgressLock.EnterWriteLock();
                    try
                    {
                        var transactionPersistentContext = new TransactionPersistentContext(true);

                        TimeSpan? timeout;

                        if (pagesToWrite.Count < _waj._env.Options.MaxNumberOfPagesInJournalBeforeFlush)
                            timeout = null;
                        else
                            timeout = Infinity;

                        using (var txw = _waj._env.NewLowLevelTransaction(transactionPersistentContext, TransactionFlags.ReadWrite, timeout: timeout))
                        {
                            _lastFlushedJournalId = lastProcessedJournal;
                            _lastFlushedTransactionId = lastFlushedTransactionId;
                            _lastFlushedJournal = _waj._files.First(x => x.Number == lastProcessedJournal);

                            foreach (var unused in unusedJournals)
                            {
                                _journalsToDelete[unused.Number] = unused;
                            }

                            if (unusedJournals.Count > 0)
                            {
                                var lastUnusedJournalNumber = unusedJournals[unusedJournals.Count - 1].Number;
                                _waj._files = _waj._files.RemoveWhile(x => x.Number <= lastUnusedJournalNumber);
                            }

                            if (_waj._files.Count == 0)
                                _waj.CurrentFile = null;

                            FreeScratchPages(unusedJournals, txw);

                            // by forcing a commit, we free the read transaction that held the lazy tx buffer (if existed)
                            // and make those pages available in the scratch files
                            txw.IsLazyTransaction = false;
                            _waj.HasLazyTransactions = false;

                            txw.Commit();
                        }
                    }
                    finally
                    {
                        _waj._env.FlushInProgressLock.ExitWriteLock();
                    }

                    QueueDataFileSync();
                }
                finally
                {
                    if (lockTaken)
                        Monitor.Exit(_flushingLock);
                }

                _waj._env.LogsApplied();
            }

            private static void ThrowReadByeondOldestActiveTransaction(long currentJournalMaxTransactionId,
                long previousJournalMaxTransactionId, long oldestActiveTransaction)
            {
                throw new InvalidOperationException(
                    "Journal applicator read beyond the oldest active transaction in the next journal file. " +
                    "This should never happen. Current journal max tx id: " +
                    currentJournalMaxTransactionId +
                    ", previous journal max ix id: " + previousJournalMaxTransactionId +
                    ", oldest active transaction: " + oldestActiveTransaction);
            }

            private List<JournalSnapshot> GetJournalSnapshots()
            {
                var files = _waj._files;
                var jrnls = new List<JournalSnapshot>(files.Count);
                foreach (var file in files)
                {
                    jrnls.Add(file.GetSnapshot());
                }
                jrnls.Sort();
                return jrnls;
            }

            private void QueueDataFileSync()
            {
                if (_totalWrittenButUnsyncedBytes > 32 * Constants.Size.Megabyte)
                {
                    if (_waj._logger.IsInfoEnabled)
                        _waj._logger.Info(
                            $"Asking for required sync on {_waj._dataPager.FileName} because there are {_totalWrittenButUnsyncedBytes / 1024:#,#} kb writtern & unsynced");
                    _waj._env.ForceSyncDataFile();
                }
                else
                {
                    _waj._env.QueueForSyncDataFile();
                }
            }

            public void WaitForSyncToCompleteOnDispose()
            {
                if (Monitor.IsEntered(_flushingLock) == false)
                    throw new InvalidOperationException("This method can only be called while holding the flush lock");

                if (_waj._env.Disposed == false)
                    throw new InvalidOperationException(
                        "This method can only be called after the storage environment has been disposed");

                if (Monitor.TryEnter(_fsyncLock))
                {
                    Monitor.Exit(_fsyncLock);
                    return;
                }

                // now the sync lock is in progress, but it can't complete because we are holding the flush lock
                // we'll first give the flush lock and then wait on the fsync lock until the sync is completed
                // then we'll re-aqcuire the flush lock

                Monitor.Exit(_flushingLock);
                try
                {
                    // we wait to take the lock here to ensure that all previous sync operations
                    // has completed, and we know that no new ones can start
                    Monitor.Enter(_fsyncLock);
                    try
                    {
                        // now we know that the sync is done
                        // we also know that no other sync can start now
                        // because Disposed is set to true
                    }
                    finally
                    {
                        Monitor.Exit(_fsyncLock);
                    }
                }
                finally
                {
                    Monitor.Enter(_flushingLock);// reacquire the lock
                }

            }

            public void SyncDataFile()
            {
                // This function can take a LONG time, and it needs to run concurrently with the
                // rest of the system, so in order to handle this properly, we do:
                // 1) Take the flushing lock (if we fail, we'll requeue for the sync)
                // 2) Take a snapshot of the current status of this env flushing status
                // 3) Release the lock & sync the file (take a long time)
                // 4) Re-take the lock, update the sync status in the header with the values we snapshotted
                bool fsyncLockTaken = false;
                try
                {
                    long lastSyncedJournal;
                    long currentTotalWrittenBytes;
                    long lastSyncedTransactionId;
                    var journalsToDelete = new List<KeyValuePair<long, JournalFile>>();
                    bool flushLockTaken = false;
                    // this is a pointer because we need to pass the value to a lambda
                    // inside UpdateFileHeaderAfterDataFileSync, so we just allocate it here.
                    // We need those values to be the frozen value at the time we _started_ the
                    // sync process
                    TransactionHeader* lastReadTxHeader = stackalloc TransactionHeader[1];
                    try
                    {
                        Monitor.TryEnter(_flushingLock, TimeSpan.FromMilliseconds(250), ref flushLockTaken);

                        if (flushLockTaken == false)
                        {
                            // can't get the lock, we'll try again later, this time we are running
                            // as forced, because we have higher priority
                            if (_waj._logger.IsInfoEnabled)
                                _waj._logger.Info(
                                    $"Asking for required sync on {_waj._dataPager.FileName} because started a sync and aborted because we couldn't get the flushing lock");
                            _waj._env.ForceSyncDataFile();
                            return;
                        }

                        if (_waj._env.Disposed)
                            return; // we have already disposed, nothing to do here

                        if (_lastFlushedJournal == null)
                            // nothing was flushed since we last synced, nothing to do
                            return;

                        // we only ever take the _fsyncLock _after_ we already took the flush lock
                        // so this will never be contended
                        Monitor.TryEnter(_fsyncLock, ref fsyncLockTaken);
                        if (fsyncLockTaken == false)
                        {
                            // probably another sync taking place right now, let us schedule another one, just in case
                            _waj._env.QueueForSyncDataFile();
                            return;
                        }
                        currentTotalWrittenBytes = _totalWrittenButUnsyncedBytes;
                        lastSyncedJournal = _lastFlushedJournalId;
                        lastSyncedTransactionId = _lastFlushedTransactionId;
                        SetLastReadTxHeader(_lastFlushedJournal, lastSyncedTransactionId, lastReadTxHeader);
                        if (lastSyncedTransactionId != lastReadTxHeader->TransactionId)
                        {
                            VoronUnrecoverableErrorException.Raise(_waj._env,
                                $"Error syncing the data file. The last sync tx is {lastSyncedTransactionId}, but the journal's last tx id is {lastReadTxHeader->TransactionId}, possible file corruption?"
                            );
                        }

                        _lastFlushedJournal = null;
                        _lastSyncTime = DateTime.UtcNow;
                        _lastSyncJournalId = lastSyncedJournal;

                        foreach (var toDelete in _journalsToDelete)
                        {
                            if (toDelete.Key > lastSyncedJournal)
                                continue;

                            journalsToDelete.Add(toDelete);
                        }
                        foreach (var kvp in journalsToDelete)
                        {
                            _journalsToDelete.Remove(kvp.Key);
                        }
                    }
                    finally
                    {
                        if (flushLockTaken)
                            Monitor.Exit(_flushingLock);
                    }

                    // danger mode assumes that no OS crashes can happen, in order to get best performance

                    if (_waj._env.Options.TransactionsMode != TransactionsMode.Danger)
                    {
                        // We do the sync _outside_ of the lock, letting the rest of the stuff proceed
                        var sp = Stopwatch.StartNew();
                        _waj._dataPager.Sync();
                        if (_waj._logger.IsInfoEnabled)
                        {
                            var sizeInKb = (_waj._dataPager.NumberOfAllocatedPages * Constants.Storage.PageSize) / Constants.Size.Kilobyte;
                            _waj._logger.Info($"Sync of {sizeInKb:#,#} kb file with {currentTotalWrittenBytes / Constants.Size.Kilobyte:#,#} kb dirty in {sp.Elapsed}");
                        }

                    }


                    lock (_flushingLock)
                    {
                        _totalWrittenButUnsyncedBytes -= currentTotalWrittenBytes;
                        UpdateFileHeaderAfterDataFileSync(lastSyncedJournal, lastSyncedTransactionId, lastReadTxHeader);

                        foreach (var toDelete in journalsToDelete)
                        {
                            if (_waj._env.Options.IncrementalBackupEnabled == false)
                                toDelete.Value.DeleteOnClose = true;

                            toDelete.Value.Release();
                        }
                    }
                }
                finally
                {
                    if (fsyncLockTaken)
                        Monitor.Exit(_fsyncLock);
                }
            }

            private void ApplyPagesToDataFileFromScratch(Dictionary<long, PagePosition> pagesToWrite)
            {
                var scratchBufferPool = _waj._env.ScratchBufferPool;
                var scratchPagerStates = new Dictionary<int, PagerState>();

                try
                {
                    long written = 0;
                    var sp = Stopwatch.StartNew();
                    using (var meter = _waj._dataPager.Options.IoMetrics.MeterIoRate(_waj._dataPager.FileName, IoMetrics.MeterType.DataFlush, 0))
                    {
                        using (var batchWrites = _waj._dataPager.BatchWriter())
                        {
                            foreach (var pagePosition in pagesToWrite.Values)
                            {
                                var scratchNumber = pagePosition.ScratchNumber;
                                PagerState pagerState;
                                if (scratchPagerStates.TryGetValue(scratchNumber, out pagerState) == false)
                                {
                                    pagerState = scratchBufferPool.GetPagerState(scratchNumber);
                                    pagerState.AddRef();

                                    scratchPagerStates.Add(scratchNumber, pagerState);
                                }

                                var numberOfPages = scratchBufferPool.CopyPage(
                                    batchWrites,
                                    scratchNumber,
                                    pagePosition.ScratchPos,
                                    pagerState);

                                written += numberOfPages * Constants.Storage.PageSize;
                            }
                        }

                        meter.IncrementSize(written);
                    }

                    if (_waj._logger.IsInfoEnabled)
                        _waj._logger.Info($"Flushed {pagesToWrite.Count:#,#} pages to { _waj._dataPager.FileName} with {written / Constants.Size.Kilobyte:#,#} kb in {sp.Elapsed}");

                    _totalWrittenButUnsyncedBytes += written;
                }
                finally
                {
                    foreach (var scratchPagerState in scratchPagerStates.Values)
                    {
                        scratchPagerState.Release();
                    }
                }
            }

            private void FreeScratchPages(IEnumerable<JournalFile> unusedJournalFiles, LowLevelTransaction txw)
            {
                // we release up to the last read transaction, because there might be new read transactions that are currently
                // running, that started after the flush
                var lastSyncedTransactionId = Math.Min(_lastFlushedTransactionId, _waj._env.CurrentReadTransactionId - 1);

                // we have to free pages of the unused journals before the remaining ones that are still in use
                // to prevent reading from them by any read transaction (read transactions search journals from the newest
                // to read the most updated version)


                foreach (var journalFile in unusedJournalFiles.OrderBy(x => x.Number))
                {
                    journalFile.FreeScratchPagesOlderThan(txw, lastSyncedTransactionId);
                }

                foreach (var jrnl in _waj._files.OrderBy(x => x.Number))
                {
                    jrnl.FreeScratchPagesOlderThan(txw, lastSyncedTransactionId);
                }
            }

            private List<JournalFile> GetUnusedJournalFiles(IEnumerable<JournalSnapshot> jrnls, long lastProcessedJournal, long lastFlushedTransactionId)
            {
                var unusedJournalFiles = new List<JournalFile>();
                foreach (var j in jrnls)
                {
                    if (j.Number > lastProcessedJournal) // after the last log we synced, nothing to do here
                        continue;
                    if (j.Number == lastProcessedJournal) // we are in the last log we synced
                    {
                        if (j.Available4Kbs != 0 || //　if there are more pages to be used here or
                        j.PageTranslationTable.MaxTransactionId() != lastFlushedTransactionId) // we didn't synchronize whole journal
                            continue; // do not mark it as unused
                    }
                    unusedJournalFiles.Add(_waj._files.First(x => x.Number == j.Number));
                }
                return unusedJournalFiles;
            }

            private void UpdateFileHeaderAfterDataFileSync(long lastSyncedJournal,
                long lastSyncedTransactionId, TransactionHeader* lastReadTxHeader)
            {
                Debug.Assert(lastSyncedJournal != -1);
                Debug.Assert(lastSyncedTransactionId != -1);

                _waj._headerAccessor.Modify(header =>
                {
                    header->TransactionId = lastReadTxHeader->TransactionId;
                    header->LastPageNumber = lastReadTxHeader->LastPageNumber;

                    header->Journal.LastSyncedJournal = lastSyncedJournal;
                    header->Journal.LastSyncedTransactionId = lastSyncedTransactionId;

                    header->Root = lastReadTxHeader->Root;

                    _waj._updateLogInfo(header);
                });
            }

            public void SetLastReadTxHeader(JournalFile file, long maxTransactionId, TransactionHeader* lastReadTxHeader)
            {
                var readTxHeader = stackalloc TransactionHeader[1];
                lastReadTxHeader->TransactionId = -1;
                long txPos = 0;
                while (true)
                {
                    if (file.ReadTransaction(txPos, readTxHeader) == false)
                        break;
                    if (readTxHeader->HeaderMarker != Constants.TransactionHeaderMarker)
                        break;
                    if (readTxHeader->TransactionId > maxTransactionId)
                        break;
                    if (lastReadTxHeader->TransactionId > readTxHeader->TransactionId)
                        // we got to a trasaction that is smaller than the previous one, this is very 
                        // likely a reused jouranl with old transaction, which we can ignore
                        break;

                    *lastReadTxHeader = *readTxHeader;

                    var totalSize = readTxHeader->CompressedSize + sizeof(TransactionHeader);


                    var roundTo4Kb = (totalSize / (4 * Constants.Size.Kilobyte)) +
                                     (totalSize % (4 * Constants.Size.Kilobyte) == 0 ? 0 : 1);

                    // We skip to the next transaction header.
                    txPos += roundTo4Kb * 4 * Constants.Size.Kilobyte;
                }
            }

            public void Dispose()
            {
                foreach (var journalFile in _journalsToDelete)
                {
                    // we need to release all unused journals
                    // however here we don't force them to DeleteOnClose
                    // because we didn't synced the data file yet
                    // and we will need them on a next database recovery
                    journalFile.Value.Release();
                }
            }

            public bool IsCurrentThreadInFlushOperation => Monitor.IsEntered(_flushingLock);

            public IDisposable TryTakeFlushingLock(ref bool lockTaken, TimeSpan? timeout = null)
            {
                if (timeout == null)
                {
                    Monitor.TryEnter(_flushingLock, ref lockTaken);
                }
                else
                {
                    Monitor.TryEnter(_flushingLock, timeout.Value, ref lockTaken);
                }

                bool localLockTaken = lockTaken;

                _ignoreLockAlreadyTaken = true;

                return new DisposableAction(() =>
                {
                    _ignoreLockAlreadyTaken = false;
                    if (localLockTaken)
                        Monitor.Exit(_flushingLock);
                });
            }

            public IDisposable TakeFlushingLock()
            {
                bool lockTaken = false;
                Monitor.Enter(_flushingLock, ref lockTaken);
                _ignoreLockAlreadyTaken = true;

                return new DisposableAction(() =>
                {
                    _ignoreLockAlreadyTaken = false;
                    if (lockTaken)
                        Monitor.Exit(_flushingLock);
                });
            }

            internal void DeleteCurrentAlreadyFlushedJournal()
            {
                if (_waj._env.Options.IncrementalBackupEnabled)
                    return;

                if (_waj._files.Count == 0)
                    return;

                if (_waj._files.Count != 1)
                    throw new InvalidOperationException("Cannot delete current journal because there is more journals being in use");

                var current = _waj._files.First();

                if (current.Number != _lastFlushedJournalId)
                    throw new InvalidOperationException(string.Format("Cannot delete current journal because it isn't last synced file. Current journal number: {0}, the last one which was synced {1}", _waj.CurrentFile?.Number ?? -1, _lastFlushedJournalId));


                if (_waj._env.NextWriteTransactionId - 1 != _lastFlushedTransactionId)
                    throw new InvalidOperationException("Cannot delete current journal because it hasn't synced everything up to the last write transaction");

                _waj._files = _waj._files.RemoveFront(1);
                _waj.CurrentFile = null;

                _waj._headerAccessor.Modify(header => _waj._updateLogInfo(header));

                current.DeleteOnClose = true;
                current.Release();

            }
        }

        public int WriteToJournal(LowLevelTransaction tx, int pageCount)
        {
            lock (_writeLock)
            {
                var sp = Stopwatch.StartNew();
                var journalEntry = PrepareToWriteToJournal(tx, pageCount);
                if (_logger.IsInfoEnabled)
                {
                    _logger.Info($"Preparing to write tx {tx.Id} to jouranl with {journalEntry.NumberOfUncompressedPages:#,#} pages ({(journalEntry.NumberOfUncompressedPages * Constants.Storage.PageSize) / Constants.Size.Kilobyte:#,#} kb) in {sp.Elapsed} with {journalEntry.NumberOf4Kbs / 4:#,#} kb compressed.");
                }

                if (tx.IsLazyTransaction && _lazyTransactionBuffer == null)
                {
                    _lazyTransactionBuffer = new LazyTransactionBuffer(_env.Options);
                }

                if (CurrentFile == null || CurrentFile.Available4Kbs < journalEntry.NumberOf4Kbs)
                {
                    _lazyTransactionBuffer?.WriteBufferToFile(CurrentFile, tx);
                    CurrentFile = NextFile(journalEntry.NumberOf4Kbs);
                    if (_logger.IsInfoEnabled)
                        _logger.Info($"New journal file created {CurrentFile.Number:D19}");
                }

                sp.Restart();
                CurrentFile.Write(tx, journalEntry, _lazyTransactionBuffer, pageCount);

                if (_logger.IsInfoEnabled)
                    _logger.Info($"Writing {journalEntry.NumberOf4Kbs / 4:#,#} kb to journal {CurrentFile.Number:D19} took {sp.Elapsed}");


                if (CurrentFile.Available4Kbs == 0)
                {
                    _lazyTransactionBuffer?.WriteBufferToFile(CurrentFile, tx);
                    CurrentFile = null;
                }

                ReduceSizeOfCompressionBufferIfNeeded();

                return journalEntry.NumberOfUncompressedPages;
            }
        }

        private CompressedPagesResult PrepareToWriteToJournal(LowLevelTransaction tx, int pageCountIncludingAllOverflowPages)
        {
            var txPages = tx.GetTransactionPages();
            var numberOfPages = txPages.Count;

            // We want to include the Transaction Header straight into the compression buffer.
            var sizeOfPagesHeader = numberOfPages * sizeof(TransactionHeaderPageInfo);
            var diffOverhead = sizeOfPagesHeader + (long)numberOfPages * sizeof(long);
            var diffOverheadInPages = checked((int)(diffOverhead / Constants.Storage.PageSize + (diffOverhead % Constants.Storage.PageSize == 0 ? 0 : 1)));
            long maxSizeRequiringCompression = (long)pageCountIncludingAllOverflowPages * (long)Constants.Storage.PageSize + diffOverhead;
            var outputBufferSize = LZ4.MaximumOutputLength(maxSizeRequiringCompression);

            int outputBufferInPages = checked((int)((outputBufferSize + sizeof(TransactionHeader)) / Constants.Storage.PageSize +
                                      ((outputBufferSize + sizeof(TransactionHeader)) % Constants.Storage.PageSize == 0 ? 0 : 1)));

            // The pages required includes the intermediate pages and the required output pages. 
            const int transactionHeaderPageOverhead = 1;
            var pagesRequired = (transactionHeaderPageOverhead + pageCountIncludingAllOverflowPages + diffOverheadInPages + outputBufferInPages);
            _maxNumberOfPagesRequiredForCompressionBuffer = Math.Max(pagesRequired, _maxNumberOfPagesRequiredForCompressionBuffer);
            var pagerState = _compressionPager.EnsureContinuous(0, pagesRequired);
            tx.EnsurePagerStateReference(pagerState);

            _compressionPager.EnsureMapped(tx, 0, pagesRequired);
            var outputBuffer = _compressionPager.AcquirePagePointer(tx, 0);

            var pagesInfo = (TransactionHeaderPageInfo*)outputBuffer;
            var write = outputBuffer + sizeOfPagesHeader;
            var pageSequencialNumber = 0;

            foreach (var txPage in txPages)
            {
                var scratchPage = tx.Environment.ScratchBufferPool.AcquirePagePointer(tx, txPage.ScratchFileNumber, txPage.PositionInScratchBuffer);

                pagesInfo[pageSequencialNumber].PageNumber = ((PageHeader*)scratchPage)->PageNumber;

                *(long*)write = ((PageHeader*)scratchPage)->PageNumber;
                write += sizeof(long);

                _diffPage.Output = write;

                int diffPageSize = txPage.NumberOfPages * Constants.Storage.PageSize;

                if (txPage.PreviousVersion != null)
                {
                    _diffPage.ComputeDiff(txPage.PreviousVersion.Value.Pointer, scratchPage, diffPageSize);
                }
                else
                {
                    _diffPage.ComputeNew(scratchPage, diffPageSize);
                }

                write += _diffPage.OutputSize;
                pagesInfo[pageSequencialNumber].Size = _diffPage.OutputSize == 0 ? 0 : diffPageSize;
                pagesInfo[pageSequencialNumber].DiffSize = _diffPage.IsDiff ? _diffPage.OutputSize : 0;


                // Protect pages in the scratch buffer after we are done with them
                // This ensures no one writes to them after we have written them to the journal
                // Write access is restored when doing freeing them.
                tx.DataPager.ProtectPageRange(scratchPage, (ulong)(txPage.NumberOfPages * Constants.Storage.PageSize), true);

                ++pageSequencialNumber;
            }
            var totalSizeWritten = (write - outputBuffer) + sizeOfPagesHeader;


            var fullTxBuffer = outputBuffer + (pageCountIncludingAllOverflowPages * (long)Constants.Storage.PageSize) +
                               diffOverheadInPages * (long)Constants.Storage.PageSize;

            var compressionBuffer = fullTxBuffer + sizeof(TransactionHeader);

            var compressedLen = LZ4.Encode64LongBuffer(
                outputBuffer,
                compressionBuffer,
                totalSizeWritten,
                outputBufferSize);

            // We need to account for the transaction header as part of the total length.
            var totalLength = compressedLen + sizeof(TransactionHeader);
            var remainder = totalLength % (4 * Constants.Size.Kilobyte);
            int compressed4Kbs = checked((int)((totalLength / (4 * Constants.Size.Kilobyte)) + (remainder == 0 ? 0 : 1)));

            if (remainder != 0)
            {
                // zero the remainder of the page
                UnmanagedMemory.Set(compressionBuffer + totalLength, 0, 4 * Constants.Size.Kilobyte - remainder);
            }


            var txHeader = tx.GetTransactionHeader();
            txHeader->CompressedSize = compressedLen;
            txHeader->UncompressedSize = totalSizeWritten;
            txHeader->PageCount = numberOfPages;
            txHeader->Hash = Hashing.XXHash64.Calculate(compressionBuffer, (ulong)compressedLen, (ulong)txHeader->TransactionId);

            var prepreToWriteToJournal = new CompressedPagesResult
            {
                Base = fullTxBuffer,
                NumberOf4Kbs = compressed4Kbs,
                NumberOfUncompressedPages = pageCountIncludingAllOverflowPages,
            };
            // Copy the transaction header to the output buffer. 
            Memory.Copy(fullTxBuffer, (byte*)txHeader, sizeof(TransactionHeader));
            Debug.Assert(((long)fullTxBuffer % (4 * Constants.Size.Kilobyte)) == 0, "Memory must be 4kb aligned");
            return prepreToWriteToJournal;
        }

        public void TruncateJournal()
        {
            // switching transactions modes requires to close jounal,
            // truncate it (in case of recovery) and create next journal file
            _lazyTransactionBuffer?.WriteBufferToFile(CurrentFile, null);
            CurrentFile?.JournalWriter.Truncate(Constants.Storage.PageSize * CurrentFile.WritePosIn4KbPosition);
            CurrentFile = null;
        }

        private AbstractPager CreateCompressionPager(long initialSize)
        {
            return _env.Options.CreateScratchPager($"compression.{_compressionPagerCounter++:D10}.buffers", initialSize);
        }

        private DateTime _lastCompressionBufferReduceCheck = DateTime.UtcNow;

        public void ReduceSizeOfCompressionBufferIfNeeded()
        {
            if (!ShouldReduceSizeOfCompressionPager())
                return;

            // the compression pager is too large, we probably had a big transaction and now can
            // free all of that and come back to more reasonable values.
            if (_logger.IsOperationsEnabled)
            {
                _logger.Operations(
                    $"Compression buffer: {_compressionPager} has reached size {(_compressionPager.NumberOfAllocatedPages * Constants.Storage.PageSize) / Constants.Size.Kilobyte:#,#} kb which is more than the limit " +
                    $"of {_env.Options.MaxScratchBufferSize / 1024:#,#} kb. Will trim it now to the max size allowed. If this is happen on a regular basis," +
                    " consider raising the limit (MaxScratchBufferSize option control it), since it can cause performance issues");
            }

            _lastCompressionBufferReduceCheck = DateTime.UtcNow;

            _compressionPager.Dispose();
            _compressionPager = CreateCompressionPager(_env.Options.MaxScratchBufferSize);
        }

        private bool ShouldReduceSizeOfCompressionPager()
        {
            var compressionBufferSize = _compressionPager.NumberOfAllocatedPages * Constants.Storage.PageSize;
            if (compressionBufferSize <= _env.Options.MaxScratchBufferSize)
                return false;

            if ((DateTime.UtcNow - _lastCompressionBufferReduceCheck).TotalMinutes < 5)
                return false;


            // while we are above the limit, we still recently used at least half of it, no point
            // in reducing size yet, we'll be called again
            var shouldReduceSizeOfCompressionPager = _maxNumberOfPagesRequiredForCompressionBuffer < _compressionPager.NumberOfAllocatedPages / 2;
            if (shouldReduceSizeOfCompressionPager)
            {
                return true;
            }
            _maxNumberOfPagesRequiredForCompressionBuffer = 0;
            _lastCompressionBufferReduceCheck = DateTime.UtcNow;
            return true;
        }

        public void TryReduceSizeOfCompressionBufferIfNeeded()
        {
            if (Monitor.TryEnter(_writeLock) == false)
                return;
            // if we can't get it, we are active, so it doesn't matter
            try
            {
                ReduceSizeOfCompressionBufferIfNeeded();
            }
            finally
            {
                Monitor.Exit(_writeLock);
            }
        }
    }

    public unsafe struct CompressedPagesResult
    {
        public byte* Base;
        public int NumberOf4Kbs;
        public int NumberOfUncompressedPages;
    }

}

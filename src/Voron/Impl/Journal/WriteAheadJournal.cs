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
using System.Runtime.ExceptionServices;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Sparrow.Compression;
using Sparrow.Logging;
using Sparrow.Threading;
using Sparrow.Utils;
using Voron.Data;
using Voron.Exceptions;
using Voron.Impl.FileHeaders;
using Voron.Impl.Paging;
using Voron.Util;
using Voron.Global;
using System.Buffers;

namespace Voron.Impl.Journal
{
    public unsafe class WriteAheadJournal : IDisposable
    {
        private readonly StorageEnvironment _env;
        private readonly AbstractPager _dataPager;

        private long _currentJournalFileSize;
        private DateTime _lastFile;

        private long _journalIndex = -1;

        private readonly JournalApplicator _journalApplicator;

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

        internal NativeMemory.ThreadStats CurrentFlushingInProgressHolder;

        private readonly DisposeOnce<SingleAttempt> _disposeRunner;

        public WriteAheadJournal(StorageEnvironment env)
        {
            _env = env;
            _logger = LoggingSource.Instance.GetLogger<WriteAheadJournal>(Path.GetFileName(env.ToString()));
            _dataPager = _env.Options.DataPager;
            _currentJournalFileSize = env.Options.InitialLogFileSize;
            _headerAccessor = env.HeaderAccessor;

            _compressionPager = CreateCompressionPager(_env.Options.InitialFileSize ?? _env.Options.InitialLogFileSize);
            _journalApplicator = new JournalApplicator(this);

            _disposeRunner = new DisposeOnce<SingleAttempt>(() =>
            {
                // We cannot dispose the journal until we are done with all of
                // the pending writes
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

                _files = ImmutableAppendOnlyList<JournalFile>.Empty;
            });
        }

        public ImmutableAppendOnlyList<JournalFile> Files => _files;

        public JournalApplicator Applicator => _journalApplicator;

        public bool HasLazyTransactions { get; set; }

        private JournalFile NextFile(int numberOf4Kbs = 1)
        {
            var now = DateTime.UtcNow;
            if ((now - _lastFile).TotalSeconds < 90)
            {
                _currentJournalFileSize = Math.Min(_env.Options.MaxLogFileSize, _currentJournalFileSize * 2);
            }
            var actualLogSize = _currentJournalFileSize;
            long minRequiredSize = numberOf4Kbs * 4 * Constants.Size.Kilobyte;
            if (_currentJournalFileSize < minRequiredSize)
            {
                _currentJournalFileSize = Bits.NextPowerOf2(minRequiredSize);
                actualLogSize = _currentJournalFileSize;
            }

            var journalPager = _env.Options.CreateJournalWriter(_journalIndex + 1, actualLogSize);

            // we modify the in memory state _after_ we created the file, because we have to make sure that 
            // we have created it successfully first. 
            _journalIndex++;

            _lastFile = now;

            var journal = new JournalFile(_env, journalPager, _journalIndex);
            journal.AddRef(); // one reference added by a creator - write ahead log

            _files = _files.Append(journal);

            _headerAccessor.Modify(header =>
            {
                header->Journal.CurrentJournal = journal.Number;
                header->IncrementalBackup.LastCreatedJournal = journal.Number;
            });

            return journal;
        }
        public bool RecoverDatabase(TransactionHeader* txHeader, Action<string> addToInitLog)
        {
            // note, we don't need to do any concurrency here, happens as a single threaded
            // fashion on db startup
            var requireHeaderUpdate = false;

            var logInfo = _headerAccessor.Get(ptr => ptr->Journal);
            
            if (_env.Options.IncrementalBackupEnabled == false && _env.Options.CopyOnWriteMode == false)
            {
                // we want to check that we cleanup old log files if they aren't needed
                // this is more just to be safe than anything else, they shouldn't be there.
                var unusedfiles = logInfo.LastSyncedJournal;

                while (true)
                {
                    unusedfiles--;
                    if (_env.Options.TryDeleteJournal(unusedfiles) == false)
                        break;
                }

            }

            var modifiedPages = new HashSet<long>();
            var journalFiles = new List<JournalFile>();

            long lastSyncedTxId = logInfo.LastSyncedTransactionId;
            long lastSyncJournal = logInfo.LastSyncedJournal;

            // the last sync journal is allowed to be deleted, it might have been fully synced, which is fine
            // we rely on the lastSyncedTxId to verify correctness.
            var journalToStartReadingFrom = logInfo.LastSyncedJournal;
            if (_env.Options.JournalExists(journalToStartReadingFrom) == false &&
                logInfo.Flags.HasFlag(JournalInfoFlags.IgnoreMissingLastSyncJournal) ||
                journalToStartReadingFrom == -1)
                journalToStartReadingFrom++;

            for (var journalNumber = journalToStartReadingFrom; journalNumber <= logInfo.CurrentJournal; journalNumber++)
            {
                addToInitLog?.Invoke($"Recovering journal {journalNumber} (upto last journal {logInfo.CurrentJournal})");
                var initialSize = _env.Options.InitialFileSize ?? _env.Options.InitialLogFileSize;
                var journalRecoveryName = StorageEnvironmentOptions.JournalRecoveryName(journalNumber);

                try
                {
                    using (var recoveryPager = _env.Options.CreateTemporaryBufferPager(journalRecoveryName, initialSize))
                    using (var pager = _env.Options.OpenJournalPager(journalNumber, logInfo))
                    {
                        RecoverCurrentJournalSize(pager);

                        var transactionHeader = txHeader->TransactionId == 0 ? null : txHeader;
                        using (
                            var journalReader = new JournalReader(pager, _dataPager, recoveryPager, modifiedPages, logInfo,
                                transactionHeader))
                        {
                            var transactionHeaders = ArrayPool<TransactionHeader>.Shared.Rent((int)journalReader.NumberOfAllocated4Kb);
                            try
                            {
                                var transactionCount = journalReader.RecoverAndValidate(_env.Options, transactionHeaders);

                                var lastReadHeaderPtr = journalReader.LastTransactionHeader;

                                if (lastReadHeaderPtr != null)
                                {
                                    *txHeader = *lastReadHeaderPtr;
                                    lastSyncedTxId = txHeader->TransactionId;
                                    lastSyncJournal = journalNumber;
                                }

                                pager.Dispose(); // need to close it before we open the journal writer

                                var jrnlWriter = _env.Options.CreateJournalWriter(journalNumber,
                                    pager.NumberOfAllocatedPages * Constants.Storage.PageSize);
                                var jrnlFile = new JournalFile(_env, jrnlWriter, journalNumber);
                                jrnlFile.InitFrom(journalReader, transactionHeaders, transactionCount);
                                jrnlFile.AddRef(); // creator reference - write ahead log

                                journalFiles.Add(jrnlFile);

                                if (journalReader.RequireHeaderUpdate) //this should prevent further loading of transactions
                                {
                                    requireHeaderUpdate = true;
                                    break;
                                }
                            }
                            finally
                            {
                                ArrayPool<TransactionHeader>.Shared.Return(transactionHeaders);
                            }
                        }
                    }
                }
                catch (InvalidJournalException)
                {
                    if (_env.Options.IgnoreInvalidJournalErrors == true)
                    {
                        addToInitLog?.Invoke($"Encountered invalid journal {journalNumber} @ {_env.Options}. Skipping this journal and keep going the recovery operation because '{nameof(_env.Options.IgnoreInvalidJournalErrors)}' options is set");
                        continue;
                    }

                    throw;
                }
            }


            if (_env.Options.EncryptionEnabled == false) // for encryption, we already use AEAD, so no need
            {
                // here we want to check that the checksum on all the modified pages is valid
                // we can't do that during the journal application process because we may have modifications
                // to pages that overwrite one another. So we have to do this at the end, this will detect
                // corruption when applying journals at recovery time rather than at usage.
                var tempTx = new TempPagerTransaction();

                var sortedPages = modifiedPages.ToArray();
                Array.Sort(sortedPages);

                long minPageChecked = -1;

                // we need to iterate from the end in order to filter out pages that was overwritten by later transaction

                for (var i = sortedPages.Length - 1; i >= 0; i--)
                {
                    using (tempTx) // release any resources, we just wanted to validate things
                    {
                        var modifiedPage = sortedPages[i];

                        var ptr = (PageHeader*)_dataPager.AcquirePagePointerWithOverflowHandling(tempTx, modifiedPage, null);

                        var maxPageRange = modifiedPage + VirtualPagerLegacyExtensions.GetNumberOfPages(ptr) - 1;

                        if (minPageChecked != -1 && maxPageRange >= minPageChecked)
                        {
                            continue;
                        }

                        _env.ValidatePageChecksum(modifiedPage, ptr);

                        minPageChecked = modifiedPage;
                    }
                }
            }

            _files = _files.AppendRange(journalFiles);

            if (lastSyncedTxId < 0)
                VoronUnrecoverableErrorException.Raise(_env,
                    "First transaction initializing the structure of Voron database is corrupted. Cannot access internal database metadata. Create a new database to recover.");

            Debug.Assert(lastSyncedTxId >= 0);
            Debug.Assert(lastSyncJournal >= 0);

            _journalIndex = lastSyncJournal;

            if (_env.Options.CopyOnWriteMode == false)
            {
                CleanupNewerInvalidJournalFiles(lastSyncJournal);
            }

            if (_files.Count > 0)
            {
                var lastFile = _files.Last();
                if (lastFile.Available4Kbs >= 2)
                    // it must have at least one page for the next transaction header and one 4kb for data
                    CurrentFile = lastFile;
            }

            return requireHeaderUpdate;
        }

        private void CleanupNewerInvalidJournalFiles(long lastSyncedJournal)
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


        public Page? ReadPage(LowLevelTransaction tx, long pageNumber, Dictionary<int, PagerState> scratchPagerStates)
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
                        var page = _env.ScratchBufferPool.ReadPage(tx, value.ScratchNumber, value.ScratchPage, scratchPagerStates[value.ScratchNumber]);

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
                    // ReSharper disable once RedundantArgumentDefaultValue
                    var page = _env.ScratchBufferPool.ReadPage(tx, value.ScratchNumber, value.ScratchPage, pagerState: null);

                    Debug.Assert(page.PageNumber == pageNumber);

                    return page;
                }
            }

            return null;
        }

        public void Dispose()
        {
            _disposeRunner.Dispose();
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

        public sealed class JournalApplicator : IDisposable
        {
            private readonly Dictionary<long, JournalFile> _journalsToDelete = new Dictionary<long, JournalFile>();
            private readonly object _flushingLock = new object();
            private readonly SemaphoreSlim _fsyncLock = new SemaphoreSlim(1);
            private readonly WriteAheadJournal _waj;
            private MultipleUseFlag _forceDataSync = new MultipleUseFlag();
            private readonly ManualResetEventSlim _waitForJournalStateUpdateUnderTx = new ManualResetEventSlim();

            private long _lastFlushedTransactionId;
            private long _lastFlushedJournalId;
            private JournalFile _lastFlushedJournal;
            private bool _ignoreLockAlreadyTaken;
            private long _totalWrittenButUnsyncedBytes;
            private Action<LowLevelTransaction> _updateJournalStateAfterFlush;
            private Task _pendingSync = Task.CompletedTask;

            // for testing pupose:
            public bool TestingNowWaitingSignal = false;
            public ManualResetEvent TestingWait = null;
            private readonly Stopwatch _timeSinceLastImmediateSync = Stopwatch.StartNew();
            public bool TestingSkippedUpdateDatabaseStateAfterSync { get; set; }
            public bool TestingDoNotWait { get; set; }

            public void OnTransactionCommitted(LowLevelTransaction tx)
            {
                var action = _updateJournalStateAfterFlush;
                action?.Invoke(tx);
            }

            public long TotalWrittenButUnsyncedBytes => Interlocked.Read(ref _totalWrittenButUnsyncedBytes);

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

                    var needImmediateFsync =
                        _pendingSync.IsCompleted &&
                        (_forceDataSync.Lower() ||
                         Interlocked.Read(ref _totalWrittenButUnsyncedBytes) > 32 * Constants.Size.Megabyte ||
                         _timeSinceLastImmediateSync.ElapsedMilliseconds > 3 * 60 * 1000);

                    if (needImmediateFsync)
                    {
                        _timeSinceLastImmediateSync.Restart();
                        // will never wait, we ensure that we have completed the task
                        // we call Wait() here to ensure that if there was an error in 
                        // the previous sync, we'll propagate it out and mark the env
                        // as catastrophic failure.
                        _pendingSync.Wait(token);
                        token.ThrowIfCancellationRequested();
                        var operation = new SyncOperation(this);
                        if (operation.TryGatherInformationToStartSync(out var syncCounter))
                        {
                            _pendingSync = operation.Task;
                            System.Threading.ThreadPool.QueueUserWorkItem(state => ((SyncOperation)state).CompleteSync(syncCounter), operation);
                        }
                    }

                    ApplyJournalStateAfterFlush(token, lastProcessedJournal, lastFlushedTransactionId, unusedJournals);

                    if (needImmediateFsync == false)
                        _waj._env.QueueForSyncDataFile();
                }
                finally
                {
                    if (lockTaken)
                        Monitor.Exit(_flushingLock);
                }

                _waj._env.LogsApplied();
            }

            private void ApplyJournalStateAfterFlush(CancellationToken token, long lastProcessedJournal, long lastFlushedTransactionId, List<JournalFile> unusedJournals)
            {
                // the idea here is that even though we need to run the journal through its state update under the transaction lock
                // we don't actually have to do that in our own transaction, what we'll do is to setup things so if there is a running
                // write transaction, we'll piggy back on its commit to complete our process, without interrupting its work
                _waj._env.FlushInProgressLock.EnterWriteLock();
                _waj.CurrentFlushingInProgressHolder = NativeMemory.CurrentThreadStats;

                try
                {
                    var transactionPersistentContext = new TransactionPersistentContext(true);
                    _waitForJournalStateUpdateUnderTx.Reset();
                    ExceptionDispatchInfo edi = null;
                    var sp = Stopwatch.StartNew();
                    Action<LowLevelTransaction> currentAction = txw =>
                    {
                        try
                        {
                            UpdateJournalStateUnderWriteTransactionLock(txw, lastProcessedJournal, lastFlushedTransactionId, unusedJournals);
                        }
                        catch (Exception e)
                        {
                            edi = ExceptionDispatchInfo.Capture(e);
                            throw;
                        }
                        finally
                        {
                            if (_waj._logger.IsInfoEnabled)
                                _waj._logger.Info($"Updated journal state under write tx lock after waiting for {sp.Elapsed}");
                            _updateJournalStateAfterFlush = null;
                            _waitForJournalStateUpdateUnderTx.Set();
                        }
                    };
                    Interlocked.Exchange(ref _updateJournalStateAfterFlush, currentAction);

                    WaitForJournalStateToBeUpdated(token, transactionPersistentContext, currentAction);

                    edi?.Throw();
                }
                finally
                {
                    _waj.CurrentFlushingInProgressHolder = null;
                    _waj._env.FlushInProgressLock.ExitWriteLock();
                }
            }

            private void WaitForJournalStateToBeUpdated(CancellationToken token, TransactionPersistentContext transactionPersistentContext, Action<LowLevelTransaction> currentAction)
            {
                do
                {
                    LowLevelTransaction txw = null;
                    try
                    {
                        try
                        {
                            txw = _waj._env.NewLowLevelTransaction(transactionPersistentContext,
                                TransactionFlags.ReadWrite, timeout: TimeSpan.Zero);
                        }
                        catch (OperationCanceledException)
                        {
                            break;
                        }
                        catch (TimeoutException)
                        {
                            // couldn't get the transaction lock, we'll wait for the running transaction to complete
                            // for a bit, and then try again
                            try
                            {
                                if (_waitForJournalStateUpdateUnderTx.Wait(TimeSpan.FromMilliseconds(250), token))
                                    break;
                            }
                            catch (OperationCanceledException)
                            {
                                break;
                            }
                            continue;
                        }
                        var action = _updateJournalStateAfterFlush;
                        if (action != null)
                        {
                            action(txw);
                            txw.Commit();
                        }
                        break;
                    }
                    finally
                    {
                        txw?.Dispose();
                    }
                    // if it was changed, this means that we are done
                } while (currentAction == _updateJournalStateAfterFlush);
            }

            private void UpdateJournalStateUnderWriteTransactionLock(LowLevelTransaction txw, long lastProcessedJournal, long lastFlushedTransactionId, List<JournalFile> unusedJournals)
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

                // we release up to the last read transaction, because there might be new read transactions that are currently
                // running, that started after the flush
                var lastSyncedTransactionId =
                    Math.Min(Math.Min(lastFlushedTransactionId, _waj._env.CurrentReadTransactionId - 1), txw.Id - 1);

                // we have to free pages of the unused journals before the remaining ones that are still in use
                // to prevent reading from them by any read transaction (read transactions search journals from the newest
                // to read the most updated version)


                foreach (var journalFile in unusedJournals.OrderBy(x => x.Number))
                {
                    journalFile.FreeScratchPagesOlderThan(txw, lastSyncedTransactionId);
                }

                foreach (var jrnl in _waj._files.OrderBy(x => x.Number))
                {
                    jrnl.FreeScratchPagesOlderThan(txw, lastSyncedTransactionId);
                }

                // by forcing a commit, we free the read transaction that held the lazy tx buffer (if existed)
                // and make those pages available in the scratch files
                txw.IsLazyTransaction = false;
                _waj.HasLazyTransactions = false;
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

            public void WaitForSyncToCompleteOnDispose()
            {
                if (Monitor.IsEntered(_flushingLock) == false)
                    throw new InvalidOperationException("This method can only be called while holding the flush lock");

                if (_waj._env.Disposed == false)
                    throw new InvalidOperationException(
                        "This method can only be called after the storage environment has been disposed");

                if (_fsyncLock.Wait(0))
                {
                    _fsyncLock.Release();
                    return;
                }

                // now the sync lock is in progress, but it can't complete because we are holding the flush lock
                // we'll first give the flush lock and then wait on the FSync lock until the sync is completed
                // then we'll re-aqcuire the flush lock

                Monitor.Exit(_flushingLock);
                try
                {
                    // we wait to take the lock here to ensure that all previous sync operations
                    // has completed, and we know that no new ones can start
                    _fsyncLock.Wait();
                    try
                    {
                        // now we know that the sync is done
                        // we also know that no other sync can start now
                        // because Disposed is set to true
                    }
                    finally
                    {
                        _fsyncLock.Release();
                    }
                }
                finally
                {
                    Monitor.Enter(_flushingLock);// reacquire the lock
                }

            }

            // This can take a LONG time, and it needs to run concurrently with the
            // rest of the system, so in order to handle this properly, we do:
            // 1) Take the flushing lock (if we fail, we'll requeue for the sync)
            // 2) Take a snapshot of the current status of this env flushing status
            // 3) Release the lock & sync the file (take a long time)
            // 4) Re-take the lock, update the sync status in the header with the values we snapshotted
            public class SyncOperation : IDisposable
            {
                private readonly JournalApplicator _parent;
                bool _fsyncLockTaken;
                long _lastSyncedJournal;
                long _currentTotalWrittenBytes;
                long _lastSyncedTransactionId;
                private readonly List<KeyValuePair<long, JournalFile>> _journalsToDelete;
                bool _flushLockTaken;
                private TransactionHeader _transactionHeader;
                private readonly TaskCompletionSource<object> _tcs = new TaskCompletionSource<object>(TaskCreationOptions.RunContinuationsAsynchronously);

                public SyncOperation(JournalApplicator parent)
                {
                    _parent = parent;
                    _journalsToDelete = new List<KeyValuePair<long, JournalFile>>();
                    _fsyncLockTaken = false;
                    _lastSyncedJournal = 0;
                    _currentTotalWrittenBytes = 0;
                    _lastSyncedTransactionId = 0;
                    _flushLockTaken = false;
                    _transactionHeader = new TransactionHeader();
                }

                public Task Task => _tcs.Task;

                internal Action AfterGatherInformationAction;

                public bool SyncDataFile()
                {
                    if (TryGatherInformationToStartSync(out var syncCounter) == false)
                        return false;

                    if (_parent._waj._env.Disposed)
                        return false;

                    AfterGatherInformationAction?.Invoke();

                    CallPagerSync();

                    // can take a long time, need to check again
                    if (_parent._waj._env.Disposed)
                        return false;

                    if (_parent != null)
                    {
                        _parent.TestingNowWaitingSignal = true;
                        if (_parent.TestingDoNotWait == false)
                            _parent.TestingWait?.WaitOne();
                    }

                    UpdateDatabaseStateAfterSync(syncCounter);

                    return true;
                }

                public void CompleteSync(long syncCounter)
                {
                    try
                    {
                        using (this)
                        {
                            if (_parent._waj._env.Disposed == false)// not already disposed / disposing
                            {
                                CallPagerSync();

                                UpdateDatabaseStateAfterSync(syncCounter);
                            }
                            _tcs.TrySetResult(null);
                        }
                    }
                    catch (Exception e)
                    {
                        _tcs.TrySetException(e);
                    }
                }

                private void UpdateDatabaseStateAfterSync(long previousSyncedCounter)
                {
                    lock (_parent._flushingLock)
                    {
                        if (_parent._waj._env.Disposed)
                            return;

                        // This might be an erlier sync which started at SyncDataFile()
                        // but it is UpdateDatabaseStateAfterSync _after_ the next sync call made it's update
                        // so we exiting here to prevent overrun
                        if (previousSyncedCounter != _parent._waj._env.LastSyncCounter)
                        {
                            _parent.TestingSkippedUpdateDatabaseStateAfterSync = true;
                            return;
                        }

                        Interlocked.Add(ref _parent._totalWrittenButUnsyncedBytes, -_currentTotalWrittenBytes);

                        var ignoreLastSyncJournalMissing = false;

                        foreach (var item in _journalsToDelete)
                        {
                            if (item.Value.Number == _lastSyncedJournal)
                            {
                                // we are about to delete it, so safe to ignore this
                                ignoreLastSyncJournalMissing = true;
                                break;
                            }
                        }

                        _parent.UpdateFileHeaderAfterDataFileSync(_lastSyncedJournal, _lastSyncedTransactionId, ignoreLastSyncJournalMissing, ref _transactionHeader);

                        foreach (var toDelete in _journalsToDelete)
                        {
                            if (toDelete.Value.Number > _lastSyncedJournal) // precaution
                                continue;

                            if (_parent._waj._env.Options.IncrementalBackupEnabled == false)
                                toDelete.Value.DeleteOnClose = true;

                            toDelete.Value.Release();
                        }
                    }
                }

                private void CallPagerSync()
                {
                    // danger mode assumes that no OS crashes can happen, in order to get best performance

                    if (_parent._waj._env.Options.TransactionsMode != TransactionsMode.Danger)
                    {
                        // We do the sync _outside_ of the lock, letting the rest of the stuff proceed
                        var sp = Stopwatch.StartNew();
                        _parent._waj._dataPager.Sync(Interlocked.Read(ref _parent._totalWrittenButUnsyncedBytes));
                        if (_parent._waj._logger.IsInfoEnabled)
                        {
                            var sizeInKb = (_parent._waj._dataPager.NumberOfAllocatedPages * Constants.Storage.PageSize) / Constants.Size.Kilobyte;
                            _parent._waj._logger.Info($"Sync of {sizeInKb:#,#0} kb file with {_currentTotalWrittenBytes / Constants.Size.Kilobyte:#,#0} kb dirty in {sp.Elapsed}");
                        }
                    }
                }

                public bool TryGatherInformationToStartSync(out long syncCounter)
                {
                    syncCounter = -1;
                    // We need _transactionHeader to be the frozen value at the time we _started_ the
                    // sync process
                    try
                    {
                        Monitor.TryEnter(_parent._flushingLock, 0, ref _flushLockTaken);

                        if (_flushLockTaken == false)
                        {
                            // can't get the lock, we'll try again later, this time we are running
                            // as forced, because we have higher priority
                            if (_parent._waj._logger.IsInfoEnabled)
                                _parent._waj._logger.Info(
                                    $"Asking for required sync on {_parent._waj._dataPager.FileName} because started a sync and aborted because we couldn't get the flushing lock");
                            _parent._forceDataSync.Raise();
                            return false;
                        }

                        syncCounter = Interlocked.Increment(ref _parent._waj._env.LastSyncCounter);

                        if (_parent._waj._env.Disposed)
                            return false; // we have already disposed, nothing to do here

                        if (_parent._lastFlushedJournal == null)
                            // nothing was flushed since we last synced, nothing to do
                            return false;

                        // we only ever take the _fsyncLock _after_ we already took the flush lock
                        // so this will never be contended
                        _fsyncLockTaken = _parent._fsyncLock.Wait(0);
                        if (_fsyncLockTaken == false)
                        {
                            // probably another sync taking place right now, let us schedule another one, just in case
                            _parent._waj._env.QueueForSyncDataFile();
                            return false;
                        }
                        _currentTotalWrittenBytes = Interlocked.Read(ref _parent._totalWrittenButUnsyncedBytes);
                        _lastSyncedJournal = _parent._lastFlushedJournalId;
                        _lastSyncedTransactionId = _parent._lastFlushedTransactionId;
                        _parent._lastFlushedJournal.SetLastReadTxHeader(_lastSyncedTransactionId, ref _transactionHeader);
                        if (_lastSyncedTransactionId != _transactionHeader.TransactionId)
                        {
                            VoronUnrecoverableErrorException.Raise(_parent._waj._env,
                                $"Error syncing the data file. The last sync tx is {_lastSyncedTransactionId}, but the journal's last tx id is {_transactionHeader.TransactionId}, possible file corruption?"
                            );
                        }

                        _parent._lastFlushedJournal = null;

                        foreach (var toDelete in _parent._journalsToDelete)
                        {
                            if (toDelete.Key > _lastSyncedJournal)
                                continue;

                            _journalsToDelete.Add(toDelete);
                        }

                        _parent._waj._env.Options.SetLastReusedJournalCountOnSync(_journalsToDelete.Count);

                        foreach (var kvp in _journalsToDelete)
                        {
                            _parent._journalsToDelete.Remove(kvp.Key);
                        }

                        return true;
                    }
                    finally
                    {
                        if (_flushLockTaken)
                            Monitor.Exit(_parent._flushingLock);
                    }
                }

                public void Dispose()
                {
                    if (_fsyncLockTaken)
                        _parent._fsyncLock.Release();
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
                    using (var meter = _waj._dataPager.Options.IoMetrics.MeterIoRate(_waj._dataPager.FileName.FullPath, IoMetrics.MeterType.DataFlush, 0))
                    {
                        using (var batchWrites = _waj._dataPager.BatchWriter())
                        {
                            var tempTx = new TempPagerTransaction();
                            foreach (var pagePosition in pagesToWrite.Values)
                            {
                                var scratchNumber = pagePosition.ScratchNumber;
                                if (scratchPagerStates.TryGetValue(scratchNumber, out var pagerState) == false)
                                {
                                    pagerState = scratchBufferPool.GetPagerState(scratchNumber);
                                    pagerState.AddRef();

                                    scratchPagerStates.Add(scratchNumber, pagerState);
                                }

                                if (_waj._env.Options.EncryptionEnabled == false)
                                {
                                    using (tempTx) // release any resources, we just wanted to validate things
                                    {
                                        var page = (PageHeader*)scratchBufferPool.AcquirePagePointerWithOverflowHandling(tempTx, scratchNumber, pagePosition.ScratchPage);
                                        var checksum = StorageEnvironment.CalculatePageChecksum((byte*)page, page->PageNumber, out var expectedChecksum);
                                        if (checksum != expectedChecksum)
                                            ThrowInvalidChecksumOnPageFromScratch(scratchNumber, pagePosition, page, checksum, expectedChecksum);
                                    }
                                }

                                var numberOfPages = scratchBufferPool.CopyPage(
                                    batchWrites,
                                    scratchNumber,
                                    pagePosition.ScratchPage,
                                    pagerState);

                                written += numberOfPages * Constants.Storage.PageSize;
                            }
                        }

                        meter.SetFileSize(_waj._dataPager.TotalAllocationSize);
                        meter.IncrementSize(written);
                    }

                    if (_waj._logger.IsInfoEnabled)
                        _waj._logger.Info($"Flushed {pagesToWrite.Count:#,#} pages to { _waj._dataPager.FileName} with {written / Constants.Size.Kilobyte:#,#} kb in {sp.Elapsed}");

                    Interlocked.Add(ref _totalWrittenButUnsyncedBytes, written);
                }
                finally
                {
                    foreach (var scratchPagerState in scratchPagerStates.Values)
                    {
                        scratchPagerState.Release();
                    }
                }
            }

            private static void ThrowInvalidChecksumOnPageFromScratch(int scratchNumber, PagePosition pagePosition, PageHeader* page, ulong checksum, ulong expectedChecksum)
            {
                var message = $"During apply logs to data, tried to copy {scratchNumber} / {pagePosition.ScratchNumber} ({page->PageNumber}) " +
                              $"has checksum {checksum} but expected {expectedChecksum}";

                message += $"Page flags: {page->Flags}. ";

                if ((page->Flags & PageFlags.Overflow) == PageFlags.Overflow)
                    message += $"Overflow size: {page->OverflowSize}. ";

                throw new InvalidDataException(message);
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
                                                    // we didn't synchronize whole journal
                            j.PageTranslationTable.MaxTransactionId() != lastFlushedTransactionId)
                            continue; // do not mark it as unused


                        // Since we got the snapshot, this journal file had writes, so we 
                        // are going to skip it for this round
                        if (j.WritePosIn4KbPosition != j.FileInstance.WritePosIn4KbPosition)
                            continue;
                    }
                    unusedJournalFiles.Add(_waj._files.First(x => x.Number == j.Number));
                }
                return unusedJournalFiles;
            }

            private void UpdateFileHeaderAfterDataFileSync(
                long lastSyncedJournal,
                long lastSyncedTransactionId,
                bool ignoreLastSyncJournalMissing,
                ref TransactionHeader lastReadTxHeader)
            {
                Debug.Assert(lastSyncedJournal != -1);
                Debug.Assert(lastSyncedTransactionId != -1);

                var treeRootHeader = lastReadTxHeader.Root;
                var transactionId = lastReadTxHeader.TransactionId;
                var lastPageNumber = lastReadTxHeader.LastPageNumber;

                _waj._headerAccessor.Modify(header =>
                {
                    header->TransactionId = transactionId;
                    header->LastPageNumber = lastPageNumber;

                    header->Journal.LastSyncedJournal = lastSyncedJournal;
                    header->Journal.LastSyncedTransactionId = lastSyncedTransactionId;

                    Memory.Set(header->Journal.Reserved, 0, JournalInfo.NumberOfReservedBytes);

                    if (ignoreLastSyncJournalMissing)
                        header->Journal.Flags |= JournalInfoFlags.IgnoreMissingLastSyncJournal;
                    else
                        header->Journal.Flags &= ~JournalInfoFlags.IgnoreMissingLastSyncJournal;

                    header->Root = treeRootHeader;
                });
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

                try
                {
                    _pendingSync.Wait();
                }
                catch (AggregateException)
                {
                    if (_waj._env.Disposed)
                        return;

                    throw;
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

                var logInfo = _waj._env.HeaderAccessor.Get(ptr => ptr->Journal);

                if (current.Number != logInfo.LastSyncedJournal)
                    throw new InvalidOperationException(string.Format("Cannot delete current journal because it isn't last synced file. Current journal number: {0}, the last one which was synced {1}", _waj.CurrentFile?.Number ?? -1, _lastFlushedJournalId));


                if (_waj._env.NextWriteTransactionId - 1 != logInfo.LastSyncedTransactionId)
                    throw new InvalidOperationException("Cannot delete current journal because it hasn't synced everything up to the last write transaction");

                _waj._files = _waj._files.RemoveFront(1);
                _waj.CurrentFile = null;

                _waj._headerAccessor.Modify(header =>
                {
                    header->Journal.CurrentJournal = -1;

                    if (current.Number != header->Journal.LastSyncedJournal)
                    {
                        throw new InvalidOperationException($"Attempted to remove a journal ({current.Number}) that hasn't been synced yet (last synced journal: {header->Journal.LastSyncedJournal})");
                    }

                    Memory.Set(header->Journal.Reserved, 0, JournalInfo.NumberOfReservedBytes);
                    header->Journal.Flags |= JournalInfoFlags.IgnoreMissingLastSyncJournal;
                });

                current.DeleteOnClose = true;
                current.Release();

            }
        }

        public CompressedPagesResult WriteToJournal(LowLevelTransaction tx, out string journalFilePath)
        {
            lock (_writeLock)
            {
                var sp = Stopwatch.StartNew();
                var journalEntry = PrepareToWriteToJournal(tx);
                if (_logger.IsInfoEnabled)
                {
                    _logger.Info($"Preparing to write tx {tx.Id} to journal with {journalEntry.NumberOfUncompressedPages:#,#} pages ({(journalEntry.NumberOfUncompressedPages * Constants.Storage.PageSize) / Constants.Size.Kilobyte:#,#} kb) in {sp.Elapsed} with {Math.Round(journalEntry.NumberOf4Kbs * 4d, 1):#,#.#;;0} kb compressed.");
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
                journalEntry.UpdatePageTranslationTable = CurrentFile.Write(tx, journalEntry, _lazyTransactionBuffer);
                sp.Stop();
                _lastCompressionAccelerationInfo.WriteDuration = sp.Elapsed;
                _lastCompressionAccelerationInfo.CalculateOptimalAcceleration();

                if (_logger.IsInfoEnabled)
                    _logger.Info($"Writing {journalEntry.NumberOf4Kbs * 4:#,#} kb to journal {CurrentFile.Number:D19} took {sp.Elapsed}");

                journalFilePath = CurrentFile.JournalWriter.FileName.FullPath;

                if (CurrentFile.Available4Kbs == 0)
                {
                    _lazyTransactionBuffer?.WriteBufferToFile(CurrentFile, tx);
                    CurrentFile = null;
                }

                ZeroCompressionBufferIfNeeded(tx);
                ReduceSizeOfCompressionBufferIfNeeded();

                return journalEntry;
            }
        }

        private CompressedPagesResult PrepareToWriteToJournal(LowLevelTransaction tx)
        {
            var txPages = tx.GetTransactionPages();
            var numberOfPages = txPages.Count;
            var pagesCountIncludingAllOverflowPages = 0;
            foreach (var page in txPages)
            {
                pagesCountIncludingAllOverflowPages += page.NumberOfPages;
            }

            var performCompression = pagesCountIncludingAllOverflowPages > _env.Options.CompressTxAboveSizeInBytes / Constants.Storage.PageSize;

            var sizeOfPagesHeader = numberOfPages * sizeof(TransactionHeaderPageInfo);
            var overhead = sizeOfPagesHeader + (long)numberOfPages * sizeof(long);
            var overheadInPages = checked((int)(overhead / Constants.Storage.PageSize + (overhead % Constants.Storage.PageSize == 0 ? 0 : 1)));

            const int transactionHeaderPageOverhead = 1;
            var pagesRequired = (transactionHeaderPageOverhead + pagesCountIncludingAllOverflowPages + overheadInPages);

            PagerState pagerState;
            try
            {
                pagerState = _compressionPager.EnsureContinuous(0, pagesRequired);
            }
            catch (InsufficientMemoryException)
            {
                // RavenDB-10830: failed to lock memory of temp buffers in encrypted db, let's create new file with initial size

                _compressionPager.Dispose();
                _compressionPager = CreateCompressionPager(_env.Options.InitialFileSize ?? _env.Options.InitialLogFileSize);
                _lastCompressionBufferReduceCheck = DateTime.UtcNow;
                throw;
            }

            tx.EnsurePagerStateReference(pagerState);

            _compressionPager.EnsureMapped(tx, 0, pagesRequired);
            var txHeaderPtr = _compressionPager.AcquirePagePointer(tx, 0);
            var txPageInfoPtr = txHeaderPtr + sizeof(TransactionHeader);
            var pagesInfo = (TransactionHeaderPageInfo*)txPageInfoPtr;

            var write = txPageInfoPtr + sizeOfPagesHeader;
            var pageSequentialNumber = 0;
            var pagesEncountered = 0;
            foreach (var txPage in txPages)
            {
                var scratchPage = tx.Environment.ScratchBufferPool.AcquirePagePointerWithOverflowHandling(tx, txPage.ScratchFileNumber, txPage.PositionInScratchBuffer);
                var pageHeader = (PageHeader*)scratchPage;

                // When encryption is off, we do validation by checksum
                if (_env.Options.EncryptionEnabled == false)
                {
                    pageHeader->Checksum = StorageEnvironment.CalculatePageChecksum(scratchPage, pageHeader->PageNumber, pageHeader->Flags, pageHeader->OverflowSize);
                }

                pagesInfo[pageSequentialNumber].PageNumber = pageHeader->PageNumber;
                txPage.ScratchPageNumber = pageHeader->PageNumber;

                *(long*)write = pageHeader->PageNumber;
                write += sizeof(long);

                if (_env.Options.EncryptionEnabled == false && performCompression)
                {
                    _diffPage.Output = write;

                    int diffPageSize = txPage.NumberOfPages * Constants.Storage.PageSize;
                    pagesEncountered += txPage.NumberOfPages;
                    Debug.Assert(pagesEncountered <= pagesCountIncludingAllOverflowPages);
                    if (txPage.PreviousVersion != null)
                    {
                        _diffPage.ComputeDiff(txPage.PreviousVersion.Value.Pointer, scratchPage, diffPageSize);
                    }
                    else
                    {
                        _diffPage.ComputeNew(scratchPage, diffPageSize);
                    }

                    write += _diffPage.OutputSize;
                    pagesInfo[pageSequentialNumber].Size = _diffPage.OutputSize == 0 ? 0 : diffPageSize;
                    pagesInfo[pageSequentialNumber].IsNewDiff = txPage.PreviousVersion == null;
                    pagesInfo[pageSequentialNumber].DiffSize = _diffPage.IsDiff ? _diffPage.OutputSize : 0;
                    Debug.Assert(Math.Max(pagesInfo[pageSequentialNumber].Size, pagesInfo[pageSequentialNumber].DiffSize) <= diffPageSize);
                }
                else
                {
                    // If encryption is enabled we cannot use diffs in the journal. 
                    // When recovering, we need to compare the page (diff) from the journal to the page on the data file.
                    // To do that we need to first decrypt the page from the data file, but what happens if it was only partially 
                    // written when we crashed? We cannot decrypt partial data, therefore we cannot compare the diff to the plaintext on disk.
                    // The solution is to write the full page to the journal and when recovering, copy the full page to the data file. 
                    int size = txPage.NumberOfPages * Constants.Storage.PageSize;
                    pagesEncountered += txPage.NumberOfPages;
                    Debug.Assert(pagesEncountered <= pagesCountIncludingAllOverflowPages);

                    Memory.Copy(write, scratchPage, size);

                    write += size;
                    pagesInfo[pageSequentialNumber].Size = size;
                    pagesInfo[pageSequentialNumber].DiffSize = 0;
                }
                ++pageSequentialNumber;
            }

            var totalSizeWritten = write - txPageInfoPtr;

            long compressedLen = 0;

            if (performCompression)
            {
                var outputBufferSize = LZ4.MaximumOutputLength(totalSizeWritten);
                int outputBufferInPages = checked((int)((outputBufferSize + sizeof(TransactionHeader)) / Constants.Storage.PageSize +
                                                        ((outputBufferSize + sizeof(TransactionHeader)) % Constants.Storage.PageSize == 0 ? 0 : 1)));

                _maxNumberOfPagesRequiredForCompressionBuffer = Math.Max(pagesRequired + outputBufferInPages, _maxNumberOfPagesRequiredForCompressionBuffer);

                var totalSizeWrittenPlusTxHeader = totalSizeWritten + sizeof(TransactionHeader);
                var pagesWritten = (totalSizeWrittenPlusTxHeader / Constants.Storage.PageSize) +
                                   (totalSizeWrittenPlusTxHeader % Constants.Storage.PageSize == 0 ? 0 : 1);

                try
                {
                    pagerState = _compressionPager.EnsureContinuous(pagesWritten, outputBufferInPages);
                }
                catch (InsufficientMemoryException)
                {
                    // RavenDB-10830: failed to lock memory of temp buffers in encrypted db, let's create new file with initial size

                    _compressionPager.Dispose();
                    _compressionPager = CreateCompressionPager(_env.Options.InitialFileSize ?? _env.Options.InitialLogFileSize);
                    _lastCompressionBufferReduceCheck = DateTime.UtcNow;
                    throw;
                }

                tx.EnsurePagerStateReference(pagerState);
                _compressionPager.EnsureMapped(tx, pagesWritten, outputBufferInPages);

                txHeaderPtr = _compressionPager.AcquirePagePointer(tx, pagesWritten);
                var compressionBuffer = txHeaderPtr + sizeof(TransactionHeader);

                var compressionDuration = Stopwatch.StartNew();
                var path = CurrentFile?.JournalWriter?.FileName?.FullPath ?? _env.Options.GetJournalPath(Math.Max(0, _journalIndex))?.FullPath;
                using (var metrics = _env.Options.IoMetrics.MeterIoRate(path, IoMetrics.MeterType.Compression, 0)) // Note that the last journal may be replaced if we switch journals, however it doesn't affect web graph
                {
                    var compressionAcceleration = _lastCompressionAccelerationInfo.LastAcceleration;

                    compressedLen = LZ4.Encode64LongBuffer(
                        txPageInfoPtr,
                        compressionBuffer,
                        totalSizeWritten,
                        outputBufferSize,
                        compressionAcceleration);

                    metrics.SetCompressionResults(totalSizeWritten, compressedLen, compressionAcceleration);
                }
                compressionDuration.Stop();

                _lastCompressionAccelerationInfo.CompressionDuration = compressionDuration.Elapsed;
            }
            else
            {
                _maxNumberOfPagesRequiredForCompressionBuffer = Math.Max(pagesRequired, _maxNumberOfPagesRequiredForCompressionBuffer);
            }

            // We need to account for the transaction header as part of the total length.
            var totalSize = performCompression ? compressedLen : totalSizeWritten;
            var totalLength = totalSize + sizeof(TransactionHeader);
            var remainder = totalLength % (4 * Constants.Size.Kilobyte);
            int entireBuffer4Kbs = checked((int)((totalLength / (4 * Constants.Size.Kilobyte)) + (remainder == 0 ? 0 : 1)));

            if (remainder != 0)
            {
                // zero the remainder of the page
                Memory.Set(txHeaderPtr + totalLength, 0, 4 * Constants.Size.Kilobyte - remainder);
            }

            var reportedCompressionLength = performCompression ? compressedLen : -1;

            // Debug.Assert(txHeaderPtr != null);

            var txHeader = tx.GetTransactionHeader();
            txHeader->CompressedSize = reportedCompressionLength;
            txHeader->UncompressedSize = totalSizeWritten;
            txHeader->PageCount = numberOfPages;
            if (_env.Options.EncryptionEnabled == false)
            {
                if (performCompression)
                    txHeader->Hash = Hashing.XXHash64.Calculate(txHeaderPtr + sizeof(TransactionHeader), (ulong)compressedLen, (ulong)txHeader->TransactionId);
                else
                    txHeader->Hash = Hashing.XXHash64.Calculate(txPageInfoPtr, (ulong)totalSizeWritten, (ulong)txHeader->TransactionId);
            }
            else
            {
                // if encryption is enabled, we are already validating the tx using
                // the AEAD method, so no need to do it twice
                txHeader->Hash = 0;
            }

            var prepreToWriteToJournal = new CompressedPagesResult
            {
                Base = txHeaderPtr,
                NumberOf4Kbs = entireBuffer4Kbs,
                NumberOfUncompressedPages = pagesCountIncludingAllOverflowPages,
            };
            // Copy the transaction header to the output buffer. 
            Memory.Copy(txHeaderPtr, (byte*)txHeader, sizeof(TransactionHeader));
            Debug.Assert(((long)txHeaderPtr % (4 * Constants.Size.Kilobyte)) == 0, "Memory must be 4kb aligned");

            if (_env.Options.EncryptionEnabled)
                EncryptTransaction(txHeaderPtr);

            return prepreToWriteToJournal;
        }

        internal static readonly byte[] Context = Encoding.UTF8.GetBytes("Txn-Acid");

        private void EncryptTransaction(byte* fullTxBuffer)
        {
            var txHeader = (TransactionHeader*)fullTxBuffer;

            txHeader->Flags |= TransactionPersistenceModeFlags.Encrypted;
            ulong macLen = (ulong)Sodium.crypto_aead_xchacha20poly1305_ietf_abytes();
            var subKeyLen = Sodium.crypto_aead_xchacha20poly1305_ietf_keybytes();
            var subKey = stackalloc byte[(int)subKeyLen];
            fixed (byte* mk = _env.Options.MasterKey)
            fixed (byte* ctx = Context)
            {
                var num = txHeader->TransactionId;
                if (Sodium.crypto_kdf_derive_from_key(subKey, subKeyLen, (ulong)num, ctx, mk) != 0)
                    throw new InvalidOperationException("Unable to generate derived key");
            }

            var npub = fullTxBuffer + TransactionHeader.NonceOffset;
            Sodium.randombytes_buf(npub, (UIntPtr)TransactionHeader.NonceSize);

            var size = txHeader->CompressedSize != -1 ? txHeader->CompressedSize : txHeader->UncompressedSize;

            var rc = Sodium.crypto_aead_xchacha20poly1305_ietf_encrypt_detached(
                fullTxBuffer + TransactionHeader.SizeOf,
                fullTxBuffer + TransactionHeader.SizeOf - macLen,
                &macLen,
                fullTxBuffer + TransactionHeader.SizeOf,
                (ulong)size,
                fullTxBuffer,
                (ulong)(TransactionHeader.SizeOf - TransactionHeader.NonceOffset),
                null,
                npub,
                subKey
            );

            Debug.Assert(macLen == (ulong)Sodium.crypto_aead_xchacha20poly1305_ietf_abytes());

            if (rc != 0)
                throw new InvalidOperationException("Failed to call crypto_aead_xchacha20poly1305_ietf_encrypt, rc = " + rc);
        }

        private class CompressionAccelerationStats
        {
            public TimeSpan CompressionDuration;
            public TimeSpan WriteDuration;

            private int _lastAcceleration = 1;
            private int _flux; // allow us to ignore fluctuations by requiring several consecutive operations to change 

            public int LastAcceleration => _lastAcceleration;

            public void CalculateOptimalAcceleration()
            {
                // if comression is _much_ higher than write time, increase acceleration
                if (CompressionDuration > WriteDuration.Add(WriteDuration))
                {
                    if (_lastAcceleration < 99)
                    {
                        _lastAcceleration = Math.Min(99, _lastAcceleration + 2);
                        _flux = -4;
                    }
                    return;
                }

                if (CompressionDuration <= WriteDuration)
                {
                    // write time is higher than compression time, so compression is worth it
                    if (++_flux > 5)
                    {
                        _lastAcceleration = Math.Max(1, _lastAcceleration - 1);
                        _flux = 3;
                    }

                    return;
                }

                // compression time is _higher_ than write time. Probably fast I/O system, so we can 
                // afford to reduce the compression rate and try to get higher speeds
                if (--_flux < -5)
                {
                    _lastAcceleration = Math.Min(99, _lastAcceleration + 1);
                    _flux = -2;
                }
            }
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
            return _env.Options.CreateTemporaryBufferPager($"compression.{_compressionPagerCounter++:D10}.buffers", initialSize);
        }

        private DateTime _lastCompressionBufferReduceCheck = DateTime.UtcNow;
        private CompressionAccelerationStats _lastCompressionAccelerationInfo = new CompressionAccelerationStats();

        public void ReduceSizeOfCompressionBufferIfNeeded(bool forceReduce = false)
        {
            var initialSize = _env.Options.InitialFileSize ?? _env.Options.InitialLogFileSize;
            if (ShouldReduceSizeOfCompressionPager(initialSize, forceReduce) == false)
                return;

            // the compression pager is too large, we probably had a big transaction and now can
            // free all of that and come back to more reasonable values.
            if (forceReduce == false && _logger.IsOperationsEnabled)
            {
                _logger.Operations(
                    $"Compression buffer: {_compressionPager} has reached size {new Size(_compressionPager.NumberOfAllocatedPages * Constants.Storage.PageSize, SizeUnit.Bytes)} which is more than the initial size " +
                    $"of {new Size(initialSize, SizeUnit.Bytes)}. Will trim it now to the max size allowed. If this is happen on a regular basis," +
                    " consider raising the limit (MaxScratchBufferSize option control it), since it can cause performance issues");
            }

            _lastCompressionBufferReduceCheck = DateTime.UtcNow;

            _compressionPager.Dispose();
            _compressionPager = CreateCompressionPager(initialSize);
        }

        public void ZeroCompressionBufferIfNeeded(IPagerLevelTransactionState tx)
        {
            if (_env.Options.EncryptionEnabled == false)
                return;

            var compressionBufferSize = _compressionPager.NumberOfAllocatedPages * Constants.Storage.PageSize;
            _compressionPager.EnsureMapped(tx, 0, checked((int)_compressionPager.NumberOfAllocatedPages));
            var pagePointer = _compressionPager.AcquirePagePointer(tx, 0);
            Sodium.sodium_memzero(pagePointer, (UIntPtr)compressionBufferSize);
        }

        private bool ShouldReduceSizeOfCompressionPager(long initialSize, bool forceReduce)
        {
            var compressionBufferSize = _compressionPager.NumberOfAllocatedPages * Constants.Storage.PageSize;
            if (compressionBufferSize <= initialSize)
                return false;

            if (forceReduce)
                return true;

            if ((DateTime.UtcNow - _lastCompressionBufferReduceCheck).TotalMinutes < 5)
                return false;

            if (_maxNumberOfPagesRequiredForCompressionBuffer < _compressionPager.NumberOfAllocatedPages / 2)
            {
                // we recently used at least half of the compression buffer,
                // no point in reducing size yet, we'll try to do it again next time
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
                // called when the storage environment was idle
                ReduceSizeOfCompressionBufferIfNeeded(forceReduce: true);
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
        public JournalFile.UpdatePageTranslationTableAction? UpdatePageTranslationTable;
    }
}


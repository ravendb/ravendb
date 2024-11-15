// -----------------------------------------------------------------------
//  <copyright file="WriteAheadJournal.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using Sparrow;
using Sparrow.Binary;
using System;
using System.Buffers;
using System.Collections.Concurrent;
using System.Collections.Frozen;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Runtime.ExceptionServices;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Sparrow.Compression;
using Sparrow.Logging;
using Sparrow.Platform;
using Sparrow.Server;
using Sparrow.Server.Exceptions;
using Sparrow.Server.Meters;
using Sparrow.Server.Utils;
using Sparrow.Threading;
using Voron.Exceptions;
using Voron.Impl.FileHeaders;
using Voron.Impl.Paging;
using Voron.Impl.Scratch;
using Voron.Util;
using Constants = Voron.Global.Constants;
using NativeMemory = Sparrow.Utils.NativeMemory;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;
using System.Threading.Channels;
using Sparrow.Server.Logging;
using Sparrow.Server.LowMemory;
using Sparrow.Server.Platform;
using Voron.Data.BTrees;
using Voron.Impl.FreeSpace;
using Voron.Logging;
using static Voron.Impl.Paging.Pager;

namespace Voron.Impl.Journal
{
    public sealed unsafe class WriteAheadJournal : IDisposable
    {
        private readonly StorageEnvironment _env;
        public SharedJournalState SharedJournalState = new ();

        public record JournalStateRecord(
            LowLevelTransaction Transaction,
            TaskCompletionSource Tcs,
            Pal.journal_entry Entry);

        public record PendingJournalStateRecord(
            LowLevelTransaction Transaction,
            TaskCompletionSource Tcs,
            Pal.journal_entry Entry)
        {
            public JournalStateRecord JournalStateRecord => new JournalStateRecord(Transaction, Tcs, Entry);
        }

        private long _currentJournalFileSize;
        private DateTime _lastFile;

        private long _journalIndex = -1;

        public long CurrentJournalIndex => _journalIndex;
        
        private readonly WriteAheadJournal.JournalApplicator _journalApplicator;

        private ImmutableAppendOnlyList<JournalFile> _files = ImmutableAppendOnlyList<JournalFile>.Empty;
        internal JournalFile CurrentFile;

        private readonly HeaderAccessor _headerAccessor;
        private Pager _compressionPager;
        private Pager.State _compressionPagerState;
        private long _compressionPagerCounter;

        private readonly DiffPages _diffPage = new DiffPages();
        private readonly RavenLogger _logger;

        private readonly object _writeLock = new object();
        private int _maxNumberOfPagesRequiredForCompressionBuffer;

        private readonly DisposeOnce<SingleAttempt> _disposeRunner;

        public class LinkedJournalsRecord : IDisposable
        {
            public static readonly Guid LinkedJournalId = new("66d2ff9c-6251-462c-bde5-e05ba50110cf");
            public static readonly long TransactionIdMarker = MemoryMarshal.Read<long>("LinkJrnl"u8);
            
            private byte* _buffer;
            private int _bufferSize;
            private NativeMemory.ThreadStats _threadStats;
            private readonly List<string> _paths = new();
            private readonly List<Guid> _journalIds = new();
            private int _linksDataSize;
            private readonly Dictionary<string, long> _lastCreatedJournals = [];
            private readonly Dictionary<string, long> _lastFlushedJournals = [];

            public void Add(string relativePath, Guid journalId)
            {
                _paths.Add(relativePath);
                _journalIds.Add(journalId);

                _linksDataSize += relativePath.Length + sizeof(Guid);
            }
            
            public void RegisterNewJournal(string journal, long number) => _lastCreatedJournals[journal] =  number;

            public List<string> GetFoldersToSync(long lastJournal)
            {
                List<string> folders = null;
                foreach (var (path, number) in _lastCreatedJournals)
                {
                    ref var index = ref CollectionsMarshal.GetValueRefOrAddDefault(_lastFlushedJournals, path, out _);
                    if (index >= lastJournal)
                        continue;

                    folders ??= [];
                    folders.Add(path);
                    _lastCreatedJournals[path] = number;
                }
                return folders;
            }
            
            public bool HasEntries => _paths.Count > 0;

            public Pal.journal_entry CreateEntry()
            {
                int reqSize = checked(
                    Encoding.UTF8.GetMaxByteCount(_linksDataSize) + _paths.Count + sizeof(TransactionHeader)
                );
                if (reqSize > _bufferSize)
                {
                    if(_buffer is not null)
                        PlatformSpecific.NativeMemory.Free4KbAlignedMemory(_buffer, _bufferSize, _threadStats);

                    _bufferSize = ((reqSize - 1) / 4096 + 1) * 4096;
                    _buffer = PlatformSpecific.NativeMemory.Allocate4KbAlignedMemory(_bufferSize, out _threadStats);
                }

                Memory.Set(_buffer, 0, sizeof(TransactionHeader));
                var header = (TransactionHeader*)_buffer;
                header->JournalId = LinkedJournalId;
                header->Flags = TransactionPersistenceModeFlags.LinkedJournalsRecord;
                header->HeaderMarker = Constants.TransactionHeaderMarker;
                header->TransactionId = TransactionIdMarker;
                header->PageCount = _paths.Count;
                header->TxMarker = TransactionMarker.Commit;
                header->CompressedSize = -1;

                var data = _buffer + sizeof(TransactionHeader);
                int usableBufferSize = _bufferSize - sizeof(TransactionHeader);
                var span = new Span<byte>(data, usableBufferSize);
                for (int index = 0; index < _paths.Count; index++)
                {
                    var path = _paths[index];
                    var journalId = _journalIds[index];

                    MemoryMarshal.Cast<Guid, byte>(new Span<Guid>(ref journalId)).CopyTo(span);
                    span = span[sizeof(Guid)..];

                    int written = Encoding.UTF8.GetBytes(path, span);
                    span[written] = 0;
                    span = span[(written + 1)..];
                }

                var dataSize = usableBufferSize - span.Length;
                header->UncompressedSize = dataSize;
                header->Hash = Hashing.XXHash64.Calculate(data, dataSize, (ulong)header->TransactionId);
                
                _paths.Clear();
                _linksDataSize = 0;
                
                var actualSize = header->UncompressedSize + sizeof(TransactionHeader);
                return new Pal.journal_entry
                {
                    Base = header,
                    NumberOf4Kbs = ((actualSize - 1) / 4096 + 1) 
                };
            }

            public void Dispose()
            {
                if(_buffer is not null)
                    PlatformSpecific.NativeMemory.Free4KbAlignedMemory(_buffer, _bufferSize, _threadStats);
                _buffer = null;
            }
        }

        public ScopeForSharedJournals SharedJournalsScope()
        {
            if (_rootJournalMergedCommitsCts != null)
                throw new InvalidOperationException("Already using shared scope, can only be done once");
            _rootJournalMergedCommitsCts = new();
            return new ScopeForSharedJournals(this);
        }

        public readonly struct ScopeForSharedJournals(WriteAheadJournal journal) : IDisposable
        {
            public void Dispose()
            {
                journal?._rootJournalMergedCommitsCts?.Cancel();
                journal?.RejectCommitsToMerge();
            }
        }
        
        public WriteAheadJournal(StorageEnvironment env)
        {
            _env = env;
            _is32Bit = env.Options.ForceUsing32BitsPager || PlatformDetails.Is32Bits;
            // this must be at least 1 to ensure that we can always make forward progress
            _minimumSharedJournalsMergeCount = Math.Max(1, _env.Options.MinimumSharedJournalsMergeCount);
            _logger = RavenLogManager.Instance.GetLoggerForVoron<WriteAheadJournal>(_env.Options, env.ToString());
            _currentJournalFileSize = env.Options.InitialLogFileSize;
            _headerAccessor = env.HeaderAccessor;

            (_compressionPager, _compressionPagerState) = CreateCompressionPager(_env.Options.InitialFileSize ?? _env.Options.InitialLogFileSize);
            _journalApplicator = new WriteAheadJournal.JournalApplicator(this);

            _disposeRunner = new DisposeOnce<SingleAttempt>(() =>
            {
                RejectCommitsToMerge();

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
                _linkedJournalsRecord.Dispose();
            });
        }

        public ImmutableAppendOnlyList<JournalFile> Files => _files;

        public WriteAheadJournal.JournalApplicator Applicator => _journalApplicator;
        
        public bool HasBranchCommits => SharedJournalState.HasBranchCommits;

        private JournalFile NextFile(long numberOf4Kbs)
        {
            var now = DateTime.UtcNow;
            
            long maxLogFileSize = _env.Options.MaxLogFileSize;

            if ((now - _lastFile).TotalSeconds < 90)
            {
                _currentJournalFileSize = Math.Min(maxLogFileSize, _currentJournalFileSize * 2);
            }
            var actualLogSize = _currentJournalFileSize;
            long minRequiredSize = numberOf4Kbs * 4 * Constants.Size.Kilobyte;
            if (_currentJournalFileSize < minRequiredSize)
            {
                _currentJournalFileSize = Bits.PowerOf2(minRequiredSize);
                if (_currentJournalFileSize > maxLogFileSize)
                    _currentJournalFileSize = Math.Max(maxLogFileSize, minRequiredSize);

                actualLogSize = _currentJournalFileSize;
            }

            var journalPager = _env.Options.CreateJournalWriter(_journalIndex + 1, actualLogSize);

            // we modify the in memory state _after_ we created the file, because we have to make sure that 
            // we have created it successfully first. 
            _journalIndex++;

            _lastFile = now;

            var journal = new JournalFile(_env, journalPager, _journalIndex, FrozenSet<Guid>.Empty);
            journal.NewlyCreatedFile = true;
            journal.DoneWriting = new SingleUseFlag();
            journal.AddRef(); // one reference added by a creator - write ahead log
            journal.RegisteredEnvironments[_env] = journal;
            _files = _files.Append(journal);

            return journal;
        }

        public bool RecoverDatabase(TransactionHeader* txHeader, out long lastJournalNumber, Action<LogLevel, string> addToInitLog)
        {
            // note, we don't need to do any concurrency here, happens as a single threaded
            // fashion on db startup
            var requireHeaderUpdate = false;

            var currentFileHeader = _headerAccessor.CopyHeader();
            var logInfo = currentFileHeader.Journal;
            
            if (_env.Options.IncrementalBackupEnabled == false && _env.Options.CopyOnWriteMode == false)
            {
                // we want to check that we clean up old log files if they aren't needed
                // this is more just to be safe than anything else, they shouldn't be there.
                var unusedFiles = logInfo.LastSyncedJournal;
                while (true)
                {
                    unusedFiles--;
                    if (_env.Options.TryDeleteJournal(unusedFiles) == false)
                        break;
                }
            }

            var modifiedPages = new HashSet<long>();

            var journalFiles = new List<JournalFile>();
            long lastFlushedTxId = logInfo.LastSyncedTransactionId;
            long lastFlushedJournal = logInfo.LastSyncedJournal;
            long lastProcessedJournal = logInfo.LastSyncedJournal;

            // the last sync journal is allowed to be deleted, it might have been fully synced, which is fine
            // we rely on the lastSyncedTxId to verify correctness.
            var journalToStartReadingFrom = logInfo.LastSyncedJournal;
            if (_env.Options.JournalExists(journalToStartReadingFrom) == false &&
                logInfo.Flags.HasFlag(JournalInfoFlags.IgnoreMissingLastSyncJournal) ||
                journalToStartReadingFrom == -1)
                journalToStartReadingFrom++;

            var dataPager = _env.DataPager;
            var currentState = _env.CurrentStateRecord;
            var dataPagerState = currentState.DataPagerState;
            lastJournalNumber = -1;
            var deleteLastJournal = false;

            var lastJournal = _env.Options.GetLatestJournalNumber();
            if (lastJournal.HasValue == false && journalToStartReadingFrom == 0)
            {
                goto FinishJournalRecovery;
            }

            lastJournal ??= journalToStartReadingFrom;

            for (var journalNumber = journalToStartReadingFrom; journalNumber <= lastJournal.Value; journalNumber++)
            {
                addToInitLog?.Invoke(LogLevel.Debug, $"Recovering journal {journalNumber:#,#;;0}...");
                var initialSize = _env.Options.InitialFileSize ?? _env.Options.InitialLogFileSize;
                var journalRecoveryName = StorageEnvironmentOptions.JournalRecoveryName(journalNumber);
                try
                {
                    (Pager journalPager, Pager.State journalPagerState) = _env.Options.OpenJournalPager(journalNumber, logInfo);
                    using var _ = journalPager;
                    (Pager recoveryPager, Pager.State recoveryPagerState) = _env.Options.CreateTemporaryBufferPager(journalRecoveryName, initialSize, _env.Options.Encryption.IsEnabled);
                    using var __ = recoveryPager;

                    RecoverCurrentJournalSize(journalPagerState, out var isMoreThanMaxFileSize);
                    if (_env.Options.JournalExists(journalNumber + 1) is false)
                        deleteLastJournal = isMoreThanMaxFileSize;

                    Pager.PagerTransactionState txState = default;
                    var transactionHeader = txHeader->TransactionId == 0 ? null : txHeader;

                    List<TransactionHeader> transactionHeaders;
                    var journalReader = new JournalReader(_env, journalNumber, journalPager, journalPagerState, dataPager, recoveryPager, modifiedPages, logInfo, currentFileHeader,
                        transactionHeader);
                    try
                    {
                        transactionHeaders = journalReader.RecoverAndValidate(ref dataPagerState, ref recoveryPagerState, ref txState, _env.Options);

                        if (transactionHeaders.Count > 0)
                        {
                            lastJournalNumber = journalNumber;
                            *txHeader = transactionHeaders[^1];

                            if (lastFlushedJournal != -1 && txHeader->TransactionId < lastFlushedTxId)
                            {
                                throw new InvalidOperationException(
                                    $"After recovering {journalPager.FileName} file we got tx {txHeader->TransactionId} as the last one but it's lower than last flushed transaction - tx {lastFlushedTxId} (from {StorageEnvironmentOptions.JournalName(lastFlushedJournal)})");
                            }
                            lastFlushedTxId = txHeader->TransactionId;

                            if (journalReader.Next4Kb > 0) // only if journal has some data
                            {
                                lastFlushedJournal = journalNumber;
                            }
                            else
                            {
                                // empty journal file

                                if (transactionHeaders.Count != 0)
                                    throw new InvalidOperationException($"Got empty journal file but it has some transaction headers (count: {transactionHeaders.Count})");
                            }
                        }

                    }
                    finally
                    {
                        journalReader.Complete(ref dataPagerState, ref txState);
                        journalPager.Dispose(); // need to close it before we open the journal writer
                    }

                    
                    if (_env.Options.RootJournal is null)
                    {
                        var jrnlWriter = _env.Options.CreateJournalWriter(journalNumber, journalPagerState.TotalAllocatedSize);
                        var jrnlFile = new JournalFile(_env, jrnlWriter, journalNumber, journalReader.RecoveredJournalIds.ToFrozenSet());
                        jrnlFile.DoneWriting = new SingleUseFlag();
                        jrnlFile.InitFrom(_env, journalReader, transactionHeaders);
                        jrnlFile.AddRef(); // creator reference - write ahead log

                        journalFiles.Add(jrnlFile);
                    }

                    lastProcessedJournal = journalNumber;

                    if (journalReader.RequireHeaderUpdate) //this should prevent further load of transactions
                    {
                        requireHeaderUpdate = true;
                        break;
                    }

                    addToInitLog?.Invoke(LogLevel.Debug, $"Journal {journalNumber:#,#;;0} Recovered");

                    _env.UpdateDataPagerState(dataPagerState);
                }
                catch (InvalidJournalException)
                {
                    if (_env.Options.IgnoreInvalidJournalErrors == true)
                    {
                        addToInitLog?.Invoke(LogLevel.Warn,
                            $"Encountered invalid journal {journalNumber} @ {_env.Options}. Skipping this journal and keep going the recovery operation because '{nameof(_env.Options.IgnoreInvalidJournalErrors)}' options is set");
                        continue;
                    }

                    if (journalNumber == lastJournal.Value && logInfo.Flags.HasFlag(JournalInfoFlags.IgnoreMissingLastSyncJournal))
                    {
                        addToInitLog?.Invoke(LogLevel.Warn,
                            $"Missing journal {journalNumber} @ {_env.Options}. Skipping this journal and keep going the recovery operation because '{nameof(JournalInfoFlags.IgnoreMissingLastSyncJournal)}' flag is set");
                        continue;
                    }

                    throw;
                }
            }

FinishJournalRecovery:
            if (_env.Options.Encryption.IsEnabled == false) // for encryption, we already use AEAD, so no need
            {
                // here we want to check that the checksum on all the modified pages is valid
                // we can't do that during the journal application process because we may have modifications
                // to pages that overwrite one another. So we have to do this at the end, this will detect
                // corruption when applying journals at recovery time rather than at usage.

                if (_env.Options.SkipChecksumValidationOnDatabaseLoading == false)
                {
                    // we need to iterate from the end in order to filter out pages that was overwritten by later transaction
                    var sortedPages = modifiedPages.ToArray();

                    Array.Sort(sortedPages);

                    var overflowDetector = new RecoveryOverflowDetector();

                    addToInitLog?.Invoke(LogLevel.Debug, $"Validate checksum on {modifiedPages.Count:#,#;;0} pages");

                    var sp = Stopwatch.StartNew();

                    for (var i = sortedPages.Length - 1; i >= 0; i--)
                    {
                        var modifiedPage = sortedPages[i];

                        if (sp.Elapsed.TotalSeconds >= 60)
                        {
                            sp.Restart();
                            addToInitLog?.Invoke(LogLevel.Debug, $"Still calculating checksum... {(sortedPages.Length - i) / sortedPages.Length * 100:0.00}% ({(sortedPages.Length - i):#,#;;0} out of {sortedPages.Length:#,#;;0}");
                        }

                        Pager.PagerTransactionState state = default;
                        try
                        {
                            var ptr = (PageHeader*)dataPager.AcquirePagePointerWithOverflowHandling(dataPagerState, ref state, modifiedPage);

                            int numberOfPages = Paging.Paging.GetNumberOfPages(ptr);

                            if (overflowDetector.IsOverlappingAnotherPage(modifiedPage, numberOfPages))
                            {
                                // if page is overlapping an already validated page it means this one was freed, we must not check it
                                continue;
                            }

                            _env.ValidateInMemoryPageChecksum(modifiedPage, ptr);

                            overflowDetector.SetPageChecked(modifiedPage);
                        }
                        finally
                        {
                            state.InvokeDispose(_env, ref dataPagerState, ref state);
                        }
                    }

                    sp.Stop();
                    addToInitLog?.Invoke(LogLevel.Debug, $"Validate of {sortedPages.Length:#,#;;0} pages completed in {sp.Elapsed}");
                }
                else
                {
                    addToInitLog?.Invoke(LogLevel.Debug, $"SkipChecksumValidationOnDbLoading set to true. Skipping checksum validation of {modifiedPages.Count:#,#;;0} pages.");
                }
            }

            if (lastFlushedTxId < 0)
                VoronUnrecoverableErrorException.Raise(_env,
                    "First transaction initializing the structure of Voron database is corrupted. Cannot access internal database metadata. Create a new database to recover.");

            Debug.Assert(lastFlushedTxId >= 0);
            // Debug.Assert(lastFlushedJournal >= 0); explicitly commented - it's valid state to not flush any pages from processed journal if we had already everything synced or journal was empty
            Debug.Assert(lastProcessedJournal >= 0);

            if (journalFiles.Count > 0)
            {
                var toDelete = new List<JournalFile>();

                foreach (var journalFile in journalFiles)
                {
                    if (journalFile.Number < lastProcessedJournal)
                    {
                        _journalApplicator.AddJournalToDelete(journalFile);
                        toDelete.Add(journalFile);
                    }
                    else if (deleteLastJournal)
                    {
                        Debug.Assert(_env.Options.JournalExists(journalFile.Number) && 
                                     _env.Options.JournalExists(journalFile.Number +1) is false);
                        
                        _journalApplicator.AddJournalToDelete(journalFile);
                        toDelete.Add(journalFile);
                    }
                    else
                    {
                        journalFile.RegisteredEnvironments[_env] = journalFile;
                        _files = _files.Append(journalFile);
                    }
                }

                var instanceOfLastFlushedJournal = journalFiles.FirstOrDefault(x => x.Number == lastFlushedJournal);
                // last flushed journal might not exist because it could be already deleted and the only journal we have is empty
                if (instanceOfLastFlushedJournal != null)
                {
                    _journalApplicator.SetLastFlushed(lastFlushedTxId, instanceOfLastFlushedJournal, toDelete, null);
                }
            }

            _journalIndex = lastProcessedJournal;

            if (_files.Count > 0)
            {
                for (int i = 0; i < _files.Count-1; i++)
                {
                    _files[i].DoneWriting.Raise();
                }
                var lastFile = _files[^1];
                if (lastFile.GetAvailable4Kbs(_env.CurrentStateRecord) >= 2 &&
                    lastFile.HasLegacyTransaction is false)
                    // it must have at least one page for the next transaction header and one 4kb for data
                {
                    CurrentFile = lastFile;
                }
                else
                {
                    lastFile.DoneWriting.Raise();
                }
            }

            addToInitLog?.Invoke(LogLevel.Debug, $"Info: Current File = '{CurrentFile?.Number}', Position (4KB)='{CurrentFile?.GetWritePosIn4KbPosition(_env.CurrentStateRecord)}'. Require Header Update = {requireHeaderUpdate}");

            if (requireHeaderUpdate)
            {
                // we didn't process all journals due to encountered errors
                // we must delete all the journals after the last processed one

                var nextJournalsToDelete = lastProcessedJournal +1;
                while (_env.Options.TryDeleteJournal(nextJournalsToDelete))
                    nextJournalsToDelete++;
                
                if (CurrentFile != null)
                {
                    // we're gonna have further writes to a partially recovered journal
                    // there might be more transactions already there (even with valid hash) that we didn't apply
                    // in order to avoid false positive recovery errors next time (if the journal will still exist)
                    // let's erase not processed transactions that are gonna be overwritten anyway

                    const long fourKb = 4L * Constants.Size.Kilobyte;

                    var ptr = PlatformSpecific.NativeMemory.Allocate4KbAlignedMemory( fourKb, out var threadStats);

                    try
                    {
                        Memory.Set(ptr, 0, fourKb);

                        Span<Pal.journal_entry> entries = stackalloc Pal.journal_entry[1];
                        entries[0].NumberOf4Kbs = 1;
                        entries[0].Base = ptr;
                        for (long pos = CurrentFile.GetWritePosIn4KbPosition(_env.CurrentStateRecord); pos < CurrentFile.JournalWriter.NumberOfAllocated4Kb; pos++)
                        {
                            CurrentFile.JournalWriter.Write(pos, entries, 1);
                        }
                    }
                    finally
                    {
                        PlatformSpecific.NativeMemory.Free4KbAlignedMemory(ptr, fourKb, threadStats);
                    }
                }
            }

            if (_env.Options.CopyOnWriteMode == false)
            {
                addToInitLog?.Invoke(LogLevel.Debug, $"Cleanup Newer Invalid Journal Files (Last Flushed Journal={lastProcessedJournal})");

                CleanupNewerInvalidJournalFiles(lastProcessedJournal);
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

        private void RecoverCurrentJournalSize(Pager.State state, out bool isMoreThanMaxFileSize)
        {
            var journalSize = Bits.PowerOf2(state.TotalAllocatedSize);
            if (journalSize >= _env.Options.MaxLogFileSize) // can't set for more than the max log file size
            {
                isMoreThanMaxFileSize = true;
                return;
            }

            // this set the size of the _next_ journal file size
            _currentJournalFileSize = Math.Min(journalSize, _env.Options.MaxLogFileSize);
            isMoreThanMaxFileSize = false;
        }

        public void Dispose()
        {
            _disposeRunner.Dispose();
        }

        public JournalInfo GetCurrentJournalInfo()
        {
            return _headerAccessor.Get((in FileHeader header) => header.Journal);
        }

        public sealed class JournalApplicator : IDisposable
        {
            private readonly ConcurrentDictionary<long, JournalFile> _journalsToDelete = new();
            private readonly object _flushingLock = new();
            private readonly SemaphoreSlim _fsyncLock = new(1);
            private readonly WriteAheadJournal _waj;
            private readonly ManualResetEventSlim _onWriteTransactionCompleted = new();
            private readonly LockTaskResponsible _flushLockTaskResponsible;

            public int FlushInProgress;

            public sealed record LastFlushState(long TransactionId, 
                JournalFile Journal, 
                List<JournalFile> JournalsToDelete,
                List<string> PathsToSync,
                TreeRootHeader Root,
                long LastPageNumber)
            {
                public readonly SingleUseFlag DoneFlag = new();

                public long JournalId => Journal?.Number ?? -1;
                public bool IsValid => Journal != null && JournalsToDelete != null;

                public static LastFlushState Empty => new(-1, null, null, null, default, -1);
            }

            private LastFlushState _lastFlushed = LastFlushState.Empty;
            private long _totalWrittenButUnsyncedBytes;
            private bool _ignoreLockAlreadyTaken;
            private Action<LowLevelTransaction> _updateJournalStateAfterFlush;
            private DateTime _lastFlushTime;
            private DateTime _lastSyncTime;

            public bool HasUpdateJournalStateAfterFlush => _updateJournalStateAfterFlush != null;

            public void AddJournalToDelete(JournalFile journal)
            {
                _journalsToDelete[journal.Number] = journal;
            }

            public void OnTransactionCommitted(LowLevelTransaction tx)
            {
                var action = _updateJournalStateAfterFlush;
                action?.Invoke(tx);
            }

            public void OnTransactionCompleted(LowLevelTransaction tx)
            {
                // we are getting the transaction here just to verify that the write lock is held
                Debug.Assert(tx.Flags is TransactionFlags.ReadWrite);
                if (tx.Committed && tx.AppliedJournalStateAfterFlush)
                {
                    _updateJournalStateAfterFlush = null;
                }
            }

            public void AfterTransactionWriteLockReleased()
            {
                _onWriteTransactionCompleted.Set();
            }

            public long LastFlushedTransactionId => _lastFlushed.TransactionId;
            public long LastFlushedJournalId => _lastFlushed.JournalId;
            public long TotalWrittenButUnsyncedBytes => Interlocked.Read(ref _totalWrittenButUnsyncedBytes);

            internal int TotalCommittedSinceLastFlushPages;
            internal bool ShouldFlush => TotalCommittedSinceLastFlushPages != 0 || _lastFlushed.TransactionId != _waj._env.CurrentReadTransactionId;

            public bool ShouldSync => TotalWrittenButUnsyncedBytes != 0;
            public int JournalsToDeleteCount => _journalsToDelete.Count;
            public JournalFile[] JournalsToDelete => _journalsToDelete.Values.ToArray();

            public DateTime LastFlushTime => _lastFlushTime;
            public DateTime LastSyncTime => _lastSyncTime;


            public JournalApplicator(WriteAheadJournal waj)
            {
                _waj = waj;
                _flushLockTaskResponsible = new LockTaskResponsible(_flushingLock, waj._env.Token)
                {
#if DEBUG
                    OnBeforeLockEnter = () => ThrowOnFlushLockEnterWhileWriteTransactionLockIsTaken()
#endif
                };
            }

            private ApplyLogsToDataFileState _applyLogsToDataFileStateFromPreviousFailedAttempt;

            private bool _failedToUpdateJournalState = false;

            public void ApplyLogsToDataFile(CancellationToken token, TimeSpan timeToWait)
            {
                if (token.IsCancellationRequested)
                    return;

                if (Monitor.IsEntered(_flushingLock) && _ignoreLockAlreadyTaken == false)
                    throw new InvalidJournalFlushRequestException("Applying journals to the data file has been already requested on the same thread");

                ApplyLogsToDataFileState currentState = null;
                ByteStringContext byteStringContext = null;
                bool lockTaken = false;
                try
                {
                    ThrowOnFlushLockEnterWhileWriteTransactionLockIsTaken();

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

                    Interlocked.Exchange(ref FlushInProgress, 1);

                    if (_waj._env.Disposed)
                        return;

                    if (_failedToUpdateJournalState)
                    {
                        // we have failed to update the journal state during the last flush, the environment is considered to be in faulty state (hence it's going to be restarted)
                        // we need to refrain from executing more flushes since we cannot relay neither on in-memory nor data date of the environment

                        if (_waj._logger.IsWarnEnabled)
                        {
                            _waj._logger.Warn("Ignoring the flush due to failure in updating the journal state. This is a catastrophic failure so the environment is going to be restarted.");
                        }
                        return;
                    }

                    _forTestingPurposes?.OnApplyLogsToDataFileUnderFlushingLock?.Invoke();
                    _waj._env.ActiveTransactions.ForceRecheckingOldestTransactionByFlusherThread();
                    long uptoTxIdExclusive = _waj._env.ActiveTransactions.OldestTransaction;

                    var sparseRegionsToFlush = _waj._env.TryGetLatestSparseRegionsToFlush(uptoTxIdExclusive);
                    if (sparseRegionsToFlush != Span<(long Start, long Count)>.Empty)
                    {
                        // This needs to happen _before_ we actually write to the disk
                        // because we _first_ zero a range and then we may write data to that range (filling some of it up).
                        // That is fine, and means that we don't need to track re-uses. 
                        MarkSparseRegionsInDataFile(sparseRegionsToFlush);
                    }

                    if (_applyLogsToDataFileStateFromPreviousFailedAttempt != null)
                    {
                        // we have to keep this around since TryGetLatestEnvironmentStateToFlush will _consume_ the state
                        // so until we successfully flush the data, we need to remember to repeat this operation
                        // flushing can fail because of disk full, etc...
                    }
                    else
                    {
                        // RavenDB-13302: we need to force a re-check this before we make decisions here
                        _applyLogsToDataFileStateFromPreviousFailedAttempt = _waj._env.TryGetLatestEnvironmentStateToFlush(
                            uptoTxIdExclusive: uptoTxIdExclusive);
                        if (_applyLogsToDataFileStateFromPreviousFailedAttempt == null)
                            return; // nothing to do
                    }

                    Debug.Assert(_applyLogsToDataFileStateFromPreviousFailedAttempt is { Record: not null, Buffers: not null });
                    var currentTotalCommittedSinceLastFlushPages = TotalCommittedSinceLastFlushPages;

                    Pager.State dataPagerState;
                    try
                    {
                        byteStringContext = new ByteStringContext(SharedMultipleUseFlag.None);
                        dataPagerState = ApplyPagesToDataFileFromScratch(_applyLogsToDataFileStateFromPreviousFailedAttempt);
                    }
                    catch (Exception e) when (e is OutOfMemoryException or EarlyOutOfMemoryException)
                    {
                        if (_waj._logger.IsWarnEnabled)
                        {
                            _waj._logger.Warn("Could not allocate enough space to apply pages to data file", e);
                        }
                        // on 32 bits systems, we likely run out of address space, nothing that we can do, this should
                        // be handled by the 32 bits pager.
                        return;
                    }
                    catch (DiskFullException diskFullEx)
                    {
                        if (_waj._logger.IsErrorEnabled)
                        {
                            _waj._logger.Error("The disk is full!", diskFullEx);
                        }
                        _waj._env.HandleDataDiskFullException(diskFullEx);
                        return;
                    }

                    // we can clear this here, since we aren't handling any errors further down this function
                    // any error from here on out is a catastrophic failure, and will be handled by recovering from 
                    // scratch
                    currentState = _applyLogsToDataFileStateFromPreviousFailedAttempt;
                    _applyLogsToDataFileStateFromPreviousFailedAttempt = null;

                    Interlocked.Add(ref TotalCommittedSinceLastFlushPages, -currentTotalCommittedSinceLastFlushPages);

                    try
                    {
                        ApplyJournalStateAfterFlush(token, currentState.Buffers, currentState.Record, dataPagerState, byteStringContext);
                    }
                    catch (Exception)
                    {
                        _failedToUpdateJournalState = true;
                        throw;
                    }
                    
                    _waj._env.SuggestSyncDataFile();
                }
                finally
                {
                    if (_applyLogsToDataFileStateFromPreviousFailedAttempt == null) // cannot clear this if we are going to re-use it next time 
                    {
                        currentState?.Buffers.Clear();
                    }
                    byteStringContext?.Dispose();
                    if (lockTaken)
                    {
                        Interlocked.Exchange(ref FlushInProgress, 0);
                        Monitor.Exit(_flushingLock);
                    }
                }

                _waj._env.LogsApplied();
            }

            private void ApplyJournalStateAfterFlush(CancellationToken token,
                List<PageFromScratchBuffer> bufferOfPageFromScratchBuffersToFree,
                EnvironmentStateRecord record,
                Pager.State dataPagerState,
                ByteStringContext byteStringContext)
            {
                // the idea here is that even though we need to run the journal through its state update under the transaction lock
                // we don't actually have to do that in our own transaction, what we'll do is to setup things so if there is a running
                // write transaction, we'll piggy back on its commit to complete our process, without interrupting its work
                var transactionPersistentContext = new TransactionPersistentContext(true);
                _onWriteTransactionCompleted.Reset();
                ExceptionDispatchInfo edi = null;
                var sp = Stopwatch.StartNew();

                var executedSuccessfully = false;

                var applied = WaitForJournalStateToBeUpdated(token, transactionPersistentContext, txw =>
                {
                    try
                    {
                        txw.AppliedJournalStateAfterFlush = true;
                        txw.UpdateDataPagerState(dataPagerState);
                        UpdateJournalStateUnderWriteTransactionLock(txw, bufferOfPageFromScratchBuffersToFree, record);

                        executedSuccessfully = true;

                        if (_waj._logger.IsDebugEnabled)
                            _waj._logger.Debug($"Updated journal state under write tx lock (txId: {txw.Id}) after waiting for {sp.Elapsed}");
                    }
                    catch (Exception e)
                    {
                        if (_waj._logger.IsWarnEnabled)
                            _waj._logger.Warn($"Failed to update journal state under write tx lock (waited - {sp.Elapsed})", e);

                        edi = ExceptionDispatchInfo.Capture(e);
                        throw;
                    }
                }, byteStringContext);

                if (edi != null)
                    edi.Throw();
                else if (applied && executedSuccessfully == false)
                    throw new InvalidOperationException($"Journal state was not applied successfully after the flush (waited - {sp.Elapsed}, last flushed tx: id - {record.TransactionId}, written to journal - {record.FlushedToJournal})");
            }

            private bool WaitForJournalStateToBeUpdated(CancellationToken token, TransactionPersistentContext transactionPersistentContext,
                Action<LowLevelTransaction> currentAction, ByteStringContext byteStringContext)
            {
                _forTestingPurposes?.OnWaitForJournalStateToBeUpdated_BeforeAssigning_updateJournalStateAfterFlush?.Invoke();

                Interlocked.Exchange(ref _updateJournalStateAfterFlush, currentAction);

                _forTestingPurposes?.OnWaitForJournalStateToBeUpdated_AfterAssigning_updateJournalStateAfterFlush?.Invoke();

                do
                {
                    LowLevelTransaction txw = null;
                    try
                    {
                        try
                        {
                            txw = _waj._env.NewLowLevelTransaction(transactionPersistentContext,
                                TransactionFlags.ReadWrite, timeout: TimeSpan.Zero, context: byteStringContext);
                        }
                        catch (OperationCanceledException)
                        {
                            return false; // we disposed the server
                        }
                        catch (TimeoutException)
                        {
                            // couldn't get the transaction lock, we'll wait for the running transaction to complete
                            // for a bit, and then try again

                            _flushLockTaskResponsible.RunTaskIfNotAlreadyRan();

                            // 2 options here:
                            // - we got a notification after the transaction was committed, in which case 
                            //   _updateJournalStateAfterFlush was set to null while it was holding the write tx lock
                            //   and we'll exit (from the while)
                            // - we got a notification that the transaction is over (for any reason)
                            //   and we'll try to acquire the write tx lock again

                            var satisfiedIndex = WaitHandle.WaitAny(new[] { _onWriteTransactionCompleted.WaitHandle, token.WaitHandle }, TimeSpan.FromMilliseconds(250));

                            switch (satisfiedIndex)
                            {
                                case 0:
                                    // once we get a signal (_onWriteTransactionCompleted), we should be able to acquire the write tx lock since we prevent new write transactions.
                                    // this is just a precaution in order to prevent a loop here if the implementation will change in the future.
                                    _onWriteTransactionCompleted.Reset();
                                    continue;

                                case 1:
                                    // cancellation token
                                    return false;

                                case WaitHandle.WaitTimeout:
                                    // timeout
                                    continue;

                                default:
                                    throw new InvalidOperationException($"Unknown handle at index: {satisfiedIndex}");
                            }
                        }

                        // here we rely on the Commit() invoking the _updateJournalStateAfterFlush call as part of its work
                        txw.Commit();
                    }
                    finally
                    {
                        txw?.Dispose();
                    }
                    // if it was changed, this means that we are done - note that it could be applied by the commit of another write transaction
                } while (currentAction == _updateJournalStateAfterFlush);

                return true;
            }

            private void UpdateJournalStateUnderWriteTransactionLock(LowLevelTransaction txw,
                List<PageFromScratchBuffer> bufferOfPageFromScratchBuffersToFree,
                EnvironmentStateRecord flushedRecord)
            {
                _forTestingPurposes?.OnUpdateJournalStateUnderWriteTransactionLock?.Invoke();

                JournalFile journalFile = _waj._files.FirstOrDefault(x => x.Number == flushedRecord.FlushedToJournal);
                if (journalFile is null)
                {
                    throw new InvalidOperationException($"Unable to find journal file {flushedRecord.FlushedToJournal} in {_waj._env.DataPager.FileName}");
                }

                var unusedJournals = new List<JournalFile>();
                if (_waj._env.HasAdditionalTransactionsToFlush)
                {
                    _waj._files = _waj._files.RemoveWhile(x =>
                    {
                        if (x.Number < flushedRecord.FlushedToJournal)
                            return true;
                        return x.Number == flushedRecord.FlushedToJournal && x.GetAvailable4Kbs(flushedRecord) == 0;
                    }, unusedJournals);
                }
                else
                {
                    // this means that there are no additional transactions to flush, 
                    // and since we are under the write lock, there are no new transactions 
                    // created, so we can skip the latest journal state, since it may have
                    // been updated using a branch env without creating a real transaction
                    // on the root, or the root may have completed writing to the journal
                    // without updating the branch. So we rely on the done writing flag
                    // to tell us that the file is no longer meant for writing and since
                    // there are no pending transactions to flush, we can safely delete
                    // all journals that are done writing
                    _waj._files = _waj._files.RemoveWhile(x => x.DoneWriting.IsRaised(), unusedJournals);
                }

                if (_waj._logger.IsDebugEnabled)
                {
                    _waj._logger.Debug($"Detected {unusedJournals.Count} unused journals after flush ({nameof(flushedRecord.TransactionId)} - {flushedRecord.TransactionId}). " +
                                          $"Journals to delete: {string.Join(',', unusedJournals.Select(x => x.Number.ToString()))}");
                }

                foreach (var unused in unusedJournals)
                {
                    AddJournalToDelete(unused);
                }

                List<string> pathsToSync = null;
                if (unusedJournals.Count > 0)
                {
                    pathsToSync = _waj._linkedJournalsRecord.GetFoldersToSync(unusedJournals[^1].Number);
                }

                SetLastFlushed(flushedRecord.TransactionId, journalFile,
                    _journalsToDelete.Values.ToList(), pathsToSync);

                if (_waj._files.Count == 0)
                {
                    _waj.CurrentFileIsDone();
                }

                var scratchBufferPool = _waj._env.ScratchBufferPool;
                if (scratchBufferPool == null)
                    throw new ArgumentNullException(nameof(scratchBufferPool));
                if (bufferOfPageFromScratchBuffersToFree == null)
                    throw new ArgumentNullException(nameof(bufferOfPageFromScratchBuffersToFree));
#if DEBUG
                var freedUpToTx = long.MinValue;
                var minScratchNumber = int.MaxValue;
                var maxScratchNumber = int.MinValue;
#endif

                foreach (var pageFromScratchBuffer in bufferOfPageFromScratchBuffersToFree)
                {
                    if (pageFromScratchBuffer == null)
                        throw new ArgumentNullException(nameof(pageFromScratchBuffer));
                    if (pageFromScratchBuffer.File == null)
                        throw new ArgumentNullException(nameof(pageFromScratchBuffer.File));

                    scratchBufferPool.Free(txw, pageFromScratchBuffer.File.Number, pageFromScratchBuffer.PositionInScratchBuffer);

#if DEBUG
                    freedUpToTx = long.Max(freedUpToTx, pageFromScratchBuffer.AllocatedInTransaction);
                    maxScratchNumber = int.Max(maxScratchNumber, pageFromScratchBuffer.File.Number);
                    minScratchNumber = int.Min(minScratchNumber, pageFromScratchBuffer.File.Number);
#endif
                }

#if DEBUG
                for (int i = minScratchNumber; i <= maxScratchNumber; i++)
                {
                    var scratchBufferFile = scratchBufferPool.GetScratchBufferFile(i);

                    if (scratchBufferFile == null)
                        continue;

                    scratchBufferFile.AssertNoPagesAllocatedInTransactionOlderThan(freedUpToTx);
                }
#endif
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
                    ThrowOnFlushLockEnterWhileWriteTransactionLockIsTaken();

                    Monitor.Enter(_flushingLock);// reacquire the lock
                }
            }

            private TestingStuff _forTestingPurposes;

            internal TestingStuff ForTestingPurposesOnly()
            {
                if (_forTestingPurposes != null)
                    return _forTestingPurposes;

                return _forTestingPurposes = new TestingStuff();
            }

            internal sealed class TestingStuff
            {
                internal Action OnUpdateJournalStateUnderWriteTransactionLock;

                internal Action OnApplyLogsToDataFileUnderFlushingLock;

                internal Action OnWaitForJournalStateToBeUpdated_BeforeAssigning_updateJournalStateAfterFlush;

                internal Action OnWaitForJournalStateToBeUpdated_AfterAssigning_updateJournalStateAfterFlush;
            }

            // This can take a LONG time, and it needs to run concurrently with the
            // rest of the system, so in order to handle this properly, we do:
            // 1) Take the flushing lock (if we fail, we'll requeue for the sync)
            // 2) Take a snapshot of the current status of this env flushing status
            // 3) Release the lock & sync the file (take a long time)
            // 4) Re-take the lock, update the sync status in the header with the values we snapshotted
            public sealed class SyncOperation(WriteAheadJournal.JournalApplicator parent) : IDisposable
            {
                bool _fsyncLockTaken;
                private LastFlushState _lastFlushed;
                long _currentTotalWrittenBytes = -1;
                private readonly TaskCompletionSource<object> _tcs = new(TaskCreationOptions.RunContinuationsAsynchronously);

                public Task Task => _tcs.Task;

                internal Action AfterGatherInformationAction;

                public bool SyncDataFile()
                {
                    _fsyncLockTaken = parent._fsyncLock.Wait(0);
                    if (_fsyncLockTaken == false)
                    {
                        // probably another sync taking place right now, let us schedule another one, just in case
                        parent._waj._env.SuggestSyncDataFile();
                        return false;
                    }

                    if (parent._flushLockTaskResponsible.WaitForTaskToBeDone(GatherInformationToStartSync) == false)
                        return false;

                    AfterGatherInformationAction?.Invoke();

                    if (parent._waj._env.Disposed)
                        return false;

                    CallPagerSync();
                    
                    if (_lastFlushed.PathsToSync is not null)
                    {
                        var record = parent._waj._env.CurrentStateRecord;
                        var rc = Pal.rvn_sync_directories(record.DataPagerState.Handle, 
                            _lastFlushed.PathsToSync.ToArray(),
                            _lastFlushed.PathsToSync.Count, out var err);
                        if(rc != PalFlags.FailCodes.Success)
                            PalHelper.ThrowLastError(rc, err, $"Failed to sync directories during flush: {string.Join(",", _lastFlushed.PathsToSync)}");
                    }

                    // can take a long time, need to check again
                    if (parent._waj._env.Disposed)
                        return false;

                    return parent._flushLockTaskResponsible.WaitForTaskToBeDone(UpdateDatabaseStateAfterSync);
                }

                private bool UpdateDatabaseStateAfterSync()
                {
                    AssertGatherInformationToStartSyncBeforeUpdate();

                    if (parent._waj._env.Disposed)
                        return false;

                    Interlocked.Add(ref parent._totalWrittenButUnsyncedBytes, -_currentTotalWrittenBytes);

                    var ignoreLastSyncJournalMissing = false;
                    foreach (var item in _lastFlushed.JournalsToDelete)
                    {
                        if (item.Number == _lastFlushed.JournalId)
                        {
                            // we are about to delete it, so safe to ignore this
                            ignoreLastSyncJournalMissing = true;
                            break;
                        }
                    }

                    parent.UpdateFileHeaderAfterDataFileSync(_lastFlushed, ignoreLastSyncJournalMissing);

                    foreach (var toDelete in _lastFlushed.JournalsToDelete)
                    {
                        if (toDelete.Number > _lastFlushed.JournalId) // precaution
                            continue;
                        
                        if (parent._waj._env.Options.IncrementalBackupEnabled == false)
                            toDelete.ShouldDelete = true;

                        parent._journalsToDelete.TryRemove(toDelete.Number, out _);
                        toDelete.Release();
                    }

                    parent._lastSyncTime = DateTime.UtcNow;

                    return true;
                }

                [Conditional("DEBUG")]
                private void AssertGatherInformationToStartSyncBeforeUpdate()
                {
                    if (_lastFlushed == null && _currentTotalWrittenBytes == -1)
                    {
                        throw new InvalidOperationException(
                            $"Try to {nameof(UpdateDatabaseStateAfterSync)} without calling {nameof(GatherInformationToStartSync)} before");
                    }
                }

                private void CallPagerSync()
                {
                    // We do the sync _outside_ of the lock, letting the rest of the stuff proceed
                    var sp = Stopwatch.StartNew();
                    var dataPager = parent._waj._env.DataPager;
                    var currentStateRecord = parent._waj._env.CurrentStateRecord;
                    var dataPagerState = currentStateRecord.DataPagerState;
                    dataPager.Sync(dataPagerState, Interlocked.Read(ref parent._totalWrittenButUnsyncedBytes));
                    if (parent._waj._logger.IsDebugEnabled)
                    {
                        var sizeInKb = (dataPagerState.NumberOfAllocatedPages * Constants.Storage.PageSize) / Constants.Size.Kilobyte;
                        parent._waj._logger.Debug(
                            $"Sync of {sizeInKb:#,#0} kb file with {_currentTotalWrittenBytes / Constants.Size.Kilobyte:#,#0} kb dirty in {sp.Elapsed}");
                    }
                }

                private bool GatherInformationToStartSync()
                {
                    if (parent._waj._env.Disposed)
                        return false; // we have already disposed, nothing to do here

                    _lastFlushed = parent._lastFlushed;

                    if (_lastFlushed.IsValid == false)
                        return false;

                    if (_lastFlushed.DoneFlag.IsRaised())
                        // nothing was flushed since we last synced, nothing to do
                        return false;

                    _currentTotalWrittenBytes = Interlocked.Read(ref parent._totalWrittenButUnsyncedBytes);

                    if (_lastFlushed.Journal.LastTransactionId == -1)
                        return false;

                    _lastFlushed.DoneFlag.Raise();

                    return true;
                }

                public void Dispose()
                {
                    if (_fsyncLockTaken)
                        parent._fsyncLock.Release();
                }
            }

            internal sealed class LockTaskResponsible
            {
                private readonly object _lock;
                private readonly CancellationToken _token;
                private AssignedTask _active;
                private readonly ManualResetEventSlim _waitForTaskToBeDone = new ManualResetEventSlim();

                private sealed class AssignedTask
                {
                    public readonly Func<bool> Task;
                    public readonly SingleUseFlag DoneFlag = new SingleUseFlag();
                    public Exception Error;
                    public volatile bool Result = true;

                    public AssignedTask(Func<bool> task) => Task = task;
                }

                public LockTaskResponsible(object @lock, CancellationToken token)
                {
                    _lock = @lock;
                    _token = token;
                }

#if DEBUG
                public Action OnBeforeLockEnter { get; set; }
#endif

                public bool WaitForTaskToBeDone(Func<bool> task)
                {
                    var current = new AssignedTask(task);
                    try
                    {
                        while (true)
                        {
                            var isAssigned = Interlocked.CompareExchange(ref _active, current, null) == null;
                            if (isAssigned)
                                break;

                            if (_waitForTaskToBeDone.Wait(TimeSpan.FromMilliseconds(250), _token))
                            {
                                _waitForTaskToBeDone.Reset();
                            }
                        }

                        while (true)
                        {
                            var isLockTaken = false;
#if DEBUG
                            OnBeforeLockEnter?.Invoke();
#endif
                            Monitor.TryEnter(_lock, 0, ref isLockTaken);
                            if (isLockTaken)
                            {
                                try
                                {
                                    RunTaskIfNotAlreadyRan();
                                }
                                finally
                                {
                                    Monitor.Exit(_lock);
                                }
                            }

                            if (_waitForTaskToBeDone.Wait(TimeSpan.FromMilliseconds(250), _token))
                            {
                                _waitForTaskToBeDone.Reset();
                            }

                            if (current.DoneFlag.IsRaised())
                            {
                                if (current.Error != null)
                                    throw new InvalidOperationException("The lock task failed", current.Error);
                                return current.Result;
                            }
                        }
                    }
                    catch (OperationCanceledException)
                    {
                        return false;
                    }
                }

                public void RunTaskIfNotAlreadyRan()
                {
                    AssertRunTaskWithLock();
                    var current = Interlocked.Exchange(ref _active, null);
                    if (current == null)
                        return;
                    try
                    {
                        _token.ThrowIfCancellationRequested();
                        current.Result = current.Task();
                    }
                    catch (Exception e)
                    {
                        current.Error = e;
                    }
                    finally
                    {
                        current.DoneFlag.Raise();
                        _waitForTaskToBeDone.Set();
                    }
                }

                [Conditional("DEBUG")]
                private void AssertRunTaskWithLock()
                {
                    if (Monitor.IsEntered(_lock))
                        return;

                    throw new InvalidOperationException("The task has to be under the lock");
                }
            }

            private Pager.State ApplyPagesToDataFileFromScratch(ApplyLogsToDataFileState state)
            {
                long written = 0;
                var sp = Stopwatch.StartNew();
                var options = _waj._env.Options;
                var dataPager = _waj._env.DataPager;
                var currentStateRecord = _waj._env.CurrentStateRecord;
                var dataPagerState = currentStateRecord.DataPagerState;
                var record = state.Record;
                using (var meter = options.IoMetrics.MeterIoRate(dataPager.FileName, IoMetrics.MeterType.DataFlush, 0))
                {
                    var pagesBuffer = ArrayPool<Pal.page_to_write>.Shared.Rent(record.ScratchPagesTable.Count);
                    Pager.PagerTransactionState txState = default;
                    try
                    {
                        Span<Pal.page_to_write> pages = GetSortedPages(ref txState, record, pagesBuffer, out written);
                        if (pages.IsEmpty)
                            return dataPagerState;
                        dataPager.EnsureContinuous(ref dataPagerState, pages[^1].page_num, pages[^1].count_of_pages);
                        (dataPagerState.TotalFileSize, dataPagerState.TotalDiskSpace) = dataPager.GetFileSize(dataPagerState);
                        fixed (Pal.page_to_write* ptr = pages)
                        {
                            var rc = dataPager.Write(dataPagerState.Handle, ptr, pages.Length,  out var errorCode);
                            if (rc != PalFlags.FailCodes.Success)
                            {
                                Pager.RaiseError(dataPager.FileName, errorCode, rc, dataPagerState.TotalAllocatedSize);
                            }
                        }
                    }
                    finally
                    {
                        ArrayPool<Pal.page_to_write>.Shared.Return(pagesBuffer);
                        txState.InvokeDispose(_waj._env, ref dataPagerState, ref txState);
                    }

                    meter.SetFileSize(dataPagerState.TotalAllocatedSize);
                    meter.IncrementSize(written);
                }

                if (_waj._logger.IsDebugEnabled)
                    _waj._logger.Debug($"Flushed {record.ScratchPagesTable.Count:#,#} pages to {dataPager.FileName} with {new Size(written, SizeUnit.Bytes)} in {sp.Elapsed}.");
                else if (_waj._logger.IsWarnEnabled && sp.Elapsed > options.LongRunningFlushingWarning)
                    _waj._logger.Warn($"Very long data flushing. It took {sp.Elapsed} to flush {record.ScratchPagesTable.Count:#,#} pages to {dataPager.FileName} with {new Size(written, SizeUnit.Bytes)}.");

                Interlocked.Add(ref _totalWrittenButUnsyncedBytes, written);

                return dataPagerState;
            }

            private void MarkSparseRegionsInDataFile(Span<(long Start, long Count)> sparseRegions)
            {
                var currentStateRecord = _waj._env.CurrentStateRecord;
                var dataPagerState = currentStateRecord.DataPagerState;
             
                foreach (var (start, count) in sparseRegions)
                {
                    _waj._env.DataPager.SetSparseRange(dataPagerState,
                        start * Constants.Storage.PageSize,
                        count * Constants.Storage.PageSize
                    );
                }
            }

            private void MarkSparseRegionsInDataFile(Pager.State dataPagerState, Span<long> sparseRegions)
            {
                var count = Sorting.SortAndRemoveDuplicates(sparseRegions);
                var start = sparseRegions[0];
                var len = FreeSpaceHandling.NumberOfPagesInSection; 
                for (int i = 1; i < count; i++)
                {
                    if (start + len == sparseRegions[i])
                    {
                        len += FreeSpaceHandling.NumberOfPagesInSection;
                        continue;
                    }
                    MarkSparseRegion();

                    start = sparseRegions[i];
                    len = FreeSpaceHandling.NumberOfPagesInSection;
                }
                
                MarkSparseRegion();

                void MarkSparseRegion()
                {
                    _waj._env.DataPager.SetSparseRange(dataPagerState, 
                        start * Constants.Storage.PageSize,
                        len *Constants.Storage.PageSize
                    );
                }
            }

            private Span<Pal.page_to_write> GetSortedPages(ref Pager.PagerTransactionState txState, EnvironmentStateRecord record,
                Pal.page_to_write[] pagesBuffer, out long written)
            {
                int index = 0;
                written = 0;
                var lastFlushedTx = _lastFlushed.TransactionId;
                foreach (var (pageNum, pageValue) in record.ScratchPagesTable)
                {
                    if (lastFlushedTx >= pageValue.AllocatedInTransaction)
                        continue; // We already wrote those pages to disk in a previous flush... 

                    Debug.Assert(pageValue.AllocatedInTransaction <= record.TransactionId, "pageValue.AllocatedInTransaction <= record.TransactionId");
                    
                    var page = PreparePage(ref txState, pageValue);
                    int countOfPages = page.GetNumberOfPages();
                    written += countOfPages * Constants.Storage.PageSize;
                    pagesBuffer[index++] = new Pal.page_to_write
                    {
                        page_num = page.PageNumber,
                        ptr = page.Pointer,
                        count_of_pages = countOfPages
                    };
                    Debug.Assert(pageNum == page.PageNumber, "pageNum == page.PageNumber");
                }
                var pages = new Span<Pal.page_to_write>(pagesBuffer, 0, index);
                pages.Sort();
                return pages;
            }

            private Page PreparePage(ref Pager.PagerTransactionState txState, PageFromScratchBuffer pageValue)
            {
                byte* page = pageValue.ReadRaw(ref txState);

                if (_waj._env.Options.Encryption.IsEnabled == false)
                {
                    PageHeader* pageHeader = ((PageHeader*)page);
                    var checksum = StorageEnvironment.CalculatePageChecksum(page, pageHeader->PageNumber, out var expectedChecksum);
                    if (checksum != expectedChecksum)
                        ThrowInvalidChecksumOnPageFromScratch(pageValue.File.Number, pageValue, pageHeader, checksum, expectedChecksum);
                }

                return new Page(page);
            }

            [DoesNotReturn]
            private static void ThrowInvalidChecksumOnPageFromScratch(int scratchNumber, PageFromScratchBuffer pagePosition, PageHeader* page, ulong checksum, ulong expectedChecksum)
            {
                var message = $"During apply logs to data, tried to copy {scratchNumber} / {pagePosition.File.Number} ({page->PageNumber}) " +
                              $"has checksum {checksum} but expected {expectedChecksum}. ";

                message += $"Page flags: {page->Flags}. ";

                if ((page->Flags & PageFlags.Overflow) == PageFlags.Overflow)
                    message += $"Overflow size: {page->OverflowSize}. ";

                throw new InvalidDataException(message);
            }

            private void UpdateFileHeaderAfterDataFileSync(
                LastFlushState flushState,
                bool ignoreLastSyncJournalMissing
                )
            {
                var lastSyncedJournal = flushState.JournalId;
                var lastSyncedTransactionId = flushState.TransactionId;
                
                Debug.Assert(lastSyncedJournal != -1);
                Debug.Assert(lastSyncedTransactionId != -1);

                var treeRootHeader = flushState.Root;
                var transactionId = flushState.TransactionId;
                var lastPageNumber = flushState.LastPageNumber;

                _waj._headerAccessor.Modify((ref FileHeader header) =>
                {
                    header.TransactionId = transactionId;
                    header.LastPageNumber = lastPageNumber;

                    header.Journal.LastSyncedJournal = lastSyncedJournal;
                    header.Journal.LastSyncedTransactionId = lastSyncedTransactionId;
                    header.Journal.Reserved1 = 0;
                    for (int i = 0; i < JournalInfo.NumberOfReservedBytes; i++)
                    {
                        header.Journal.Reserved2[i] = 0;
                    }

                    if (ignoreLastSyncJournalMissing)
                        header.Journal.Flags |= JournalInfoFlags.IgnoreMissingLastSyncJournal;
                    else
                        header.Journal.Flags &= ~JournalInfoFlags.IgnoreMissingLastSyncJournal;

                    header.Root = treeRootHeader;
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
            }

            public IDisposable TryTakeFlushingLock(ref bool lockTaken, TimeSpan? timeout = null)
            {
                ThrowOnFlushLockEnterWhileWriteTransactionLockIsTaken();

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

                ThrowOnFlushLockEnterWhileWriteTransactionLockIsTaken();

                Monitor.Enter(_flushingLock, ref lockTaken);
                _ignoreLockAlreadyTaken = true;

                return new DisposableAction(() =>
                {
                    _ignoreLockAlreadyTaken = false;
                    if (lockTaken)
                        Monitor.Exit(_flushingLock);
                });
            }

            [Conditional("DEBUG")]
            private void ThrowOnFlushLockEnterWhileWriteTransactionLockIsTaken()
            {
                var currentWriteTransactionHolder = _waj._env._currentWriteTransactionIdHolder;
                var currentTxLockCount = _waj._env._transactionWriter.CurrentCount;

                if (currentWriteTransactionHolder == System.Environment.CurrentManagedThreadId)
                {
                    throw new InvalidOperationException("The flushing lock must be taken before acquiring the write transaction lock. " +
                                                        "This check is supposed to prevent potential deadlock and guarantee the same order of taking those two locks." +
                                                        $"(Thread holding the write tx lock - Name: '{NativeMemory.GetByThreadId(currentWriteTransactionHolder)?.Name}', Id: {currentWriteTransactionHolder}. " +
                                                        $"Current thread - Id: {Thread.CurrentThread.ManagedThreadId}. " +
                                                        $"Current tx lock count: {currentTxLockCount})");
                }
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

                var logInfo = _waj._env.HeaderAccessor.Get((in FileHeader header) => header.Journal);

                if (current.Number != logInfo.LastSyncedJournal)
                    throw new InvalidOperationException(string.Format("Cannot delete current journal because it isn't last synced file. Current journal number: {0}, the last one which was synced {1}", _waj.CurrentFile?.Number ?? -1, _lastFlushed.JournalId));


                if (_waj._env.CurrentReadTransactionId < logInfo.LastSyncedTransactionId)
                    throw new InvalidOperationException("Cannot delete current journal because it hasn't synced everything up to the last write transaction");

                _waj._files = _waj._files.RemoveFront(1);
                _waj.CurrentFileIsDone();

                _waj._headerAccessor.Modify((ref FileHeader header) =>
                {
                    if (current.Number != header.Journal.LastSyncedJournal)
                    {
                        throw new InvalidOperationException($"Attempted to remove a journal ({current.Number}) that hasn't been synced yet (last synced journal: {header.Journal.LastSyncedJournal})");
                    }
                    header.Journal.Reserved1 = 0;
                    for (int i = 0; i < JournalInfo.NumberOfReservedBytes; i++)
                    {
                        header.Journal.Reserved2[i] = 0;
                    }
                    header.Journal.Flags |= JournalInfoFlags.IgnoreMissingLastSyncJournal;
                });

                current.ShouldDelete = true;
                current.Release();
            }

            public void SetLastFlushed(long lastTransactionId, JournalFile journalFile, List<JournalFile> journalFiles, List<string> pathsToSync)
            {
                var lastFlushedTxHeader = journalFile.GetLastReadTxHeader(lastTransactionId);
         
                var newState = new LastFlushState(lastTransactionId, 
                    journalFile, journalFiles, pathsToSync,
                    lastFlushedTxHeader.Root, lastFlushedTxHeader.LastPageNumber);

                if (lastTransactionId != lastFlushedTxHeader.TransactionId)
                {
                    ThrowErrorWhenSyncingDataFile(newState, lastFlushedTxHeader.TransactionId, _waj._env);
                }
                
                Interlocked.Exchange(ref _lastFlushed, newState);

                _lastFlushTime = DateTime.UtcNow;
            }
            
            
            [DoesNotReturn]
            public static void ThrowErrorWhenSyncingDataFile(LastFlushState lastFlushed, long flushedTransactionId, StorageEnvironment env)
            {
                var message =
                    $"Error syncing the data file. The last sync tx is {lastFlushed.TransactionId}, " +
                    $"but the journal's last tx id is {flushedTransactionId}, possible file corruption?";
                Guid journalId = env.HeaderAccessor.JournalId;
                var (firstTx, lastTx, count) = lastFlushed.Journal.GetTransactionStatsFor(journalId);
                    
                if (count is not 0)
                {
                    message += $" Debug details - transaction headers count: {count}, first tx: {firstTx}, last tx: {lastTx} for: {journalId}.";
                }
                else
                {
                    message += $" Debug details - journal doesn't have transaction headers for: {journalId}.";
                }
                VoronUnrecoverableErrorException.Raise(env, message);
            }
        }

        private void CurrentFileIsDone()
        {
            // Note, if this is a root/branch situation, the same
            // flag is used by all instances of this journal file
            CurrentFile?.DoneWriting.Raise();
            CurrentFile = null;
        }

        public (long NumberOfUncompressedPages, long NumberOf4Kbs) WriteToJournal(LowLevelTransaction tx)
        {
            lock (_writeLock)
            {
                // RavenDB-12854: in 32 bits locking/unlocking the memory is done separately for each mapping
                // we use temp tx for dealing with compression buffers pager to avoid locking (zeroing) it's content during tx dispose
                // because we might have another transaction already using it
                Pager.PagerTransactionState tempTxState = new() { IsWriteTransaction = true };
                long numberOfUncompressedPages = 0;
                long numberOf4Kbs = 0;
                TaskCompletionSource branchCommit = null;
                try
                {
                    var rootJournal = _env.Options.RootJournal ?? this;
                    if (_env.Options.RootJournal != null && rootJournal._rootJournalMergedCommitsCts is null)
                    {
                        throw new InvalidOperationException("Unable to commit as a branch if the root journal I'm associated with is not within a shared journal scope");
                    }

                    JournalStateRecord journalStateRecord = null;
                    if (tx.ShouldWriteTransactionChangesToJournal)
                    {
                        var start = Stopwatch.GetTimestamp();
                        var entry = PrepareToWriteToJournal(tx, ref tempTxState, out numberOfUncompressedPages);
                        branchCommit = new TaskCompletionSource();
                        numberOf4Kbs = entry.NumberOf4Kbs;
                        var pendingJournalStateRecord = new PendingJournalStateRecord(tx, branchCommit, entry);
                        journalStateRecord = pendingJournalStateRecord.JournalStateRecord;
                        if (this != rootJournal)
                        {
                            rootJournal.SharedJournalState.Enqueue(pendingJournalStateRecord);
                        }

                        if (_logger.IsDebugEnabled)
                        {
                            var elapsed = Stopwatch.GetElapsedTime(start);
                            _logger.Debug(
                                $"Preparing to write tx {tx.Id} to journal with {numberOfUncompressedPages:#,#} pages ({new Size(numberOfUncompressedPages * Constants.Storage.PageSize, SizeUnit.Bytes)}) in {elapsed} with {new Size(entry.NumberOf4Kbs * 4, SizeUnit.Kilobytes)} compressed.");
                        }
                    }

                    if (this == rootJournal)
                    {
                        WriteBuffersToJournal(tx, journalStateRecord);
                    }
                    else
                    {
                        Debug.Assert(tx.ShouldWriteTransactionChangesToJournal, "ShouldWriteTransactionChangesToJournal must be true for branch commit");
                        rootJournal.SubmitBranchJournalEntry(branchCommit?.Task);
                    }

                    if (_env.Options.Encryption.IsEnabled && _env.Options.Encryption.HasExternalJournalCompressionBufferHandlerRegistration == false)
                    {
                        ZeroCompressionBuffer(ref tempTxState);
                    }

                    ReduceSizeOfCompressionBufferIfNeeded();

                    return (numberOfUncompressedPages, numberOf4Kbs);
                }
                catch (Exception e)
                {
                    branchCommit?.TrySetException(e);
                    throw;
                }
                finally
                {
                    tempTxState.InvokeDispose(_env, ref _compressionPagerState, ref tempTxState);
                }
            }
        }

        public IJournalMerger BranchJournalMerger; 
        private void SubmitBranchJournalEntry(Task commitCompleted)
        {
            Debug.Assert(_env.Options.RootJournal is null, "_env.Options.RootJournal is null");
            {
                var token = _rootJournalMergedCommitsCts.Token;
                token.ThrowIfCancellationRequested();
            }
            var handler = BranchJournalMerger;
            if(handler == null)
                throw new InvalidOperationException($"Call to {nameof(SubmitBranchJournalEntry)} when there is no handler registered for the {nameof(BranchJournalMerger)}");
           
            handler.JournalMergeSubmitted();
            
            // here we are going to wait for the root to do the actual write to disk
            // note that we *explicitly* do NOT use the cancellation token, since 
            // we _must_ wait in the branch until the root releases us, because the 
            // may be in the middle of writing from our buffer and returning here
            // will release this memory pre-maturely
            commitCompleted.Wait();
        }

        private void WriteBuffersToJournal(LowLevelTransaction tx, JournalStateRecord rootEntry)
        {
            Debug.Assert(SharedJournalState.Entries.Length is 0 || SharedJournalState.JournalRecords.Count is 0);

            _forTestingPurposes?.OnWriteBuffersToJournal?.Invoke(SharedJournalState.MergedCommitsQueue);

            long requiredSizeIn4Kbs = 0;
            if (rootEntry != null)
            {
                SharedJournalState.PrepareForCommit(rootEntry);
                requiredSizeIn4Kbs = rootEntry.Entry.NumberOf4Kbs;
            }

            while (SharedJournalState.JournalRecords.Count < _minimumSharedJournalsMergeCount)
            {
                if (SharedJournalState.TryDequeue(out var cur) is false)
                    break;

                if (CurrentFile != null) // there is an existing journal file 
                {
                    // there is space to write the current entries, but not if we add the current entry, so flush and then create a new file
                    // note that this can also happen on the first run, when requiredSizeIn4Kbs is 0
                    if (CurrentFile.GetAvailable4Kbs(tx.CurrentStateRecord) < requiredSizeIn4Kbs + cur.Entry.NumberOf4Kbs ||
                        // This file may not be valid for this environment, so we need to create a fresh one
                        CurrentFile.IsValidFileFor(cur.Transaction.Environment) is false)
                    {
                        FlushMergedJournalEntries(tx, requiredSizeIn4Kbs);
                        requiredSizeIn4Kbs = 0;
                        CurrentFileIsDone();
                    }
                }
                // there is no file available, so we want to buffer as much as possible, but not cross
                // the maximum log file size by batching entries
                else if (requiredSizeIn4Kbs + cur.Entry.NumberOf4Kbs >= _env.Options.MaxLogFileSize)
                {
                    FlushMergedJournalEntries(tx, requiredSizeIn4Kbs);
                    requiredSizeIn4Kbs = 0;
                    CurrentFileIsDone();
                }

                requiredSizeIn4Kbs += cur.Entry.NumberOf4Kbs;
                SharedJournalState.PrepareForCommit(cur.JournalStateRecord);
            }

            FlushMergedJournalEntries(tx, requiredSizeIn4Kbs);

            if (SharedJournalState.IsEmpty is false)
            {
                // we may have bailed early to ensure low latency for
                // the root env, so we tell the merger it has more work still...
                BranchJournalMerger?.JournalMergeSubmitted();
            }
        }

        private void FlushMergedJournalEntries(LowLevelTransaction tx, long requiredSizeIn4Kbs)
        {
            if (SharedJournalState.JournalRecords.Count is 0)
                return;

            if (CurrentFile == null ||
                CurrentFile.GetAvailable4Kbs(tx.CurrentStateRecord) < requiredSizeIn4Kbs)
            {
                CurrentFileIsDone();
                CurrentFile = NextFile(requiredSizeIn4Kbs);
                if (_logger.IsDebugEnabled)
                    _logger.Debug($"New journal file created {CurrentFile.Number:D19} with size {CurrentFile.JournalSize}");
            }

            foreach (var rec in SharedJournalState.JournalRecords)
            {
                var environment = rec.Transaction.Environment;
                JournalFile journalFile = environment.Journal.EnsureRegistered(CurrentFile, out var alreadyExists);
                rec.Transaction.WrittenToJournalNumber = journalFile.Number;
                journalFile.SetTransactionFrom(rec.Entry);
                if (alreadyExists) 
                    continue;
                
                // here we need to register the new journal file link as a journal entry so on recovery, we'll
                // know to restore any dropped hard links that may have been lost (since we are *not* calling fsync()
                // on the directory during the creation of the link)
                string relativePath = Path.GetRelativePath(
                    CurrentFile.JournalWriter.FileName.FullPath, 
                    journalFile.JournalWriter.FileName.FullPath);
                _linkedJournalsRecord.Add(relativePath, environment.HeaderAccessor.JournalId);

                if (environment.Options is StorageEnvironmentOptions.DirectoryStorageEnvironmentOptions)
                {
                    _linkedJournalsRecord.RegisterNewJournal(
                        environment.Options.JournalPath.ToFullPath(),
                        CurrentFile.Number
                    );
                }
            }

            if (_linkedJournalsRecord.HasEntries)
            {
                var entry = _linkedJournalsRecord.CreateEntry();
                if (_env.Options.Encryption.IsEnabled)
                    EncryptTransaction((byte*)entry.Base);

                requiredSizeIn4Kbs += entry.NumberOf4Kbs;
                SharedJournalState.PrepareForCommit(entry);
                long available4Kbs = CurrentFile.GetAvailable4Kbs(tx.CurrentStateRecord);
                // This should be rare, we have a full journal, and we _had_ enough space, but not enough after we 
                // included the linked journal record. So we have to extend the file size directly before issuing the 
                // actual write.
                if (available4Kbs < requiredSizeIn4Kbs)
                {
                    long newSize = (CurrentFile.JournalWriter.NumberOfAllocated4Kb - available4Kbs + requiredSizeIn4Kbs) * 4 * Constants.Size.Kilobyte;
                    CurrentFile.JournalWriter.Truncate(newSize);
                    if (_logger.IsDebugEnabled)
                        _logger.Debug($"Journal file {CurrentFile.Number:D19} was extended to size {CurrentFile.JournalSize} to allow the linked journals entry");
                }
            }
            
            var entries = SharedJournalState.Entries;
            var start = Stopwatch.GetTimestamp();

            tx._forTestingPurposes?.ActionToCallJustBeforeWritingToJournal?.Invoke();
            long positionIn4Kbs = CurrentFile.Write(tx, entries);
            // We must update the _root_ transaction as well here, since if we have a batch
            // that does not include the root env, then we have to update the position of 
            // the journal writer
            tx.UpdateJournal(CurrentFile.Number, positionIn4Kbs);

            var elapsed = Stopwatch.GetElapsedTime(start);

            foreach (var rec in SharedJournalState.JournalRecords)
            {
                var llt = rec.Transaction;
                llt.UpdateJournal(llt.WrittenToJournalNumber, positionIn4Kbs);
                rec.Tcs.TrySetResult();
            }

            SharedJournalState.Reset();

            if (_logger.IsDebugEnabled)
                _logger.Debug(
                    $"Writing {new Size(4 * requiredSizeIn4Kbs, SizeUnit.Kilobytes)} to journal {CurrentFile.Number:D19} took {elapsed} with journal entries from {SharedJournalState.JournalRecords.Count:##,###} environments");

            if (CurrentFile.GetAvailable4Kbs(tx.CurrentStateRecord) == 0)
            {
                CurrentFileIsDone();
            }
        }

        private bool _alreadyCheckedIfLatestJournalFileIsLinked; 

        /// <summary>
        /// This is effectively single threaded, even though it is used _across_ environments.
        /// If we have a single root environment, then the write lock protect it, as usual.
        /// Across environments, this is only called from the _root_ environment, across all branches,
        /// so this is safe to use without worrying about concurrency
        /// </summary>
        private JournalFile EnsureRegistered(JournalFile journalFile, out bool alreadyExists)
        {
            ref var matchingJournal = ref CollectionsMarshal.GetValueRefOrAddDefault(journalFile.RegisteredEnvironments, _env, out alreadyExists);
            if (alreadyExists)
            {
                return matchingJournal;
            }

            string existingJournalFileName;
            if (_alreadyCheckedIfLatestJournalFileIsLinked is false)
            {
                _alreadyCheckedIfLatestJournalFileIsLinked = true;
                if (_env.Options.IsLinked(_journalIndex, journalFile.JournalWriter.FileName.FullPath, out existingJournalFileName))
                {
                    // The file is already linked, so we can reuse the file link
                    matchingJournal = AddJournal(_journalIndex);
                    return matchingJournal;
                }
            }

            long journalIndex = _journalIndex + 1;

            _env.Options.LinkFiles(journalIndex,journalFile.JournalWriter.FileName.FullPath, out existingJournalFileName);
            matchingJournal = AddJournal(journalIndex);

            // we modify the in memory state _after_ we created the file, because we have to make sure that 
            // we have created it successfully first. 
            _journalIndex++;
            
            return matchingJournal;

            JournalFile AddJournal(long index)
            {
                var journalWriter = _env.Options.CreateJournalWriterForBranchEnvironment(index, existingJournalFileName, journalFile);
                var journal = new JournalFile(_env, journalWriter, index, FrozenSet<Guid>.Empty);
                journal.NewlyCreatedFile = true;
                journal.DoneWriting = journalFile.DoneWriting;
                journal.AddRef();
                _files = _files.Append(journal);
                return journal;
            }
        }


        private Pal.journal_entry PrepareToWriteToJournal(LowLevelTransaction tx, ref Pager.PagerTransactionState txState, out long numberOfUncompressedPages)
        {
            var txPages = tx.GetTransactionPages();
            var numberOfPages = txPages.Count;
            var pagesCountIncludingAllOverflowPages = 0;
            foreach (var page in txPages)
            {
                pagesCountIncludingAllOverflowPages += page.NumberOfPages;
            }

            var sizeOfPagesHeader = numberOfPages * sizeof(TransactionHeaderPageInfo);
            var overhead = sizeOfPagesHeader + (long)numberOfPages * sizeof(long);
            var overheadInPages = checked((int)(overhead / Constants.Storage.PageSize + (overhead % Constants.Storage.PageSize == 0 ? 0 : 1)));

            const int transactionHeaderPageOverhead = 1;
            var pagesRequired = (transactionHeaderPageOverhead + pagesCountIncludingAllOverflowPages + overheadInPages);

            if (_is32Bit)
            {
                pagesRequired = AdjustPagesRequiredFor32Bits(pagesRequired);
            }

            try
            {
                _compressionPager.EnsureContinuous(ref _compressionPagerState, 0, pagesRequired);
                Debug.Assert(_compressionPagerState.TotalAllocatedSize >= pagesRequired * Constants.Storage.PageSize, "_compressionPagerState.TotalAllocatedSize >= pagesRequired* Constants.Storage.PageSize");
            }
            catch (InsufficientMemoryException)
            {
                // RavenDB-10830: failed to lock memory of temp buffers in encrypted db, let's create new file with initial size

                _compressionPager.Dispose();
                (_compressionPager, _compressionPagerState) = CreateCompressionPager(_env.Options.InitialFileSize ?? _env.Options.InitialLogFileSize);
                _lastCompressionBufferReduceCheck = DateTime.UtcNow;
                throw;
            }

            _compressionPager.EnsureMapped(_compressionPagerState, ref txState, 0, pagesRequired);
            var stateOfThePagerForPagesToBeCompressed = _compressionPagerState;
            var txHeaderPtr = _compressionPager.MakeWritable(_compressionPagerState,
                _compressionPager.AcquireRawPagePointer(_compressionPagerState, ref txState, 0)
            );
            var txPageInfoPtr = txHeaderPtr + sizeof(TransactionHeader);
            var pagesInfo = (TransactionHeaderPageInfo*)txPageInfoPtr;

            var write = txPageInfoPtr + sizeOfPagesHeader;
            var pageSequentialNumber = 0;
            var pagesEncountered = 0;
            foreach (var txPage in txPages)
            {
                var scratchPage = txPage.ReadWritable(ref tx.PagerTransactionState);
                var pageHeader = (PageHeader*)scratchPage;

                // When encryption is off, we do validation by checksum
                if (_env.Options.Encryption.IsEnabled == false)
                {
                    pageHeader->Checksum = StorageEnvironment.CalculatePageChecksum(scratchPage, pageHeader->PageNumber, pageHeader->Flags, pageHeader->OverflowSize);
                }

                ref TransactionHeaderPageInfo transactionHeaderPageInfo = ref pagesInfo[pageSequentialNumber];
                transactionHeaderPageInfo.PageNumber = pageHeader->PageNumber;

                *(long*)write = pageHeader->PageNumber;
                write += sizeof(long);

                if (_env.Options.Encryption.IsEnabled == false)
                {
                    _diffPage.Output = write;

                    int diffPageSize = txPage.NumberOfPages * Constants.Storage.PageSize;
                    pagesEncountered += txPage.NumberOfPages;
                    Debug.Assert(pagesEncountered <= pagesCountIncludingAllOverflowPages);
                    if (txPage.PreviousVersion.IsValid)
                    {
                        _diffPage.ComputeDiff(txPage.PreviousVersion.Pointer, scratchPage, diffPageSize);
                    }
                    else
                    {
                        _diffPage.ComputeNew(scratchPage, diffPageSize);
                    }

                    write += _diffPage.OutputSize;
                    transactionHeaderPageInfo.Size = _diffPage.OutputSize == 0 ? 0 : diffPageSize;
                    transactionHeaderPageInfo.IsNewDiff = txPage.PreviousVersion.IsValid == false;
                    transactionHeaderPageInfo.DiffSize = _diffPage.IsDiff ? _diffPage.OutputSize : 0;
                    Debug.Assert(Math.Max(transactionHeaderPageInfo.Size, transactionHeaderPageInfo.DiffSize) <= diffPageSize);
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
                    transactionHeaderPageInfo.Size = size;
                    transactionHeaderPageInfo.DiffSize = 0;
                }
                ++pageSequentialNumber;
            }

            var totalSizeWritten = write - txPageInfoPtr;

            long compressedLen = 0;

            // We want to do compression when the size of the data to store is bigger than the threshold.
            // This is essentially a threshold between IO blocks usage and the CPU consumption of LZ4
            // PERF: Before v7.0 we would avoid doing the diff if the amount of pages was big, however,
            // the speed of diffing is roughly that of a memory copy, which is so fast that the benefit of always doing
            // it is big enough to just do it every time. 
            var performCompression = totalSizeWritten > _env.Options.CompressTxAboveSizeInBytes;
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
                    // IMPORTANT: The memory we got in the previous call to AcquirePagePointer() is associated with the _compressionPagerState
                    // which may *change* because we are extending the file. We need to *ensure* that we keep hold of that state until the
                    // end of this method, to avoid releasing the buffer too early when the state's finalizer is running. This is why we
                    // have the stateOfThePagerForPagesToBeCompressed held there
                    Debug.Assert(stateOfThePagerForPagesToBeCompressed != null, "Read the comment above!");
                    _compressionPager.EnsureContinuous(ref _compressionPagerState, pagesWritten, outputBufferInPages);
                    Debug.Assert(_compressionPagerState.TotalAllocatedSize >= (pagesWritten + outputBufferInPages) * Constants.Storage.PageSize,
                        "_compressionPagerState.TotalAllocatedSize >= (pagesWritten+outputBufferInPages)* Constants.Storage.PageSize");
                }
                catch (InsufficientMemoryException)
                {
                    // RavenDB-10830: failed to lock memory of temp buffers in encrypted db, let's create new file with initial size

                    _compressionPager.Dispose();
                    (_compressionPager, _compressionPagerState) = CreateCompressionPager(_env.Options.InitialFileSize ?? _env.Options.InitialLogFileSize);
                    _lastCompressionBufferReduceCheck = DateTime.UtcNow;
                    throw;
                }

                _compressionPager.EnsureMapped(_compressionPagerState, ref tx.PagerTransactionState, pagesWritten, outputBufferInPages);

                txHeaderPtr = _compressionPager.MakeWritable(_compressionPagerState,
                    _compressionPager.AcquireRawPagePointer(_compressionPagerState, ref tx.PagerTransactionState, pagesWritten)
                );
                var compressionBuffer = txHeaderPtr + sizeof(TransactionHeader);

                var path = CurrentFile?.JournalWriter?.FileName?.FullPath ?? _env.Options.GetJournalPath(Math.Max(0, _journalIndex))?.FullPath;
                using (var metrics = _env.Options.IoMetrics.MeterIoRate(path, IoMetrics.MeterType.Compression, 0)) // Note that the last journal may be replaced if we switch journals, however it doesn't affect web graph
                {
                    int compressionAcceleration = _env.Options.JournalsCompressionAcceleration;
                    compressedLen = LZ4.Encode64LongBuffer(
                        txPageInfoPtr,
                        compressionBuffer,
                        totalSizeWritten,
                        outputBufferSize,
                        compressionAcceleration);

                    metrics.SetCompressionResults(totalSizeWritten, compressedLen, compressionAcceleration);
                }
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

            ref var txHeader = ref tx.TransactionHeader;
            txHeader.CompressedSize = reportedCompressionLength;
            txHeader.UncompressedSize = totalSizeWritten;
            txHeader.PageCount = numberOfPages;
            txHeader.JournalId = _headerAccessor.JournalId;
            
            if (_env.Options.Encryption.IsEnabled == false)
            {
                if (performCompression)
                    txHeader.Hash = Hashing.XXHash64.Calculate(txHeaderPtr + sizeof(TransactionHeader), (ulong)compressedLen, (ulong)txHeader.TransactionId);
                else
                    txHeader.Hash = Hashing.XXHash64.Calculate(txPageInfoPtr, (ulong)totalSizeWritten, (ulong)txHeader.TransactionId);
            }
            else
            {
                // if encryption is enabled, we are already validating the tx using
                // the AEAD method, so no need to do it twice
                txHeader.Hash = 0;
            }

            // Copy the transaction header to the output buffer. 
            Unsafe.Copy(txHeaderPtr, ref txHeader);
            Debug.Assert(((long)txHeaderPtr % (4 * Constants.Size.Kilobyte)) == 0, "Memory must be 4kb aligned");

            if (_env.Options.Encryption.IsEnabled)
                EncryptTransaction(txHeaderPtr);

            GC.KeepAlive(stateOfThePagerForPagesToBeCompressed);
            numberOfUncompressedPages = pagesCountIncludingAllOverflowPages;
            return new Pal.journal_entry
            {
                Base = txHeaderPtr,
                NumberOf4Kbs = entireBuffer4Kbs,
            };
        }

        private const int PagesIn1Mb = Constants.Size.Megabyte / Constants.Storage.PageSize;

        /// <summary>
        /// The idea of this function is to calculate page sizes that will cause less fragmentation in 32 bit mode
        /// for allocation smaller than 1MB we will allocate the next power of 2
        /// for allocation larger than 1MB we will alligned them to be MB alligned 
        /// </summary>
        /// <param name="pagesRequired"></param>
        /// <returns></returns>
        private static int AdjustPagesRequiredFor32Bits(int pagesRequired)
        {
            var bytes = (long)pagesRequired * Constants.Storage.PageSize;
            if (bytes < Constants.Size.Megabyte / 2)
            {
                pagesRequired = (int)Bits.PowerOf2(bytes) / Constants.Storage.PageSize;
            }
            else
            {
                pagesRequired = pagesRequired - pagesRequired % PagesIn1Mb + PagesIn1Mb;
            }

            return pagesRequired;
        }

        internal static readonly byte[] Context = Encoding.UTF8.GetBytes("Txn-Acid");

        private void EncryptTransaction(byte* fullTxBuffer)
        {
            var txHeader = (TransactionHeader*)fullTxBuffer;

            txHeader->Flags |= TransactionPersistenceModeFlags.Encrypted;
            ulong macLen = (ulong)Sodium.crypto_aead_xchacha20poly1305_ietf_abytes();
            var subKeyLen = Sodium.crypto_aead_xchacha20poly1305_ietf_keybytes();
            var subKey = stackalloc byte[(int)subKeyLen];
            fixed (byte* mk = _env.Options.Encryption.MasterKey)
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

        private (Pager Pager, Pager.State State) CreateCompressionPager(long initialSize)
        {
            return _env.Options.CreateTemporaryBufferPager(
                $"compression.{_compressionPagerCounter++:D10}{StorageEnvironmentOptions.DirectoryStorageEnvironmentOptions.BuffersFileExtension}", initialSize,
                encrypted: false);
        }

        private DateTime _lastCompressionBufferReduceCheck = DateTime.UtcNow;
        private readonly bool _is32Bit;

        private void ReduceSizeOfCompressionBufferIfNeeded(bool forceReduce = false)
        {
            var maxSize = _env.Options.MaxScratchBufferSize;
            if (ShouldReduceSizeOfCompressionPager(maxSize, forceReduce) == false)
            {
                // PERF: Compression buffer will be reused, it is safe to discard the content to clear the modified bit.
                // For encrypted databases, discarding locked memory is *expensive*, so we avoid it
                if (_env.Options.Encryption.IsEnabled == false)
                    _compressionPager.DiscardWholeFile(_compressionPagerState);

                return;
            }


            // the compression pager is too large, we probably had a big transaction and now can
            // free all of that and come back to more reasonable values.
            if (forceReduce == false && _logger.IsInfoEnabled)
            {
                _logger.Info(
                    $"Compression buffer: {_compressionPager} has reached size {new Size(_compressionPagerState.NumberOfAllocatedPages * Constants.Storage.PageSize, SizeUnit.Bytes)} which is more than the maximum size " +
                    $"of {new Size(maxSize, SizeUnit.Bytes)}. Will trim it now to the max size allowed. If this is happen on a regular basis," +
                    " consider raising the limit (MaxScratchBufferSize option control it), since it can cause performance issues");
            }

            _lastCompressionBufferReduceCheck = DateTime.UtcNow;

            _compressionPager.Dispose();

            _forTestingPurposes?.OnReduceSizeOfCompressionBufferIfNeeded_RightAfterDisposingCompressionPager?.Invoke();

            (_compressionPager, _compressionPagerState) = CreateCompressionPager(maxSize);
        }

        public void ZeroCompressionBuffer(ref Pager.PagerTransactionState txState)
        {
            var lockTaken = false;

            if (Monitor.IsEntered(_writeLock) == false)
                Monitor.Enter(_writeLock, ref lockTaken);

            try
            {
                var compressionBufferSize = _compressionPagerState.NumberOfAllocatedPages * Constants.Storage.PageSize;
                _compressionPager.EnsureMapped(_compressionPagerState, ref txState, 0, checked((int)_compressionPagerState.NumberOfAllocatedPages));
                var pagePointer = _compressionPager.MakeWritable(_compressionPagerState,
                 _compressionPager.AcquirePagePointer(_compressionPagerState, ref txState, 0)
             );
                Sodium.sodium_memzero(pagePointer, (UIntPtr)compressionBufferSize);
            }
            finally
            {
                if (lockTaken)
                    Monitor.Exit(_writeLock);
            }
        }

        private bool ShouldReduceSizeOfCompressionPager(long maxSize, bool forceReduce)
        {
            var compressionBufferSize = _compressionPagerState.NumberOfAllocatedPages * Constants.Storage.PageSize;
            if (compressionBufferSize <= maxSize)
                return false;

            if (forceReduce)
                return true;

            if ((DateTime.UtcNow - _lastCompressionBufferReduceCheck).TotalMinutes < 5)
                return false;

            // prevent resize if we recently used at least half of the compression buffer
            var preventResize = _maxNumberOfPagesRequiredForCompressionBuffer > _compressionPagerState.NumberOfAllocatedPages / 2;

            _maxNumberOfPagesRequiredForCompressionBuffer = 0;
            _lastCompressionBufferReduceCheck = DateTime.UtcNow;
            return !preventResize;
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

        private TestingStuff _forTestingPurposes;
        private CancellationTokenSource _rootJournalMergedCommitsCts;
        private readonly int _minimumSharedJournalsMergeCount;

        private readonly LinkedJournalsRecord _linkedJournalsRecord = new();

        internal TestingStuff ForTestingPurposesOnly()
        {
            if (_forTestingPurposes != null)
                return _forTestingPurposes;

            return _forTestingPurposes = new TestingStuff();
        }

        internal sealed class TestingStuff
        {
            internal Action OnReduceSizeOfCompressionBufferIfNeeded_RightAfterDisposingCompressionPager;
            internal Action<ConcurrentQueue<WriteAheadJournal.PendingJournalStateRecord>> OnWriteBuffersToJournal;
        }

        private void RejectCommitsToMerge() => SharedJournalState.SetCancel();
    }
}

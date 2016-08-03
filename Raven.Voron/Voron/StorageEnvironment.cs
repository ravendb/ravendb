using Sparrow.Collections;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.ComponentModel;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Runtime.ExceptionServices;
using System.Runtime.InteropServices;
using System.Threading;
using System.Threading.Tasks;
using Voron.Debugging;
using Voron.Exceptions;
using Voron.Impl;
using Voron.Impl.Backup;
using Voron.Impl.FileHeaders;
using Voron.Impl.FreeSpace;
using Voron.Impl.Journal;
using Voron.Impl.Paging;
using Voron.Impl.Scratch;
using Voron.Trees;
using Voron.Util;

namespace Voron
{
    public class StorageEnvironment : IDisposable
    {
        private readonly StorageEnvironmentOptions _options;

        private readonly ConcurrentSet<Transaction> _activeTransactions = new ConcurrentSet<Transaction>();

        private readonly IVirtualPager _dataPager;

        private readonly WriteAheadJournal _journal;
        private readonly object _txWriter = new object();
        private readonly ManualResetEventSlim _flushWriter = new ManualResetEventSlim();

        private readonly ReaderWriterLockSlim _txCommit = new ReaderWriterLockSlim();

        private long _transactionsCounter;
        private readonly IFreeSpaceHandling _freeSpaceHandling;
        private Task _flushingTask;
        private readonly HeaderAccessor _headerAccessor;

        private CancellationTokenSource _cancellationTokenSource = new CancellationTokenSource();
        private readonly ScratchBufferPool _scratchBufferPool;
        private DebugJournal _debugJournal;
        private EndOfDiskSpaceEvent _endOfDiskSpace;
        private int _sizeOfUnflushedTransactionsInJournalFile;

        private readonly Queue<TemporaryPage> _tempPagesPool = new Queue<TemporaryPage>();

        public TransactionMergingWriter Writer { get; private set; }

        public StorageEnvironmentState State { get; private set; }

        public SnapshotReader CreateSnapshot()
        {
            return new SnapshotReader(NewTransaction(TransactionFlags.Read));
        }

#if DEBUG
        public StorageEnvironment(StorageEnvironmentOptions options,string debugJournalName)
            : this(options)
        {
            DebugJournal = new DebugJournal(debugJournalName,this);
            
            if(Writer != null)
                Writer.Dispose();
            Writer = new TransactionMergingWriter(this, _cancellationTokenSource.Token, DebugJournal);
        }
#endif

        public StorageEnvironment(StorageEnvironmentOptions options)
        {
            try
            {
                _options = options;
                _dataPager = options.DataPager;
                _freeSpaceHandling = new FreeSpaceHandling();
                _headerAccessor = new HeaderAccessor(this);
                var isNew = _headerAccessor.Initialize();

                _scratchBufferPool = new ScratchBufferPool(this);

                _journal = new WriteAheadJournal(this);
                
                if (isNew)
                    CreateNewDatabase();
                else // existing db, let us load it
                    LoadExistingDatabase();

                Writer = new TransactionMergingWriter(this, _cancellationTokenSource.Token);

                if (_options.ManualFlushing == false)
                    _flushingTask = FlushWritesToDataFileAsync();
            }
            catch (Exception)
            {
                Dispose();
                throw;
            }
        }

        public ScratchBufferPool ScratchBufferPool
        {
            get { return _scratchBufferPool; }
        }

        private unsafe void LoadExistingDatabase()
        {
            var header = stackalloc TransactionHeader[1];
            bool hadIntegrityIssues = _journal.RecoverDatabase(header);

            if (hadIntegrityIssues)
            {
                var message = _journal.Files.Count == 0 ? "Unrecoverable database" : "Database recovered partially. Some data was lost.";

                _options.InvokeRecoveryError(this, message, null);
            }

            var entry = _headerAccessor.CopyHeader();
            var nextPageNumber = (header->TransactionId == 0 ? entry.LastPageNumber : header->LastPageNumber) + 1;
            State = new StorageEnvironmentState(null, null, nextPageNumber)
            {
                NextPageNumber = nextPageNumber,
                Options = Options
            };

            _transactionsCounter = (header->TransactionId == 0 ? entry.TransactionId : header->TransactionId);

            using (var tx = NewTransaction(TransactionFlags.ReadWrite))
            {
                var root = Tree.Open(tx, header->TransactionId == 0 ? &entry.Root : &header->Root);
                var freeSpace = Tree.Open(tx, header->TransactionId == 0 ? &entry.FreeSpace : &header->FreeSpace);

                tx.UpdateRootsIfNeeded(root, freeSpace);
                tx.Commit();

            }
        }

        private void CreateNewDatabase()
        {
            const int initialNextPageNumber = 0;
            State = new StorageEnvironmentState(null, null, initialNextPageNumber)
            {
                Options = Options
            };
            using (var tx = NewTransaction(TransactionFlags.ReadWrite))
            {
                var root = Tree.Create(tx, false);
                var freeSpace = Tree.Create(tx, false);

                // important to first create the two trees, then set them on the env
                tx.UpdateRootsIfNeeded(root, freeSpace);
                
                tx.Commit();

                //since this transaction is never shipped, this is the first previous transaction
                //when applying shipped logs
            }
        }

        public IFreeSpaceHandling FreeSpaceHandling
        {
            get { return _freeSpaceHandling; }
        }

        public HeaderAccessor HeaderAccessor
        {
            get { return _headerAccessor; }
        }

        public long OldestTransaction
        {
            get
            {
                var largestTx = long.MaxValue;
                // ReSharper disable once LoopCanBeConvertedToQuery
                foreach (var activeTransaction in _activeTransactions)
                {
                    if (largestTx > activeTransaction.Id)
                        largestTx = activeTransaction.Id;
                }
                if (largestTx == long.MaxValue)
                    return 0;
                return largestTx;
            }
        }

        public long NextPageNumber
        {
            get { return State.NextPageNumber; }
        }

        public StorageEnvironmentOptions Options
        {
            get { return _options; }
        }

        public WriteAheadJournal Journal
        {
            get { return _journal; }
        }

        public bool IsDebugRecording
        {
            get
            {
                if (DebugJournal == null)
                    return false;
                return DebugJournal.IsRecording;
            }
            set
            {
                if (DebugJournal != null)
                    DebugJournal.IsRecording = value;
            }
        }

        public DebugJournal DebugJournal
        {
            get { return _debugJournal; }
            set
            {
                _debugJournal = value;

                if (Writer != null && value != null)
                {
                    Writer.Dispose();
                    Writer = new TransactionMergingWriter(this, _cancellationTokenSource.Token, _debugJournal);
                }

            }
        }

        internal List<ActiveTransaction> ActiveTransactions
        {
            get
            {
                return _activeTransactions.Select(x => new ActiveTransaction()
                {
                    Id = x.Id,
                    Flags = x.Flags
                }).ToList();
            }
        }

        public void DeleteTree(Transaction tx, string name)
        {
            if (tx.Flags == (TransactionFlags.ReadWrite) == false)
                throw new ArgumentException("Cannot create a new newRootTree with a read only transaction");

            Tree tree = tx.ReadTree(name);
            if (tree == null)
                return;

            foreach (var page in tree.AllPages())
            {
                tx.FreePage(page);
            }

            tx.Root.Delete((Slice) name);

            tx.RemoveTree(name);
        }

        public unsafe void RenameTree(Transaction tx, string fromName, string toName)
        {
            if (tx.Flags == (TransactionFlags.ReadWrite) == false)
                throw new ArgumentException("Cannot rename a new tree with a read only transaction");

            if (toName.Equals(Constants.RootTreeName, StringComparison.InvariantCultureIgnoreCase) ||
                toName.Equals(Constants.FreeSpaceTreeName, StringComparison.InvariantCultureIgnoreCase))
                throw new InvalidOperationException("Cannot create a tree with reserved name: " + toName);

            if (tx.ReadTree(toName) != null)
                throw new ArgumentException("Cannot rename a tree with the name of an existing tree: " + toName);

            Tree fromTree = tx.ReadTree(fromName);
            if (fromTree == null)
                throw new ArgumentException("Tree " + fromName + " does not exists");

            Slice key = (Slice)toName;

            tx.Root.Delete((Slice)fromName);
            var ptr = tx.Root.DirectAdd(key, sizeof(TreeRootHeader));
            fromTree.State.CopyTo((TreeRootHeader*) ptr);
            fromTree.Name = toName;
            fromTree.State.IsModified = true;
            
            tx.RemoveTree(fromName);
            tx.RemoveTree(toName);

            tx.AddTree(toName, fromTree);

            if (IsDebugRecording)
                DebugJournal.RecordWriteAction(DebugActionType.RenameTree, tx, (Slice)toName, fromName, Stream.Null);
        }

        public unsafe Tree CreateTree(Transaction tx, string name, bool keysPrefixing = false)
        {
            Tree tree = tx.ReadTree(name);
            if (tree != null)
                return tree;

            if (name.Equals(Constants.RootTreeName, StringComparison.InvariantCultureIgnoreCase))
                return tx.Root;
            if (name.Equals(Constants.FreeSpaceTreeName, StringComparison.InvariantCultureIgnoreCase))
                return tx.FreeSpaceRoot;

            if (tx.Flags == (TransactionFlags.ReadWrite) == false)
                throw new InvalidOperationException("No such tree: " + name + " and cannot create trees in read transactions");

            Slice key = name;

            tree = Tree.Create(tx, keysPrefixing);
            tree.Name = name;
            var space = tx.Root.DirectAdd(key, sizeof(TreeRootHeader));

            tree.State.CopyTo((TreeRootHeader*)space);
            tree.State.IsModified = true;
            tx.AddTree(name, tree);

            if(IsDebugRecording)
                DebugJournal.RecordWriteAction(DebugActionType.CreateTree, tx, Slice.Empty,name,Stream.Null);

            return tree;
        }

        public void Dispose()
        {
            if(DebugJournal != null)
                DebugJournal.Dispose();

            _cancellationTokenSource.Cancel();
            _flushWriter.Set();

            try
            {
                var flushingTaskCopy = _flushingTask;
                if (flushingTaskCopy != null)
                {
                    switch (flushingTaskCopy.Status)
                    {
                        case TaskStatus.RanToCompletion:
                        case TaskStatus.Canceled:
                            break;
                        default:
                            try
                            {
                                flushingTaskCopy.Wait();
                            }
                            catch (AggregateException ae)
                            {
                                if (ae.InnerException is OperationCanceledException == false)
                                    throw;
                            }
                            break;
                    }
                }
            }
            finally
            {
                var errors = new List<Exception>();
                foreach (var disposable in new IDisposable[]
                {
                    Writer,
                    _headerAccessor,
                    _scratchBufferPool,
                    _options.OwnsPagers ? _options : null,
                    _journal
                }.Concat(_tempPagesPool))
                {
                    try
                    {
                        if (disposable != null)
                            disposable.Dispose();
                    }
                    catch (Exception e)
                    {
                        errors.Add(e);
                    }
                }

                if (errors.Count != 0)
                    throw new AggregateException(errors);
            }
        }

        public Transaction NewTransaction(TransactionFlags flags, TimeSpan? timeout = null)
        {
            bool txLockTaken = false;
            try
            {
                if (flags == (TransactionFlags.ReadWrite))
                {
                    var wait = timeout ?? (Debugger.IsAttached ? TimeSpan.FromMinutes(30) : TimeSpan.FromSeconds(30));
                    Monitor.TryEnter(_txWriter, wait, ref txLockTaken);
                    if (txLockTaken == false)
                    {
                        throw new TimeoutException("Waited for " + wait +
                                                    " for transaction write lock, but could not get it");
                    }
                    
                    if (_endOfDiskSpace != null)
                    {
                        if (_endOfDiskSpace.CanContinueWriting)
                        {
                            var flushingTask = _flushingTask;
                            Debug.Assert(flushingTask != null && (flushingTask.Status == TaskStatus.Canceled || flushingTask.Status == TaskStatus.RanToCompletion));
                            _cancellationTokenSource = new CancellationTokenSource();
                            _flushingTask = FlushWritesToDataFileAsync();
                            _endOfDiskSpace = null;
                        }
                    }
                }

                Transaction tx;

                _txCommit.EnterReadLock();
                try
                {
                    long txId = flags == TransactionFlags.ReadWrite ? _transactionsCounter + 1 : _transactionsCounter;
                    tx = new Transaction(this, txId, flags, _freeSpaceHandling);

                    if (IsDebugRecording)
                    {
                        RecordTransactionState(tx, DebugActionType.TransactionStart);
                        tx.RecordTransactionState = RecordTransactionState;
                    }
                }
                finally
                {
                    _txCommit.ExitReadLock();
                }

                _activeTransactions.Add(tx);
                tx.EnsurePagerStateReference(_dataPager.PagerState);

                if (flags == TransactionFlags.ReadWrite)
                {
                    tx.AfterCommit = TransactionAfterCommit;
                }

                return tx;
            }
            catch (Exception)
            {
                if (txLockTaken)
                    Monitor.Exit(_txWriter);
                throw;
            }
        }

        private void RecordTransactionState(Transaction tx, DebugActionType state)
        {
            DebugJournal.RecordTransactionAction(tx, state);
        }

        public long NextWriteTransactionId
        {
            get { return Thread.VolatileRead(ref _transactionsCounter) + 1; }
        }

        private void TransactionAfterCommit(Transaction tx)
        {
            if (_activeTransactions.Contains(tx) == false)
                return;
            
            _txCommit.EnterWriteLock();
            try
            {
                if (tx.Committed && tx.FlushedToJournal)
                    _transactionsCounter = tx.Id;

                State = tx.State;
            }
            finally
            {
                _txCommit.ExitWriteLock();
            }

            if (tx.FlushedToJournal == false)
                return;

            var totalPages = 0;
            // ReSharper disable once LoopCanBeConvertedToQuery
            foreach (var page in tx.GetTransactionPages())
            {
                totalPages += page.NumberOfPages;
            }

            Interlocked.Add(ref _sizeOfUnflushedTransactionsInJournalFile, totalPages);
            _flushWriter.Set();
        }

        internal void TransactionCompleted(Transaction tx)
        {
            if (_activeTransactions.TryRemove(tx) == false)
                return;

            if (tx.Flags != (TransactionFlags.ReadWrite))
                return;

            Monitor.Exit(_txWriter);
        }

        public Dictionary<string, List<long>> AllPages(Transaction tx)
        {
            var results = new Dictionary<string, List<long>>(StringComparer.OrdinalIgnoreCase)
                {
                    {"Root", tx.Root.AllPages()},
                    {"Free Space Overhead", tx.FreeSpaceRoot.AllPages()},
                    {"Free Pages", _freeSpaceHandling.AllPages(tx)}
                };

            foreach (var tree in tx.Trees)
            {
                if (tree == null)
                    continue;
                results.Add(tree.Name, tree.AllPages());
            }

            return results;
        }

        public StorageReport GenerateReport(Transaction tx, bool computeExactSizes, Action<string> progress, CancellationToken token)
        {
            var numberOfAllocatedPages = Math.Max(_dataPager.NumberOfAllocatedPages, NextPageNumber - 1); // async apply to data file task
            var numberOfFreePages = _freeSpaceHandling.AllPages(tx).Count;

            var trees = new List<Tree>();

            progress("Reading trees");

            using (var rootIterator = tx.Root.Iterate())
            {
                token.ThrowIfCancellationRequested();

                if (rootIterator.Seek(Slice.BeforeAllKeys))
                {
                    do
                    {
                        var tree = tx.ReadTree(rootIterator.CurrentKey.ToString());
                        trees.Add(tree);

                    }
                    while (rootIterator.MoveNext());
                }
            }

            token.ThrowIfCancellationRequested();

            var generator = new StorageReportGenerator(tx);

            return generator.Generate(new ReportInput
            {
                NumberOfAllocatedPages = numberOfAllocatedPages,
                NumberOfFreePages = numberOfFreePages,
                NextPageNumber = NextPageNumber,
                Journals = Journal.Files.ToList(),
                Trees = trees,
                IsLightReport = !computeExactSizes
            }, progress, token);
        }

        public EnvironmentStats Stats()
        {
            using (var tx = NewTransaction(TransactionFlags.Read))
            {
                var numberOfAllocatedPages = Math.Max(_dataPager.NumberOfAllocatedPages, State.NextPageNumber - 1); // async apply to data file task

                return new EnvironmentStats
                {
                    FreePagesOverhead = tx.FreeSpaceRoot.State.PageCount,
                    RootPages = tx.Root.State.PageCount,
                    UnallocatedPagesAtEndOfFile = _dataPager.NumberOfAllocatedPages - NextPageNumber,
                    UsedDataFileSizeInBytes = (State.NextPageNumber - 1) * AbstractPager.PageSize,
                    AllocatedDataFileSizeInBytes = numberOfAllocatedPages * AbstractPager.PageSize,
                    NextWriteTransactionId = NextWriteTransactionId,
                    ActiveTransactions = ActiveTransactions
                };

            }
        }

        [HandleProcessCorruptedStateExceptions]
        private Task FlushWritesToDataFileAsync()
        {
            return Task.Factory.StartNew(() =>
                {
                    while (_cancellationTokenSource.IsCancellationRequested == false)
                    {
                        _cancellationTokenSource.Token.ThrowIfCancellationRequested();

                        var hasWrites = _flushWriter.Wait(_options.IdleFlushTimeout);

                        _cancellationTokenSource.Token.ThrowIfCancellationRequested();

                        if (hasWrites)
                            _flushWriter.Reset();

                        var sizeOfUnflushedTransactionsInJournalFile =
                            Thread.VolatileRead(ref _sizeOfUnflushedTransactionsInJournalFile);
                        if (sizeOfUnflushedTransactionsInJournalFile == 0)
                            continue;

                        if (hasWrites == false ||
                            sizeOfUnflushedTransactionsInJournalFile >= _options.MaxNumberOfPagesInJournalBeforeFlush)
                        {
                            Interlocked.Add(ref _sizeOfUnflushedTransactionsInJournalFile, -sizeOfUnflushedTransactionsInJournalFile);

                            // we either reached our the max size we allow in the journal file before flush flushing (and therefor require a flush)
                            // we didn't have a write in the idle timeout (default: 5 seconds), this is probably a good time to try and do a proper flush
                            // while there isn't any other activity going on.

                            if (IsDebugRecording)
                                _debugJournal.RecordFlushAction(DebugActionType.FlushStart, null);

                            try
                            {
                                _journal.Applicator.ApplyLogsToDataFile(OldestTransaction, _cancellationTokenSource.Token);
                            }
                            catch (TimeoutException)
                            {
                                // we can ignore this, we'll try next time
                            }
                            catch (SEHException sehException)
                            {
                                throw new VoronUnrecoverableErrorException("Error occurred during flushing journals to the data file",
                                    new Win32Exception(sehException.HResult));
                            }

                            if (IsDebugRecording)
                                _debugJournal.RecordFlushAction(DebugActionType.FlushEnd, null);
                        }
                    }
                }, TaskCreationOptions.LongRunning);
        }

        public void FlushLogToDataFile(Transaction tx = null, bool allowToFlushOverwrittenPages = false)
        {
            if (_options.ManualFlushing == false)
                throw new NotSupportedException("Manual flushes are not set in the storage options, cannot manually flush!");

            ForceLogFlushToDataFile(tx, allowToFlushOverwrittenPages);
        }

        internal void ForceLogFlushToDataFile(Transaction tx, bool allowToFlushOverwrittenPages)
        {
            if (IsDebugRecording)
            {
                _debugJournal.RecordFlushAction(DebugActionType.FlushStart, tx);
            }

            _journal.Applicator.ApplyLogsToDataFile(OldestTransaction, _cancellationTokenSource.Token, tx, allowToFlushOverwrittenPages);

            if (IsDebugRecording)
            {
                _debugJournal.RecordFlushAction(DebugActionType.FlushEnd, tx);
            }
        }

        internal void AssertFlushingNotFailed()
        {
            var flushingTaskCopy = _flushingTask;
            if (flushingTaskCopy == null || flushingTaskCopy.IsFaulted == false)
                return;

            flushingTaskCopy.Wait();// force re-throw of error
        }

        internal void HandleDataDiskFullException(DiskFullException exception)
        {
            if(_options.ManualFlushing)
                return;

            _cancellationTokenSource.Cancel();
            _endOfDiskSpace = new EndOfDiskSpaceEvent(exception.DriveInfo);
        }

        internal IDisposable GetTemporaryPage(Transaction tx, out TemporaryPage tmp)
        {
            if (tx.Flags != TransactionFlags.ReadWrite)
                throw new ArgumentException("Temporary pages are only available for write transactions");
            if (_tempPagesPool.Count > 0)
            {
                tmp = _tempPagesPool.Dequeue();
                return tmp.ReturnTemporaryPageToPool;
            }

            tmp = new TemporaryPage();
            try
            {
                return tmp.ReturnTemporaryPageToPool = new ReturnTemporaryPageToPool(this, tmp);
            }
            catch (Exception)
            {
                tmp.Dispose();
                throw;
            }
        }

        private class ReturnTemporaryPageToPool : IDisposable
        {
            private readonly TemporaryPage _tmp;
            private readonly StorageEnvironment _env;

            public ReturnTemporaryPageToPool(StorageEnvironment env, TemporaryPage tmp)
            {
                _tmp = tmp;
                _env = env;
            }

            public void Dispose()
            {
                try
                {
                    _env._tempPagesPool.Enqueue(_tmp);
                }
                catch (Exception)
                {
                    _tmp.Dispose();
                    throw;
                }
            }
        }
    }
}

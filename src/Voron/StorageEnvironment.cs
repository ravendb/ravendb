using Sparrow.Collections;
using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Diagnostics;
using System.Linq;
using System.Runtime.InteropServices;
using System.Threading;
using System.Threading.Tasks;
using Voron.Data;
using Voron.Data.BTrees;
using Voron.Data.Fixed;
using Voron.Debugging;
using Voron.Exceptions;
using Voron.Impl;
using Voron.Impl.FileHeaders;
using Voron.Impl.FreeSpace;
using Voron.Impl.Journal;
using Voron.Impl.Paging;
using Voron.Impl.Scratch;
using Voron.Util;
using Voron.Util.Conversion;

namespace Voron
{
    public class StorageEnvironment : IDisposable
    {
        private readonly StorageEnvironmentOptions _options;

        private readonly ConcurrentSet<LowLevelTransaction> _activeTransactions = new ConcurrentSet<LowLevelTransaction>();

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
        private EndOfDiskSpaceEvent _endOfDiskSpace;
        private int _sizeOfUnflushedTransactionsInJournalFile;

        private readonly Queue<TemporaryPage> _tempPagesPool = new Queue<TemporaryPage>();

        public Guid DbId { get; set; }

        public StorageEnvironmentState State { get; private set; }

     
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
            State = new StorageEnvironmentState(null, nextPageNumber)
            {
                NextPageNumber = nextPageNumber,
                Options = Options
            };

            _transactionsCounter = (header->TransactionId == 0 ? entry.TransactionId : header->TransactionId);

            using (var tx = NewLowLevelTransaction(TransactionFlags.ReadWrite))
            {
                var root = Tree.Open(tx, null, header->TransactionId == 0 ? &entry.Root : &header->Root);

                tx.UpdateRootsIfNeeded(root);

                var treesTx = new Transaction(tx);

                var metadataTree = treesTx.ReadTree(Constants.MetadataTreeName);
                if (metadataTree == null)
                    throw new VoronUnrecoverableErrorException("Could not find metadata tree in database, possible mismatch / corruption?");

                var dbId = metadataTree.Read("db-id");
                if (dbId == null)
                    throw new VoronUnrecoverableErrorException("Could not find db id in metadata tree, possible mismatch / corruption?");

                var buffer = new byte[16];
                var dbIdBytes = dbId.Reader.Read(buffer, 0, 16);
                if (dbIdBytes != 16)
                    throw new VoronUnrecoverableErrorException("The db id value in metadata tree wasn't 16 bytes in size, possible mismatch / corruption?");

                DbId = new Guid(buffer);

                var schemaVersion = metadataTree.Read("schema-version");
                if (schemaVersion == null)
                    throw new VoronUnrecoverableErrorException("Could not find schema version in metadata tree, possible mismatch / corruption?");

                var schemaVersionVal = schemaVersion.Reader.ReadLittleEndianInt32();
                if (Options.SchemaVersion != 0 &&
                    schemaVersionVal != Options.SchemaVersion)
                {
                    throw new VoronUnrecoverableErrorException("The schema version of this database is expected to be " +
                                                       Options.SchemaVersion + " but is actually " + schemaVersionVal +
                                                       ". You need to upgrade the schema.");
                }

                tx.Commit();

            }
        }

        private void CreateNewDatabase()
        {
            const int initialNextPageNumber = 0;
            State = new StorageEnvironmentState(null, initialNextPageNumber)
            {
                Options = Options
            };
            using (var tx = NewLowLevelTransaction(TransactionFlags.ReadWrite))
            {
                var root = Tree.Create(tx, null);

                // important to first create the root trees, then set them on the env
                tx.UpdateRootsIfNeeded(root);

                var treesTx = new Transaction(tx);

                DbId = Guid.NewGuid();

                var metadataTree = treesTx.CreateTree(Constants.MetadataTreeName);
                metadataTree.Add("db-id", DbId.ToByteArray());
                metadataTree.Add("schema-version", EndianBitConverter.Little.GetBytes(Options.SchemaVersion));

                treesTx.PrepareForCommit();

                tx.Commit();
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
        public void Dispose()
        {
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

        public Transaction ReadTransaction()
        {
            return new Transaction(NewLowLevelTransaction(TransactionFlags.Read));
        }

        public Transaction WriteTransaction()
        {
            return new Transaction(NewLowLevelTransaction(TransactionFlags.ReadWrite, null));
        }

        internal LowLevelTransaction NewLowLevelTransaction(TransactionFlags flags, TimeSpan? timeout = null)
        {
            bool txLockTaken = false;
            try
            {
                if (flags == TransactionFlags.ReadWrite)
                {
                    var wait = timeout ?? (Debugger.IsAttached ? TimeSpan.FromMinutes(30) : TimeSpan.FromSeconds(30));
                    Monitor.TryEnter(_txWriter, wait, ref txLockTaken);
                    if (txLockTaken == false)
                    {
                        _flushWriter.Set();
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

                LowLevelTransaction tx;

                _txCommit.EnterReadLock();
                try
                {
                    long txId = flags == TransactionFlags.ReadWrite ? _transactionsCounter + 1 : _transactionsCounter;
                    tx = new LowLevelTransaction(this, txId, flags, _freeSpaceHandling);
                }
                finally
                {
                    _txCommit.ExitReadLock();
                }

                _activeTransactions.Add(tx);
                var state = _dataPager.PagerState;
                tx.AddPagerState(state);

                return tx;
            }
            catch (Exception)
            {
                if (txLockTaken)
                {
                    Monitor.Exit(_txWriter);
                }
                throw;
            }
        }


        public long NextWriteTransactionId
        {
            get { return Volatile.Read(ref _transactionsCounter) + 1; }
        }

        internal void TransactionAfterCommit(LowLevelTransaction tx)
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
            if (tx.IsLazyTransaction == false)
                _flushWriter.Set();
        }

        internal void TransactionCompleted(LowLevelTransaction tx)
        {
            if (_activeTransactions.TryRemove(tx) == false)
                return;

            if (tx.Flags != (TransactionFlags.ReadWrite))
                return;

            Monitor.Exit(_txWriter);
        }

        public StorageReport GenerateReport(Transaction tx, bool computeExactSizes = false)
        {
            var numberOfAllocatedPages = Math.Max(_dataPager.NumberOfAllocatedPages, NextPageNumber - 1); // async apply to data file task
            var numberOfFreePages = _freeSpaceHandling.AllPages(tx.LowLevelTransaction).Count;

            var trees = new List<Tree>();
            var fixedSizeTrees = new List<FixedSizeTree>();
            using (var rootIterator = tx.LowLevelTransaction.RootObjects.Iterate())
            {
                if (rootIterator.Seek(Slice.BeforeAllKeys))
                {
                    do
                    {
                        switch (tx.GetRootObjectType(rootIterator.CurrentKey))
                        {
                            case RootObjectType.VariableSizeTree:
                                var tree = tx.ReadTree(rootIterator.CurrentKey.ToString());
                                trees.Add(tree);
                                break;
                            case RootObjectType.EmbeddedFixedSizeTree:
                                break;
                            case RootObjectType.FixedSizeTree:
                                fixedSizeTrees.Add(tx.FixedTreeFor(rootIterator.CurrentKey, 0));
                                break;
                            default:
                                throw new ArgumentOutOfRangeException();
                        }
                    } while (rootIterator.MoveNext());
                }
            }

            var generator = new StorageReportGenerator(tx.LowLevelTransaction);

            return generator.Generate(new ReportInput
            {
                NumberOfAllocatedPages = numberOfAllocatedPages,
                NumberOfFreePages = numberOfFreePages,
                NextPageNumber = NextPageNumber,
                Journals = Journal.Files.ToList(),
                Trees = trees,
                FixedSizeTrees = fixedSizeTrees,
                IsLightReport = !computeExactSizes
            });
        }

        public EnvironmentStats Stats()
        {
            using (var tx = NewLowLevelTransaction(TransactionFlags.Read))
            {
                var numberOfAllocatedPages = Math.Max(_dataPager.NumberOfAllocatedPages, State.NextPageNumber - 1); // async apply to data file task

                return new EnvironmentStats
                {
                    FreePagesOverhead = FreeSpaceHandling.GetFreePagesOverhead(tx),
                    RootPages = tx.RootObjects.State.PageCount,
                    UnallocatedPagesAtEndOfFile = _dataPager.NumberOfAllocatedPages - NextPageNumber,
                    UsedDataFileSizeInBytes = (State.NextPageNumber - 1) * Options.PageSize,
                    AllocatedDataFileSizeInBytes = numberOfAllocatedPages * Options.PageSize,
                    NextWriteTransactionId = NextWriteTransactionId,
                    ActiveTransactions = ActiveTransactions
                };
            }
        }

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

                    var sizeOfUnflushedTransactionsInJournalFile = Volatile.Read(ref _sizeOfUnflushedTransactionsInJournalFile);
                    if (sizeOfUnflushedTransactionsInJournalFile == 0)
                        continue;

                    if (hasWrites == false || sizeOfUnflushedTransactionsInJournalFile >= _options.MaxNumberOfPagesInJournalBeforeFlush)
                    {
                        Interlocked.Add(ref _sizeOfUnflushedTransactionsInJournalFile, -sizeOfUnflushedTransactionsInJournalFile);

                        // we either reached our the max size we allow in the journal file before flush flushing (and therefor require a flush)
                        // we didn't have a write in the idle timeout (default: 5 seconds), this is probably a good time to try and do a proper flush
                        // while there isn't any other activity going on.

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
                            throw new VoronUnrecoverableErrorException("Error occurred during flushing journals to the data file", new Win32Exception(sehException.HResult));
                        }
                    }
                }
            }, TaskCreationOptions.LongRunning);
        }

        public void FlushLogToDataFile(LowLevelTransaction tx = null, bool allowToFlushOverwrittenPages = false)
        {
            if (_options.ManualFlushing == false)
                throw new NotSupportedException("Manual flushes are not set in the storage options, cannot manually flush!");

            ForceLogFlushToDataFile(tx, allowToFlushOverwrittenPages);
        }

        internal void ForceLogFlushToDataFile(LowLevelTransaction tx, bool allowToFlushOverwrittenPages)
        {
            _journal.Applicator.ApplyLogsToDataFile(OldestTransaction, _cancellationTokenSource.Token, tx, allowToFlushOverwrittenPages);
        }

        internal void AssertFlushingNotFailed()
        {
            var flushingTaskCopy = _flushingTask;
            if (flushingTaskCopy == null || flushingTaskCopy.IsFaulted == false)
                return;

            flushingTaskCopy.Wait(); // force re-throw of error
        }

        internal void HandleDataDiskFullException(DiskFullException exception)
        {
            if (_options.ManualFlushing)
                return;

            _cancellationTokenSource.Cancel();
            _endOfDiskSpace = new EndOfDiskSpaceEvent(exception.DriveInfo);
        }

        internal IDisposable GetTemporaryPage(LowLevelTransaction tx, out TemporaryPage tmp)
        {
            if (tx.Flags != TransactionFlags.ReadWrite)
                throw new ArgumentException("Temporary pages are only available for write transactions");
            if (_tempPagesPool.Count > 0)
            {
                tmp = _tempPagesPool.Dequeue();
                return tmp.ReturnTemporaryPageToPool;
            }

            tmp = new TemporaryPage(Options);
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

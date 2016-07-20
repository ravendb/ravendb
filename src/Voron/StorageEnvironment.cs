using Sparrow;
using Sparrow.Collections;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.ComponentModel;
using System.Diagnostics;
using System.Linq;
using System.Runtime.ExceptionServices;
using System.Runtime.InteropServices;
using System.Threading;
using System.Threading.Tasks;
using Sparrow.Logging;
using Voron.Data;
using Voron.Data.BTrees;
using Voron.Data.Fixed;
using Voron.Debugging;
using Voron.Exceptions;
using Voron.Global;
using Voron.Impl;
using Voron.Impl.FileHeaders;
using Voron.Impl.FreeSpace;
using Voron.Impl.Journal;
using Voron.Impl.Paging;
using Voron.Impl.Scratch;
using Voron.Platform.Posix;
using Voron.Platform.Win32;
using Voron.Util;
using Voron.Util.Conversion;

namespace Voron
{
    public class StorageEnvironment : IDisposable
    {
        private readonly LoggerSetup _loggerSetup;

        private static readonly Lazy<GlobalFlushingBehavior> GlobalFlusher = new Lazy<GlobalFlushingBehavior>(() =>
        {
            var flusher = new GlobalFlushingBehavior();
            var thread = new Thread(flusher.VoronEnvironmentFlushing)
            {
                IsBackground = true,
                Name = "Voron Global Flushing Thread"
            };
            thread.Start();
            return flusher;
        });

        /// <summary>
        /// This is the shared storage where we are going to store all the static constants for names. 
        /// WARNING: This context will never be released, so only static constants should be added here.
        /// </summary>
        public static readonly ByteStringContext LabelsContext = new ByteStringContext(ByteStringContext.MinBlockSizeInBytes);

        private readonly StorageEnvironmentOptions _options;

        private readonly ConcurrentSet<LowLevelTransaction> _activeTransactions = new ConcurrentSet<LowLevelTransaction>();

        private readonly AbstractPager _dataPager;
        private ExceptionDispatchInfo _flushingTaskFailure;
        private readonly WriteAheadJournal _journal;
        private readonly object _txWriter = new object();
        internal readonly ReaderWriterLockSlim FlushInProgressLock = new ReaderWriterLockSlim(LockRecursionPolicy.SupportsRecursion);
        private readonly ReaderWriterLockSlim _txCommit = new ReaderWriterLockSlim();

        private long _transactionsCounter;
        private readonly IFreeSpaceHandling _freeSpaceHandling;
        private readonly HeaderAccessor _headerAccessor;
        public bool IsFlushingScratchBuffer { get; set; }

        private CancellationTokenSource _cancellationTokenSource = new CancellationTokenSource();
        private readonly ScratchBufferPool _scratchBufferPool;
        private EndOfDiskSpaceEvent _endOfDiskSpace;
        private int _sizeOfUnflushedTransactionsInJournalFile;

        private readonly Queue<TemporaryPage> _tempPagesPool = new Queue<TemporaryPage>();
        public bool Disposed;

        public Guid DbId { get; set; }

        public StorageEnvironmentState State { get; private set; }

     
        public StorageEnvironment(StorageEnvironmentOptions options, LoggerSetup loggerSetup)
        {
            try
            {
                _loggerSetup = loggerSetup;
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
                    Task.Run(IdleFlushTimer);
            }
            catch (Exception)
            {
                Dispose();
                throw;
            }
        }

        private async Task IdleFlushTimer()
        {
            var cancellationToken = _cancellationTokenSource.Token;

            while (cancellationToken.IsCancellationRequested == false)
            {
                if (Disposed)
                    return;

                try
                {
                    await Task.Delay(Options.IdleFlushTimeout, cancellationToken);
                }
                catch (ObjectDisposedException)
                {
                    return;
                }
                catch (OperationCanceledException)
                {
                    return;
                }
                GlobalFlusher.Value.MaybeFlushEnvironment(this);
            }
        }

        public ScratchBufferPool ScratchBufferPool => _scratchBufferPool;

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
            Disposed = true;
            try
            {
                // if there is a pending flush operation, we need to wait for it
                if (_journal != null) // error during ctor
                {
                    bool lockTaken = false;
                    using (_journal.Applicator.TryTakeFlushingLock(ref lockTaken))
                    {
                        if (lockTaken == false)
                        {
                            // if we are here, then we didn't get the flush lock, so it is currently being run
                            // we need to wait for it to complete (so we won't be shutting down the db while we 
                            // are flushing and maybe access in valid memory.
                            using (_journal.Applicator.TakeFlushingLock())
                            {
                                // when we are here, we know that we aren't flushing, and we can dispose, 
                                // any future calls to flush will abort because we are marked as disposed
                            }
                        }
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
                        disposable?.Dispose();
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

        public Transaction ReadTransaction(ByteStringContext context = null)
        {
            return new Transaction(NewLowLevelTransaction(TransactionFlags.Read, context));
        }

        public Transaction WriteTransaction(ByteStringContext context = null)
        {
            return new Transaction(NewLowLevelTransaction(TransactionFlags.ReadWrite, context, null));
        }

        internal LowLevelTransaction NewLowLevelTransaction(TransactionFlags flags, ByteStringContext context = null, TimeSpan? timeout = null)
        {
            bool txLockTaken = false;
            bool flushInProgressReadLockTaken = false;
            try
            {
                if (flags == TransactionFlags.ReadWrite)
                {
                    var wait = timeout ?? (Debugger.IsAttached ? TimeSpan.FromMinutes(30) : TimeSpan.FromSeconds(30));
                    if (FlushInProgressLock.IsWriteLockHeld == false)
                        flushInProgressReadLockTaken = FlushInProgressLock.TryEnterReadLock(wait);
                    Monitor.TryEnter(_txWriter, wait, ref txLockTaken);
                    if (txLockTaken == false || (flushInProgressReadLockTaken == false && FlushInProgressLock.IsWriteLockHeld == false))
                    {
                        GlobalFlusher.Value.MaybeFlushEnvironment(this);
                        throw new TimeoutException("Waited for " + wait +
                                                    " for transaction write lock, but could not get it");
                    }
                    if (_endOfDiskSpace != null)
                    {
                        if (_endOfDiskSpace.CanContinueWriting)
                        {
                            _flushingTaskFailure = null;
                            _endOfDiskSpace = null;
                            _cancellationTokenSource = new CancellationTokenSource();
                            Task.Run(IdleFlushTimer);
                            GlobalFlusher.Value.MaybeFlushEnvironment(this);
                        }
                    }
                }

                LowLevelTransaction tx;

                _txCommit.EnterReadLock();
                try
                {
                    long txId = flags == TransactionFlags.ReadWrite ? _transactionsCounter + 1 : _transactionsCounter;
                    tx = new LowLevelTransaction(this, txId, flags, _freeSpaceHandling, context);
                }
                finally
                {
                    _txCommit.ExitReadLock();
                }

                _activeTransactions.Add(tx);
                var state = _dataPager.PagerState;
                tx.EnsurePagerStateReference(state);

                return tx;
            }
            catch (Exception)
            {
                if (txLockTaken)
                {
                    Monitor.Exit(_txWriter);
                }
                if (flushInProgressReadLockTaken)
                {
                    FlushInProgressLock.ExitReadLock();
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
                GlobalFlusher.Value.MaybeFlushEnvironment(this);
        }

        internal void TransactionCompleted(LowLevelTransaction tx)
        {
            if (_activeTransactions.TryRemove(tx) == false)
                return;

            if (tx.Flags != (TransactionFlags.ReadWrite))
                return;

            Monitor.Exit(_txWriter);
            if (FlushInProgressLock.IsReadLockHeld)
                FlushInProgressLock.ExitReadLock();
        }

        public StorageReport GenerateReport(Transaction tx, bool computeExactSizes = false)
        {
            var numberOfAllocatedPages = Math.Max(_dataPager.NumberOfAllocatedPages, NextPageNumber - 1); // async apply to data file task
            var numberOfFreePages = _freeSpaceHandling.AllPages(tx.LowLevelTransaction).Count;

            var trees = new List<Tree>();
            var fixedSizeTrees = new List<FixedSizeTree>();
            using (var rootIterator = tx.LowLevelTransaction.RootObjects.Iterate(false))
            {
                if (rootIterator.Seek(Slices.BeforeAllKeys))
                {
                    do
                    {
                        var curretKey = rootIterator.CurrentKey.Clone(tx.Allocator);
                        switch (tx.GetRootObjectType(curretKey))
                        {
                            case RootObjectType.VariableSizeTree:
                                var tree = tx.ReadTree(curretKey.ToString());
                                trees.Add(tree);
                                break;
                            case RootObjectType.EmbeddedFixedSizeTree:
                                break;
                            case RootObjectType.FixedSizeTree:
                                fixedSizeTrees.Add(tx.FixedTreeFor(curretKey, 0));
                                break;
                            default:
                                throw new ArgumentOutOfRangeException();
                        }
                    }
                    while (rootIterator.MoveNext());
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

        private class GlobalFlushingBehavior
        {
            private readonly ConcurrentQueue<StorageEnvironment> _maybeNeedToFlush = new ConcurrentQueue<StorageEnvironment>();
            private readonly ManualResetEventSlim _flushWriterEvent = new ManualResetEventSlim();
            private readonly SemaphoreSlim _concurrentFlushes = new SemaphoreSlim(MaxConcurrentFlushes);
            private const int MaxConcurrentFlushes = 10;

            public void VoronEnvironmentFlushing()
            {
                // We want this to always run, even if we dispose / create new storage env, this is 
                // static for the life time of the process, and environments will register / unregister from
                // it as needed
                while (true)
                {
                    _flushWriterEvent.Wait();
                    _flushWriterEvent.Reset();

                    StorageEnvironment envToFlush;
                    while (_maybeNeedToFlush.TryDequeue(out envToFlush))
                    {
                        if (envToFlush.Disposed)
                            continue;

                        var sizeOfUnflushedTransactionsInJournalFile = Volatile.Read(ref envToFlush._sizeOfUnflushedTransactionsInJournalFile);

                        if (sizeOfUnflushedTransactionsInJournalFile == 0)
                            continue; // nothing to do


                        if (sizeOfUnflushedTransactionsInJournalFile <
                            envToFlush._options.MaxNumberOfPagesInJournalBeforeFlush)
                        {
                            // we haven't reached the point where we have to flush, but we might want to, if we have enough 
                            // resources available, if we have more than half the flushing capacity, we can do it now, otherwise, we'll wait
                            // until it is actually required.
                            if (_concurrentFlushes.CurrentCount > MaxConcurrentFlushes/2)
                                continue;
                        }

                        Interlocked.Add(ref envToFlush._sizeOfUnflushedTransactionsInJournalFile, -sizeOfUnflushedTransactionsInJournalFile);

                        _concurrentFlushes.Wait();

                        if (ThreadPool.QueueUserWorkItem(env =>
                        {
                            var storageEnvironment = ((StorageEnvironment)env);
                            if (storageEnvironment.Disposed)
                                return;

                            try
                            {
                                storageEnvironment.BackgroundFlushWritesToDataFile();
                            }
                            catch (Exception e)
                            {
                                storageEnvironment._flushingTaskFailure = ExceptionDispatchInfo.Capture(e.InnerException);
                            }
                            finally
                            {
                                _concurrentFlushes.Release();
                            }
                        }, envToFlush) == false)
                        {
                            MaybeFlushEnvironment(envToFlush);// re-register if the thread pool is full
                            Thread.Sleep(0); // but let it give up the execution slice so we'll let the TP time to run
                        }
                    }}
                
            }


            public void MaybeFlushEnvironment(StorageEnvironment env)
            {
                _maybeNeedToFlush.Enqueue(env);
                _flushWriterEvent.Set();
            }
        }

        


        private void BackgroundFlushWritesToDataFile()
        {
            try
            {
                _journal.Applicator.ApplyLogsToDataFile(OldestTransaction, _cancellationTokenSource.Token,
                    // we intentionally don't wait, if the flush lock is held, something else is flushing, so we don't need
                    // to hold the thread
                    TimeSpan.Zero);
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
            catch (Exception e)
            {
                throw new VoronUnrecoverableErrorException("Error occurred during flushing journals to the data file",
                    e);
            }
        }

        public void FlushLogToDataFile(LowLevelTransaction tx = null, bool allowToFlushOverwrittenPages = false)
        {
            if (_options.ManualFlushing == false)
                throw new NotSupportedException("Manual flushes are not set in the storage options, cannot manually flush!");

            ForceLogFlushToDataFile(tx, allowToFlushOverwrittenPages);
        }

        public void ForceLogFlushToDataFile(LowLevelTransaction tx, bool allowToFlushOverwrittenPages)
        {
            _journal.Applicator.ApplyLogsToDataFile(OldestTransaction, _cancellationTokenSource.Token, 
                Debugger.IsAttached ? TimeSpan.FromMinutes(30) : TimeSpan.FromSeconds(30), 
                tx, allowToFlushOverwrittenPages);
        }

        internal void AssertFlushingNotFailed()
        {
            _flushingTaskFailure?.Throw(); // force re-throw of error
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

        public bool TryEnterTxLock()
        {
            bool txLockTaken = false;
            bool flushInProgressReadLockTaken = false;

            try
            {
                var wait = Debugger.IsAttached ? TimeSpan.FromMinutes(30) : TimeSpan.FromSeconds(30);
                if (FlushInProgressLock.IsWriteLockHeld == false)
                    flushInProgressReadLockTaken = FlushInProgressLock.TryEnterReadLock(wait);

                if (flushInProgressReadLockTaken == false && FlushInProgressLock.IsWriteLockHeld == false)
                    return false;

                Monitor.TryEnter(_txWriter, wait, ref txLockTaken);
                if (txLockTaken == false)
                {
                    FlushInProgressLock.ExitReadLock();
                    return false;
                }
            }
            catch (Exception)
            {
                if (txLockTaken)
                {
                    Monitor.Exit(_txWriter);
                }
                if (flushInProgressReadLockTaken)
                {
                    FlushInProgressLock.ExitReadLock();
                }
                throw;
            }
            return true;
        }

        public TransactionsModeResult SetTransactionMode(TransactionsMode mode, TimeSpan duration, LowLevelTransaction tx)
        {
            var oldMode = Options.TransactionsMode;

            if (oldMode == mode)
                return TransactionsModeResult.ModeAlreadySet;

            Options.TransactionsMode = mode;
            if (duration == TimeSpan.FromMinutes(0)) // infinte
                Options.NonSafeTransactionExpiration = null;
            else
                Options.NonSafeTransactionExpiration = DateTime.Now + duration;

            bool locksTaken = false;
            try
            {
                locksTaken = TryEnterTxLock();

                if (locksTaken == false)
                {
                    return TransactionsModeResult.CannotSetMode;
                }

                if (oldMode == TransactionsMode.Lazy)
                    CommitNonLazy(tx);

                if (oldMode == TransactionsMode.Danger)
                    Journal.TruncateJournal(Options.PageSize);

                switch (mode)
                {
                    case TransactionsMode.Safe:
                    case TransactionsMode.Lazy:
                        {
                            Options.PosixOpenFlags = OpenFlags.O_DSYNC | OpenFlags.O_DIRECT;
                            Options.WinOpenFlags = Win32NativeFileAttributes.Write_Through |
                                                   Win32NativeFileAttributes.NoBuffering;
                        }
                        break;

                    case TransactionsMode.Danger:
                        {
                            Options.PosixOpenFlags = 0;
                            Options.WinOpenFlags = Win32NativeFileAttributes.None;
                            Journal.TruncateJournal(Options.PageSize);
                        }
                        break;
                    default:
                        {
                            throw new InvalidOperationException("Query string value 'mode' is not a valid mode: " + mode);
                        }
                }
            }
            finally
            {
                if (locksTaken)
                {
                    Monitor.Exit(_txWriter);
                    FlushInProgressLock.ExitReadLock();
                }
            }

            return TransactionsModeResult.SetModeSuccessfully;
        }

        public void CommitNonLazy(LowLevelTransaction tx) // TODO :: is it ok to use directly LowLevelTx and call Commit ?
        {
            // this non lazy transaction forces the journal to actually
            // flush everything
            tx.IsLazyTransaction = false;
            tx.Commit();
        }
    }
}

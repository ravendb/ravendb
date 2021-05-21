using Sparrow;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.ComponentModel;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Runtime.ExceptionServices;
using System.Runtime.InteropServices;
using System.Threading;
using System.Threading.Tasks;
using Sparrow.Logging;
using Sparrow.Server;
using Sparrow.Server.Exceptions;
using Sparrow.Server.Utils;
using Sparrow.Threading;
using Sparrow.Utils;
using Voron.Data;
using Voron.Data.BTrees;
using Voron.Data.Compression;
using Voron.Data.Fixed;
using Voron.Data.Tables;
using Voron.Debugging;
using Voron.Exceptions;
using Voron.Impl;
using Voron.Impl.FileHeaders;
using Voron.Impl.FreeSpace;
using Voron.Impl.Journal;
using Voron.Impl.Paging;
using Voron.Impl.Scratch;
using Voron.Schema;
using Voron.Util;
using Voron.Util.Conversion;
using Constants = Voron.Global.Constants;

namespace Voron
{
    public delegate bool UpgraderDelegate(SchemaUpgradeTransactions transactions, int currentVersion, out int versionAfterUpgrade);

    public class StorageEnvironment : IDisposable
    {
        internal class IndirectReference
        {
            public StorageEnvironment Owner;
        }

        public Table.CompressionDictionariesHolder CompressionDictionariesHolder = new Table.CompressionDictionariesHolder();
        
        internal IndirectReference SelfReference = new IndirectReference();

        public void SuggestSyncDataFile()
        {
            GlobalFlushingBehavior.GlobalFlusher.Value.SuggestSyncEnvironment(this);
        }

        public void ForceSyncDataFile()
        {
            GlobalFlushingBehavior.GlobalFlusher.Value.ForceSyncEnvironment(this);
        }

        /// <summary>
        /// This is the shared storage where we are going to store all the static constants for names.
        /// WARNING: This context will never be released, so only static constants should be added here.
        /// </summary>
        private static readonly ByteStringContext _labelsContext = new ByteStringContext(SharedMultipleUseFlag.None, ByteStringContext.MinBlockSizeInBytes);

        public static IDisposable GetStaticContext(out ByteStringContext ctx)
        {
            Monitor.Enter(_labelsContext);

            ctx = _labelsContext;

            return new DisposableAction(() => Monitor.Exit(_labelsContext));
        }

        private readonly StorageEnvironmentOptions _options;

        public readonly ActiveTransactions ActiveTransactions = new ActiveTransactions();

        private readonly AbstractPager _dataPager;

        internal readonly LowLevelTransaction.WriteTransactionPool WriteTransactionPool =
            new LowLevelTransaction.WriteTransactionPool();

        private readonly WriteAheadJournal _journal;
        private readonly SemaphoreSlim _transactionWriter = new SemaphoreSlim(1, 1);
        private NativeMemory.ThreadStats _currentWriteTransactionHolder;
        private readonly AsyncManualResetEvent _writeTransactionRunning = new AsyncManualResetEvent();
        internal readonly ThreadHoppingReaderWriterLock FlushInProgressLock = new ThreadHoppingReaderWriterLock();
        private readonly ReaderWriterLockSlim _txCreation = new ReaderWriterLockSlim();
        private readonly CountdownEvent _envDispose = new CountdownEvent(1);

        private long _transactionsCounter;
        private readonly IFreeSpaceHandling _freeSpaceHandling;
        private readonly HeaderAccessor _headerAccessor;
        private readonly DecompressionBuffersPool _decompressionBuffers;

        private readonly CancellationTokenSource _cancellationTokenSource = new CancellationTokenSource();
        private readonly ScratchBufferPool _scratchBufferPool;
        private EndOfDiskSpaceEvent _endOfDiskSpace;

        private int _idleFlushTimerFailures = 0;
        private Task _idleFlushTimer = Task.CompletedTask;

        internal DateTime LastFlushTime;

        public DateTime LastWorkTime;

        private readonly ConcurrentQueue<TemporaryPage> _tempPagesPool = new ConcurrentQueue<TemporaryPage>();
        public bool Disposed;
        private readonly Logger _log;
        public static int MaxConcurrentFlushes = 10; // RavenDB-5221
        public static int NumOfConcurrentSyncsPerPhysDrive;
        public int TimeToSyncAfterFlushInSec;

        public Guid DbId { get; set; }

        public StorageEnvironmentState State { get; private set; }

        public event Action OnLogsApplied;

        private readonly long[] _validPages;

        public bool IsNew { get; }

        public StorageEnvironment(StorageEnvironmentOptions options)
        {
            try
            {
                SelfReference.Owner = this;
                _log = LoggingSource.Instance.GetLogger<StorageEnvironment>(options.BasePath.FullPath);
                _options = options;
                _dataPager = options.DataPager;
                _freeSpaceHandling = new FreeSpaceHandling();
                _headerAccessor = new HeaderAccessor(this);
                NumOfConcurrentSyncsPerPhysDrive = options.NumOfConcurrentSyncsPerPhysDrive;
                TimeToSyncAfterFlushInSec = options.TimeToSyncAfterFlushInSec;

                Debug.Assert(_dataPager.NumberOfAllocatedPages != 0);

                var remainingBits = _dataPager.NumberOfAllocatedPages % (8 * sizeof(long));

                _validPages = new long[_dataPager.NumberOfAllocatedPages / (8 * sizeof(long)) + (remainingBits == 0 ? 0 : 1)];
                _validPages[_validPages.Length - 1] |= unchecked(((long)ulong.MaxValue << (int)remainingBits));

                _decompressionBuffers = new DecompressionBuffersPool(options);

                options.InvokeOnDirectoryInitialize();

                IsNew = _headerAccessor.Initialize();

                _scratchBufferPool = new ScratchBufferPool(this);

                options.SetDurability();

                _journal = new WriteAheadJournal(this);

                if (options.Encryption.HasExternalJournalCompressionBufferHandlerRegistration) 
                    options.Encryption.SetExternalCompressionBufferHandler(_journal);

                if (IsNew)
                    CreateNewDatabase();
                else // existing db, let us load it
                    LoadExistingDatabase();

                Debug.Assert(_options.ManualFlushing || _idleFlushTimer.IsCompleted == false, "_idleFlushTimer.IsCompleted == false"); // initialized by transaction on storage create/open

                if (IsNew == false && _options.ManualSyncing == false)
                    SuggestSyncDataFile(); // let's suggest syncing data file after the recovery

                // Ensure we are always have the prefetcher available.
                GC.KeepAlive(GlobalPrefetchingBehavior.GlobalPrefetcher.Value);
            }
            catch (Exception)
            {
                Dispose();
                throw;
            }
        }
        private async Task IdleFlushTimer()
        {
            try
            {
                var cancellationToken = _cancellationTokenSource.Token;

                while (cancellationToken.IsCancellationRequested == false)
                {
                    if (Disposed)
                        return;

                    if (Options.ManualFlushing)
                        return;

                    try
                    {
                        if (await _writeTransactionRunning.WaitAsync(TimeSpan.FromMilliseconds(Options.IdleFlushTimeout)) == false)
                        {
                            if (Journal.Applicator.ShouldFlush)
                                GlobalFlushingBehavior.GlobalFlusher.Value.MaybeFlushEnvironment(this);

                            else if (Journal.Applicator.ShouldSync)
                                SuggestSyncDataFile();
                        }
                        else
                        {
                            await TimeoutManager.WaitFor(TimeSpan.FromMilliseconds(1000), cancellationToken).ConfigureAwait(false);
                        }
                    }
                    catch (ObjectDisposedException)
                    {
                        return;
                    }
                    catch (OperationCanceledException)
                    {
                        return;
                    }
                }
            }
            catch (Exception e)
            {
                int numberOfFailures = Interlocked.Increment(ref _idleFlushTimerFailures);

                string message = $"{nameof(IdleFlushTimer)} failed (numberOfFailures: {numberOfFailures}), unable to schedule flush / syncs of data file. Will be restarted on new write transaction";

                if (_log.IsOperationsEnabled)
                {
                    _log.Operations(message, e);
                }

                try
                {
                    _options.InvokeRecoverableFailure(message, e);
                }
                catch
                {
                    // ignored 
                }
            }
        }

        public ScratchBufferPool ScratchBufferPool => _scratchBufferPool;

        private unsafe void LoadExistingDatabase()
        {
            var header = stackalloc TransactionHeader[1];
            bool hadIntegrityIssues;

            Options.AddToInitLog?.Invoke("Starting Recovery");
            hadIntegrityIssues = _journal.RecoverDatabase(header, Options.AddToInitLog);
            var successString = hadIntegrityIssues ? "(with integrity issues)" : "(successfully)";
            Options.AddToInitLog?.Invoke($"Recovery Ended {successString}");

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

            Interlocked.Exchange(ref _transactionsCounter, header->TransactionId == 0 ? entry.TransactionId : header->TransactionId);
            var transactionPersistentContext = new TransactionPersistentContext(true);
            using (var tx = NewLowLevelTransaction(transactionPersistentContext, TransactionFlags.ReadWrite))
            using (var root = Tree.Open(tx, null, Constants.RootTreeNameSlice, header->TransactionId == 0 ? &entry.Root : &header->Root))
            using (var writeTx = new Transaction(tx))
            {
                tx.UpdateRootsIfNeeded(root);

                var metadataTree = writeTx.ReadTree(Constants.MetadataTreeNameSlice);
                if (metadataTree == null)
                    VoronUnrecoverableErrorException.Raise(tx,
                        "Could not find metadata tree in database, possible mismatch / corruption?");

                Debug.Assert(metadataTree != null);
                // ReSharper disable once PossibleNullReferenceException
                var dbId = metadataTree.Read("db-id");
                if (dbId == null)
                    VoronUnrecoverableErrorException.Raise(tx,
                        "Could not find db id in metadata tree, possible mismatch / corruption?");

                var buffer = new byte[16];
                Debug.Assert(dbId != null);
                // ReSharper disable once PossibleNullReferenceException
                var dbIdBytes = dbId.Reader.Read(buffer, 0, 16);
                if (dbIdBytes != 16)
                    VoronUnrecoverableErrorException.Raise(tx,
                        "The db id value in metadata tree wasn't 16 bytes in size, possible mismatch / corruption?");

                var databaseGuidId = _options.GenerateNewDatabaseId == false ? new Guid(buffer) : Guid.NewGuid();

                FillBase64Id(databaseGuidId);

                if (_options.GenerateNewDatabaseId)
                {
                    // save the new database id
                    metadataTree?.Add("db-id", DbId.ToByteArray());
                }

                tx.Commit();
            }

            UpgradeSchemaIfRequired();
        }

        private void UpgradeSchemaIfRequired()
        {
            Options.BeforeSchemaUpgrade?.Invoke(this);

            try
            {
                int schemaVersionVal;

                var readPersistentContext = new TransactionPersistentContext(true);
                using (var readTxInner = NewLowLevelTransaction(readPersistentContext, TransactionFlags.Read))
                using (var readTx = new Transaction(readTxInner))
                {
                    var metadataTree = readTx.ReadTree(Constants.MetadataTreeNameSlice);

                    var schemaVersion = metadataTree.Read("schema-version");
                    if (schemaVersion == null)
                        SchemaErrorException.Raise(this, "Could not find schema version in metadata tree, possible mismatch / corruption?");

                    schemaVersionVal = schemaVersion.Reader.ReadLittleEndianInt32();
                }

                if (Options.SchemaVersion != 0 &&
                    schemaVersionVal != Options.SchemaVersion)
                {
                    if (schemaVersionVal > Options.SchemaVersion)
                        ThrowSchemaUpgradeRequired(schemaVersionVal, $"Your data has a schema version '{schemaVersionVal}' that is newer than currently supported by database '{Options.SchemaVersion}'");

                    UpgraderDelegate upgrader = Options.SchemaUpgrader;
                    if (upgrader == null)
                        ThrowSchemaUpgradeRequired(schemaVersionVal, "You need to upgrade the schema but there is no schema upgrader provided.");

                    UpgradeSchema(schemaVersionVal, upgrader);
                }
            }
            catch (Exception e)
            {
                if (e is SchemaErrorException)
                    throw;
                VoronUnrecoverableErrorException.Raise(this, e.Message, e);
                throw;
            }
        }

        private void UpgradeSchema(int schemaVersionVal, UpgraderDelegate upgrader)
        {
            while (schemaVersionVal < Options.SchemaVersion)
            {
                using (var transactions = new SchemaUpgradeTransactions(this))
                {
                    // ReSharper disable once PossibleNullReferenceException
                    if (upgrader(transactions, schemaVersionVal, out schemaVersionVal) == false)
                        break;

                    var metadataTree = transactions.Write.ReadTree(Constants.MetadataTreeNameSlice);
                    //schemaVersionVal++;

                    metadataTree.Add("schema-version", EndianBitConverter.Little.GetBytes(schemaVersionVal));

                    transactions.Commit();
                }
            }

            if (schemaVersionVal != Options.SchemaVersion)
                ThrowSchemaUpgradeRequired(schemaVersionVal, "You need to upgrade the schema.");
        }

        private void ThrowSchemaUpgradeRequired(int schemaVersionVal, string message)
        {
            SchemaErrorException.Raise(this,
                "The schema version of this database is expected to be " +
                Options.SchemaVersion + " but is actually " + schemaVersionVal +
                ". " + message);
        }

        public unsafe void FillBase64Id(Guid databaseGuidId)
        {
            DbId = databaseGuidId;
            fixed (char* pChars = Base64Id)
            {
                var result = Base64.ConvertToBase64ArrayUnpadded(pChars, (byte*)&databaseGuidId, 0, 16);
                Debug.Assert(result == 22);
            }

            _options.SetEnvironmentId(databaseGuidId);
        }

        public string Base64Id { get; } = new string(' ', 22);

        private void CreateNewDatabase()
        {
            const int initialNextPageNumber = 0;
            State = new StorageEnvironmentState(null, initialNextPageNumber)
            {
                Options = Options
            };

            if (Options.SimulateFailureOnDbCreation)
                ThrowSimulateFailureOnDbCreation();

            var transactionPersistentContext = new TransactionPersistentContext();
            using (var tx = NewLowLevelTransaction(transactionPersistentContext, TransactionFlags.ReadWrite))
            using (var root = Tree.Create(tx, null, Constants.RootTreeNameSlice))
            {

                // important to first create the root trees, then set them on the env
                tx.UpdateRootsIfNeeded(root);

                using (var treesTx = new Transaction(tx))
                {

                    FillBase64Id(Guid.NewGuid());

                    var metadataTree = treesTx.CreateTree(Constants.MetadataTreeNameSlice);
                    metadataTree.Add("db-id", DbId.ToByteArray());
                    metadataTree.Add("schema-version", EndianBitConverter.Little.GetBytes(Options.SchemaVersion));

                    treesTx.PrepareForCommit();

                    tx.Commit();
                }
            }

        }

        public IFreeSpaceHandling FreeSpaceHandling => _freeSpaceHandling;

        public HeaderAccessor HeaderAccessor => _headerAccessor;

        public long NextPageNumber => State.NextPageNumber;

        public StorageEnvironmentOptions Options => _options;

        public WriteAheadJournal Journal => _journal;

        public DecompressionBuffersPool DecompressionBuffers => _decompressionBuffers;

        public void Dispose()
        {

            if (_envDispose.IsSet)
                return; // already disposed

            _cancellationTokenSource.Cancel();
            try
            {
                SelfReference.Owner = null;

                if (_journal != null) // error during ctor
                {
                    // if there is a pending flush operation, we need to wait for it
                    bool lockTaken = false;
                    using (_journal.Applicator.TryTakeFlushingLock(ref lockTaken))
                    {
                        // note that we have to set the dispose flag when under the lock
                        // (either with TryTake or Take), to avoid data race between the
                        // flusher & the dispose. The flusher will check the dispose status
                        // only after it successfully took the lock.

                        if (lockTaken == false)
                        {
                            // if we are here, then we didn't get the flush lock, so it is currently being run
                            // we need to wait for it to complete (so we won't be shutting down the db while we
                            // are flushing and maybe access invalid memory.
                            using (_journal.Applicator.TakeFlushingLock())
                            {
                                // when we are here, we know that we aren't flushing, and we can dispose,
                                // any future calls to flush will abort because we are marked as disposed

                                Disposed = true;
                                _journal.Applicator.WaitForSyncToCompleteOnDispose();
                                MoveEnvironmentToDisposeState();
                            }
                        }
                        else
                        {
                            Disposed = true;
                            _journal.Applicator.WaitForSyncToCompleteOnDispose();
                            MoveEnvironmentToDisposeState();
                        }
                    }
                }
                else
                {
                    MoveEnvironmentToDisposeState();
                }
            }
            finally
            {
                var errors = new List<Exception>();

                OnLogsApplied = null;

                _options.NullifyHandlers();

                foreach (var disposable in new IDisposable[]
                {
                    _journal,
                    _headerAccessor,
                    _scratchBufferPool,
                    _decompressionBuffers,
                    _options.OwnsPagers ? _options : null,
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

        private void MoveEnvironmentToDisposeState()
        {
            _envDispose.Signal(); // release the owner count
            if (_envDispose.Wait(Options.DisposeWaitTime) == false)
            {
                if (_envDispose.TryAddCount(1))
                // try restore the previous signal, if it failed, the _envDispose is signaled
                {
                    var activeTxs = ActiveTransactions.AllTransactions;
                    ThrowInvalidDisposeDuringActiveTransactions(activeTxs);
                }
            }
        }

        private void ThrowInvalidDisposeDuringActiveTransactions(List<ActiveTransaction> activeTxs)
        {
            throw new TimeoutException(
                $"Could not dispose the environment {Options.BasePath} after {Options.DisposeWaitTime} because there are running transaction.{Environment.NewLine}" +
                $"Either you have long running transactions or hung transactions. Can\'t dispose the environment because that would invalid memory regions{Environment.NewLine}" +
                $"that those transactions are still looking at.{Environment.NewLine}" +
                $"There are {activeTxs.Count:#,#0} transactions ({string.Join(", ", activeTxs)})"
            );
        }

        public Transaction ReadTransaction(TransactionPersistentContext transactionPersistentContext, ByteStringContext context = null)
        {
            return new Transaction(NewLowLevelTransaction(transactionPersistentContext, TransactionFlags.Read, context));
        }

        public Transaction ReadTransaction(ByteStringContext context = null)
        {
            var transactionPersistentContext = new TransactionPersistentContext();
            var newLowLevelTransaction = NewLowLevelTransaction(transactionPersistentContext, TransactionFlags.Read, context);
            return new Transaction(newLowLevelTransaction);
        }
        public Transaction CloneReadTransaction(Transaction previous, TransactionPersistentContext transactionPersistentContext = null, ByteStringContext context = null)
        {
            if (previous.IsWriteTransaction)
                throw new ArgumentException("Only read transactions can be cloned");

            _cancellationTokenSource.Token.ThrowIfCancellationRequested();
            transactionPersistentContext ??= new TransactionPersistentContext();

            try
            {
                IncrementUsageOnNewTransaction();

                LowLevelTransaction tx;

                _txCreation.EnterReadLock();
                try
                {
                    _cancellationTokenSource.Token.ThrowIfCancellationRequested();

                    tx = new LowLevelTransaction(previous.LowLevelTransaction, transactionPersistentContext, context);

                    ActiveTransactions.Add(tx);
                }
                finally
                {
                    _txCreation.ExitReadLock();
                }

                var state = _dataPager.PagerState;
                tx.EnsurePagerStateReference(ref state);

                return new Transaction(tx);
            }
            catch (Exception)
            {
                DecrementUsageOnTransactionCreationFailure();
                throw;
            }
        }

        public Transaction WriteTransaction(TransactionPersistentContext transactionPersistentContext, ByteStringContext context = null, TimeSpan? timeout = null)
        {
            var writeTransaction = new Transaction(NewLowLevelTransaction(transactionPersistentContext, TransactionFlags.ReadWrite, context, timeout));
            return writeTransaction;
        }

        public Transaction WriteTransaction(ByteStringContext context = null, TimeSpan? timeout = null)
        {
            var transactionPersistentContext = new TransactionPersistentContext();
            var newLowLevelTransaction = NewLowLevelTransaction(transactionPersistentContext, TransactionFlags.ReadWrite, context, timeout);
            var writeTransaction = new Transaction(newLowLevelTransaction);
            return writeTransaction;
        }

        internal LowLevelTransaction NewLowLevelTransaction(TransactionPersistentContext transactionPersistentContext, TransactionFlags flags, ByteStringContext context = null, TimeSpan? timeout = null)
        {
            _cancellationTokenSource.Token.ThrowIfCancellationRequested();

            bool txLockTaken = false;
            bool flushInProgressReadLockTaken = false;
            try
            {
                IncrementUsageOnNewTransaction();

                if (flags == TransactionFlags.ReadWrite)
                {
                    var wait = timeout ?? (Debugger.IsAttached ? TimeSpan.FromMinutes(30) : TimeSpan.FromSeconds(30));

                    if (FlushInProgressLock.IsWriteLockHeld == false)
                    {
                        flushInProgressReadLockTaken = FlushInProgressLock.TryEnterReadLock(wait);
                        if (flushInProgressReadLockTaken == false)
                        {
                            GlobalFlushingBehavior.GlobalFlusher.Value.MaybeFlushEnvironment(this);
                            ThrowOnTimeoutWaitingForReadFlushingInProgressLock(wait);
                        }
                    }

                    ThrowOnWriteTransactionOpenedByTheSameThread();

                    txLockTaken = _transactionWriter.Wait(wait);

                    if (txLockTaken == false)
                    {
                        GlobalFlushingBehavior.GlobalFlusher.Value.MaybeFlushEnvironment(this);
                        ThrowOnTimeoutWaitingForWriteTxLock(wait);
                    }

                    _cancellationTokenSource.Token.ThrowIfCancellationRequested();

                    _currentWriteTransactionHolder = NativeMemory.CurrentThreadStats;
                    WriteTransactionStarted();

                    if (_endOfDiskSpace != null)
                    {
                        _endOfDiskSpace.AssertCanContinueWriting();

                        _endOfDiskSpace = null;
                        GlobalFlushingBehavior.GlobalFlusher.Value.MaybeFlushEnvironment(this);
                    }

                    if (Options.ManualFlushing == false && _idleFlushTimer.IsCompleted)
                    {
                        _idleFlushTimer = Task.Run(IdleFlushTimer); // on storage environment creation or if the task has failed
                    }
                }

                LowLevelTransaction tx;

                _txCreation.EnterReadLock();
                try
                {
                    _cancellationTokenSource.Token.ThrowIfCancellationRequested();

                    long txId = flags == TransactionFlags.ReadWrite ? NextWriteTransactionId : CurrentReadTransactionId;
                    tx = new LowLevelTransaction(this, txId, transactionPersistentContext, flags, _freeSpaceHandling,
                        context)
                    {
                        FlushInProgressLockTaken = flushInProgressReadLockTaken,
                    };

                    if (flags == TransactionFlags.ReadWrite)
                    {
                        tx.CurrentTransactionHolder = _currentWriteTransactionHolder;
                        tx.AfterCommitWhenNewReadTransactionsPrevented += AfterCommitWhenNewReadTransactionsPrevented;
                    }

                    ActiveTransactions.Add(tx);

                    NewTransactionCreated?.Invoke(tx);
                }
                finally
                {
                    _txCreation.ExitReadLock();
                }

                var state = _dataPager.PagerState;
                tx.EnsurePagerStateReference(ref state);

                return tx;
            }
            catch (Exception)
            {
                try
                {
                    if (txLockTaken)
                    {
                        _currentWriteTransactionHolder = null;
                        _transactionWriter.Release();
                    }
                    if (flushInProgressReadLockTaken)
                    {
                        FlushInProgressLock.ExitReadLock();
                    }
                }
                finally
                {
                    DecrementUsageOnTransactionCreationFailure();
                }
                throw;
            }
        }

        [Conditional("DEBUG")]
        private void ThrowOnWriteTransactionOpenedByTheSameThread()
        {
            var currentWriteTransactionHolder = _currentWriteTransactionHolder;
            if (currentWriteTransactionHolder != null &&
                currentWriteTransactionHolder == NativeMemory.CurrentThreadStats)
            {
                throw new InvalidOperationException($"A write transaction is already opened by thread name: " +
                                                    $"{currentWriteTransactionHolder.Name}, Id: {currentWriteTransactionHolder.ManagedThreadId}");
            }
        }

        internal void IncrementUsageOnNewTransaction()
        {
            if (_envDispose.TryAddCount(1) == false)
                ThrowCurrentlyDisposing();
        }

        internal void DecrementUsageOnTransactionCreationFailure()
        {
            if (_envDispose.IsSet == false)
                _envDispose.Signal();
        }

        private void ThrowCurrentlyDisposing()
        {
            throw new ObjectDisposedException("The environment " + Options.BasePath + " is currently being disposed");
        }

        internal void WriteTransactionStarted()
        {
            _writeTransactionRunning.Set();
        }

        private void ThrowOnTimeoutWaitingForWriteTxLock(TimeSpan wait)
        {
            if (wait == TimeSpan.Zero)// avoid allocating any strings in common case of just trying
                throw new TimeoutException("Tried and failed to get the tx lock with no timeout, someone else is holding the lock, will retry later...");

            var copy = _currentWriteTransactionHolder;
            if (copy == NativeMemory.CurrentThreadStats)
            {
                throw new InvalidOperationException("A write transaction is already opened by this thread");
            }


            var message = $"Waited for {wait} for transaction write lock, but could not get it";
            if (copy != null)
                message += $", the tx is currently owned by thread {copy.ManagedThreadId} - {copy.Name}, OS thread id: {copy.UnmanagedThreadId}";

            throw new TimeoutException(message);
        }

        private void ThrowOnTimeoutWaitingForReadFlushingInProgressLock(TimeSpan wait)
        {
            var copy = Journal.CurrentFlushingInProgressHolder;
            if (copy == NativeMemory.CurrentThreadStats)
            {
                throw new InvalidOperationException("Flushing is already being performed by this thread");
            }

            var message = $"Waited for {wait} for read access of the flushing in progress lock, but could not get it";
            if (copy != null)
                message += $", the flushing in progress lock is currently owned by thread {copy.ManagedThreadId} - {copy.Name}";

            throw new TimeoutException(message);
        }

        public long CurrentReadTransactionId => Interlocked.Read(ref _transactionsCounter);
        public long NextWriteTransactionId => Interlocked.Read(ref _transactionsCounter) + 1;
        public CancellationToken Token => _cancellationTokenSource.Token;

        public long PossibleOldestReadTransaction(LowLevelTransaction tx)
        {
            if (tx?.LocalPossibleOldestReadTransaction != null)
                return tx.LocalPossibleOldestReadTransaction.Value;

            var oldestActive = ActiveTransactions.OldestTransaction;

            var result = oldestActive == 0 ? CurrentReadTransactionId : Math.Min(CurrentReadTransactionId, oldestActive);
            if (tx != null)
                tx.LocalPossibleOldestReadTransaction = result;
            return result;
        }

        internal ExitWriteLock PreventNewTransactions()
        {
            _txCreation.EnterWriteLock();
            return new ExitWriteLock(_txCreation);
        }

        internal bool IsInPreventNewTransactionsMode => _txCreation.IsWriteLockHeld;

        internal bool TryPreventNewReadTransactions(TimeSpan timeout, out IDisposable exitWriteLock)
        {
            if (_txCreation.TryEnterWriteLock(timeout))
            {
                exitWriteLock = new ExitWriteLock(_txCreation);
                return true;
            }

            exitWriteLock = null;
            return false;
        }

        public struct ExitWriteLock : IDisposable
        {
            readonly ReaderWriterLockSlim _rwls;

            public ExitWriteLock(ReaderWriterLockSlim rwls)
            {
                _rwls = rwls;
            }

            public void Dispose()
            {
                _rwls.ExitWriteLock();
            }
        }

        public event Action<LowLevelTransaction> NewTransactionCreated;
        public event Action<LowLevelTransaction> AfterCommitWhenNewReadTransactionsPrevented;
        internal void TransactionAfterCommit(LowLevelTransaction tx)
        {
            if (ActiveTransactions.Contains(tx) == false)
                return;

            using (PreventNewTransactions())
            {
                Journal.Applicator.OnTransactionCommitted(tx);
                ScratchBufferPool.UpdateCacheForPagerStatesOfAllScratches();
                Journal.UpdateCacheForJournalSnapshots();

                tx.OnAfterCommitWhenNewReadTransactionsPrevented();

                if (tx.Committed && tx.FlushedToJournal)
                    Interlocked.Exchange(ref _transactionsCounter, tx.Id);

                State = tx.State;
            }
        }

        internal void TransactionCompleted(LowLevelTransaction tx)
        {
            if (ActiveTransactions.TryRemove(tx) == false)
                return;
            try
            {
                if (tx.Flags != (TransactionFlags.ReadWrite))
                    return;

                if (tx.FlushedToJournal)
                {
                    var totalPages = 0;
                    // ReSharper disable once LoopCanBeConvertedToQuery
                    foreach (var page in tx.GetTransactionPages())
                    {
                        totalPages += page.NumberOfPages;
                    }

                    Interlocked.Add(ref Journal.Applicator.TotalCommittedSinceLastFlushPages, totalPages);

                    if (tx.IsLazyTransaction == false)
                        GlobalFlushingBehavior.GlobalFlusher.Value.MaybeFlushEnvironment(this);
                }

                if (tx.AsyncCommit != null)
                    return;

                _currentWriteTransactionHolder = null;
                _writeTransactionRunning.Reset();
                _transactionWriter.Release();

                if (tx.FlushInProgressLockTaken)
                    FlushInProgressLock.ExitReadLock();
            }
            finally
            {
                if (tx.AlreadyAllowedDisposeWithLazyTransactionRunning == false)
                    _envDispose.Signal();
            }
        }

        public SizeReport GenerateSizeReport(bool includeTempBuffers)
        {
            long journalsSize = 0;
            foreach (var journal in Journal.Files)
            {
                journalsSize += (long)journal.JournalWriter.NumberOfAllocated4Kb * 4 * Constants.Size.Kilobyte;
            }

            long tempBuffers = 0;
            long tempRecyclableJournals = 0;

            if (includeTempBuffers)
            {
                var journalPath = (Options as StorageEnvironmentOptions.DirectoryStorageEnvironmentOptions)?.JournalPath;
                var tempFiles = StorageReportGenerator.GenerateTempBuffersReport(Options.TempPath, journalPath);

                foreach (var file in tempFiles)
                {
                    switch (file.Type)
                    {
                        case TempBufferType.Scratch:
                            tempBuffers += file.AllocatedSpaceInBytes;
                            break;
                        case TempBufferType.RecyclableJournal:
                            tempRecyclableJournals += file.AllocatedSpaceInBytes;
                            break;
                        default:
                            throw new InvalidOperationException($"Unknown temp file type: {file.Type}");
                    }
                }
            }

            var numberOfAllocatedPages = GetNumberOfAllocatedPages();

            return new SizeReport
            {
                DataFileInBytes = StorageReportGenerator.PagesToBytes(numberOfAllocatedPages),
                JournalsInBytes = journalsSize,
                TempBuffersInBytes = tempBuffers,
                TempRecyclableJournalsInBytes = tempRecyclableJournals
            };
        }

        private long GetNumberOfAllocatedPages()
        {
            return Math.Max(_dataPager.NumberOfAllocatedPages, NextPageNumber - 1); // async apply to data file task
        }

        public StorageReport GenerateReport(Transaction tx)
        {
            var numberOfAllocatedPages = GetNumberOfAllocatedPages();
            var numberOfFreePages = _freeSpaceHandling.AllPages(tx.LowLevelTransaction).Count;

            var countOfTrees = 0;
            var countOfTables = 0;
            using (var rootIterator = tx.LowLevelTransaction.RootObjects.Iterate(false))
            {
                if (rootIterator.Seek(Slices.BeforeAllKeys))
                {
                    do
                    {
                        var currentKey = rootIterator.CurrentKey.Clone(tx.Allocator);
                        var type = tx.GetRootObjectType(currentKey);
                        switch (type)
                        {
                            case RootObjectType.VariableSizeTree:
                                countOfTrees++;
                                break;
                            case RootObjectType.EmbeddedFixedSizeTree:
                                break;
                            case RootObjectType.FixedSizeTree:
                                countOfTrees++;
                                break;
                            case RootObjectType.Table:
                                countOfTables++;
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
                CountOfTrees = countOfTrees,
                CountOfTables = countOfTables,
                Journals = Journal.Files.ToList(),
                FlushedJournals = Journal.Applicator.JournalsToDelete,
                TempPath = Options.TempPath,
                JournalPath = (Options as StorageEnvironmentOptions.DirectoryStorageEnvironmentOptions)?.JournalPath
            });
        }

        public unsafe DetailedStorageReport GenerateDetailedReport(Transaction tx, bool includeDetails = false)
        {
            var numberOfAllocatedPages = Math.Max(_dataPager.NumberOfAllocatedPages, NextPageNumber - 1); // async apply to data file task
            var numberOfFreePages = _freeSpaceHandling.AllPages(tx.LowLevelTransaction).Count;

            var totalCryptoBufferSize = GetTotalCryptoBufferSize();

            var trees = new List<Tree>();
            var fixedSizeTrees = new List<FixedSizeTree>();
            var tables = new List<Table>();
            using (var rootIterator = tx.LowLevelTransaction.RootObjects.Iterate(false))
            {
                if (rootIterator.Seek(Slices.BeforeAllKeys))
                {
                    do
                    {
                        var currentKey = rootIterator.CurrentKey.Clone(tx.Allocator);
                        var type = tx.GetRootObjectType(currentKey);
                        switch (type)
                        {
                            case RootObjectType.VariableSizeTree:
                                var tree = tx.ReadTree(currentKey);
                                trees.Add(tree);
                                break;
                            case RootObjectType.EmbeddedFixedSizeTree:
                                break;
                            case RootObjectType.FixedSizeTree:

                                if (SliceComparer.AreEqual(currentKey, NewPageAllocator.AllocationStorage)) // will be counted inside pre allocated buffers report
                                    continue;

                                fixedSizeTrees.Add(tx.FixedTreeFor(currentKey));
                                break;
                            case RootObjectType.Table:
                                var tableTree = tx.ReadTree(currentKey, RootObjectType.Table);
                                var writtenSchemaData = tableTree.DirectRead(TableSchema.SchemasSlice);
                                var writtenSchemaDataSize = tableTree.GetDataSize(TableSchema.SchemasSlice);
                                var tableSchema = TableSchema.ReadFrom(tx.Allocator, writtenSchemaData, writtenSchemaDataSize);

                                var table = tx.OpenTable(tableSchema, currentKey);
                                tables.Add(table);
                                break;
                            default:
                                throw new ArgumentOutOfRangeException();
                        }
                    }
                    while (rootIterator.MoveNext());
                }
            }

            var generator = new StorageReportGenerator(tx.LowLevelTransaction);

            return generator.Generate(new DetailedReportInput
            {
                NumberOfAllocatedPages = numberOfAllocatedPages,
                NumberOfFreePages = numberOfFreePages,
                NextPageNumber = NextPageNumber,
                Journals = Journal.Files.ToList(),
                FlushedJournals = Journal.Applicator.JournalsToDelete,
                LastFlushedTransactionId = Journal.Applicator.LastFlushedTransactionId,
                LastFlushedJournalId = Journal.Applicator.LastFlushedJournalId,
                TotalWrittenButUnsyncedBytes = Journal.Applicator.TotalWrittenButUnsyncedBytes,
                Trees = trees,
                FixedSizeTrees = fixedSizeTrees,
                Tables = tables,
                IncludeDetails = includeDetails,
                ScratchBufferPoolInfo = _scratchBufferPool.InfoForDebug(PossibleOldestReadTransaction(tx.LowLevelTransaction)),
                TempPath = Options.TempPath,
                JournalPath = (Options as StorageEnvironmentOptions.DirectoryStorageEnvironmentOptions)?.JournalPath,
                TotalEncryptionBufferSize = totalCryptoBufferSize,
                InMemoryStorageState = GetInMemoryStorageState(tx.LowLevelTransaction)
            });
        }

        public InMemoryStorageState GetInMemoryStorageState(LowLevelTransaction tx)
        {
            var state = new InMemoryStorageState
            {
                CurrentReadTransactionId = CurrentReadTransactionId,
                NextWriteTransactionId = NextWriteTransactionId,
                PossibleOldestReadTransaction = PossibleOldestReadTransaction(tx),
                ActiveTransactions = ActiveTransactions.AllTransactions,
                FlushState = new InMemoryStorageState.FlushStateDetails
                {
                    LastFlushTime = Journal.Applicator.LastFlushTime,
                    ShouldFlush = Journal.Applicator.ShouldFlush,
                    LastFlushedTransactionId = Journal.Applicator.LastFlushedJournalId,
                    LastFlushedJournalId = Journal.Applicator.LastFlushedJournalId,
                    LastTransactionIdUsedToReleaseScratches = Journal.Applicator.LastTransactionIdUsedToReleaseScratches,
                    JournalsToDelete = Journal.Applicator.JournalsToDelete.Select(x => x.Number).ToList()
                },
                SyncState = new InMemoryStorageState.SyncStateDetails
                {
                    LastSyncTime = Journal.Applicator.LastSyncTime,
                    ShouldSync = Journal.Applicator.ShouldSync,
                    TotalWrittenButUnsyncedBytes = Journal.Applicator.TotalWrittenButUnsyncedBytes
                }
            };

            return state;
        }

        private Size GetTotalCryptoBufferSize()
        {
            var sum = Size.Zero;
            foreach (var transaction in ActiveTransactions.AllTransactionsInstances)
            {
                sum += transaction.AdditionalMemoryUsageSize;
            }

            return sum;
        }

        public EnvironmentStats Stats()
        {
            var transactionPersistentContext = new TransactionPersistentContext();
            using (var tx = NewLowLevelTransaction(transactionPersistentContext, TransactionFlags.Read))
            {
                var numberOfAllocatedPages = Math.Max(_dataPager.NumberOfAllocatedPages, State.NextPageNumber - 1); // async apply to data file task

                return new EnvironmentStats
                {
                    FreePagesOverhead = FreeSpaceHandling.GetFreePagesOverhead(tx),
                    RootPages = tx.RootObjects.State.PageCount,
                    UnallocatedPagesAtEndOfFile = _dataPager.NumberOfAllocatedPages - NextPageNumber,
                    UsedDataFileSizeInBytes = (State.NextPageNumber - 1) * Constants.Storage.PageSize,
                    AllocatedDataFileSizeInBytes = numberOfAllocatedPages * Constants.Storage.PageSize,
                    NextWriteTransactionId = NextWriteTransactionId,
                };
            }
        }

        internal void BackgroundFlushWritesToDataFile()
        {
            try
            {
                _journal.Applicator.ApplyLogsToDataFile(_cancellationTokenSource.Token,
                    // we intentionally don't wait, if the flush lock is held, something else is flushing, so we don't need
                    // to hold the thread
                    TimeSpan.Zero);
            }
            catch (TimeoutException)
            {
                // we can ignore this, we'll try next time
            }
            catch (OperationCanceledException)
            {
                // db is shutting down
            }
            catch (SEHException sehException)
            {
                VoronUnrecoverableErrorException.Raise(this, "Error occurred during flushing journals to the data file",
                    new Win32Exception(sehException.HResult));
            }
            catch (Exception e)
            {
                VoronUnrecoverableErrorException.Raise(this, "Error occurred during flushing journals to the data file", e);
            }
        }

        public void FlushLogToDataFile()
        {
            if (_options.ManualFlushing == false)
                throw new NotSupportedException("Manual flushes are not set in the storage options, cannot manually flush!");

            _journal.Applicator.ApplyLogsToDataFile(_cancellationTokenSource.Token,
                Debugger.IsAttached ? TimeSpan.FromMinutes(30) : TimeSpan.FromSeconds(30));
        }

        internal void HandleDataDiskFullException(DiskFullException exception)
        {
            if (_options.ManualFlushing)
                return;

            _endOfDiskSpace = new EndOfDiskSpaceEvent(exception.DirectoryPath, exception.CurrentFreeSpace, ExceptionDispatchInfo.Capture(exception));
        }

        public unsafe void ValidatePageChecksum(long pageNumber, PageHeader* current)
        {
            long old;
            var index = pageNumber / (8 * sizeof(long));
            var bitIndex = (int)(pageNumber % (8 * sizeof(long)));
            var bitToSet = 1L << bitIndex;

            // If the page is beyond the initial size of the file we don't validate it. 
            // We assume that it is valid since we wrote it in this run.
            if (index >= _validPages.Length)
                return;

            old = _validPages[index];
            if ((old & bitToSet) != 0)
                return;

            UnlikelyValidatePage(pageNumber, current, index, old, bitToSet);
        }

        private unsafe void UnlikelyValidatePage(long pageNumber, PageHeader* current, long index, long old, long bitToSet)
        {
            var spinner = new SpinWait();
            while (true)
            {
                long modified = Interlocked.CompareExchange(ref _validPages[index], old | bitToSet, old);
                if (modified == old || (modified & bitToSet) != 0)
                    break;

                old = modified;
                spinner.SpinOnce();
            }

            // No need to call EnsureMapped here. ValidatePageChecksum is only called for pages in the datafile, 
            // which we already got using AcquirePagePointerWithOverflowHandling()

            if (pageNumber != current->PageNumber)
                ThrowInvalidPageNumber(pageNumber, current);

            ulong checksum = CalculatePageChecksum((byte*)current, current->PageNumber, current->Flags, current->OverflowSize);

            if (checksum == current->Checksum)
                return;

            ThrowInvalidChecksum(pageNumber, current, checksum);
        }

        private static unsafe void ThrowInvalidPageNumber(long pageNumber, PageHeader* current)
        {
            var message = $"When reading page {pageNumber}, we read a page with header of page {current->PageNumber}. ";

            message += $"Page flags: {current->Flags}. ";

            if ((current->Flags & PageFlags.Overflow) == PageFlags.Overflow)
                message += $"Overflow size: {current->OverflowSize}. ";

            throw new InvalidDataException(message);
        }

        private unsafe void ThrowInvalidChecksum(long pageNumber, PageHeader* current, ulong checksum)
        {
            var message = $"Invalid checksum for page {pageNumber}, data file {_options.DataPager} might be corrupted, expected hash to be {current->Checksum} but was {checksum}. ";

            message += $"Page flags: {current->Flags}. ";

            if ((current->Flags & PageFlags.Overflow) == PageFlags.Overflow)
                message += $"Overflow size: {current->OverflowSize}. ";

            throw new InvalidDataException(message);
        }

        public static unsafe ulong CalculatePageChecksum(byte* ptr, long pageNumber, out ulong expectedChecksum)
        {
            var header = (PageHeader*)(ptr);
            expectedChecksum = header->Checksum;
            return CalculatePageChecksum(ptr, pageNumber, header->Flags, header->OverflowSize);
        }

        public static unsafe ulong CalculatePageChecksum(byte* ptr, long pageNumber, PageFlags flags, int overflowSize)
        {
            var dataLength = Constants.Storage.PageSize - (PageHeader.ChecksumOffset + sizeof(ulong));
            if ((flags & PageFlags.Overflow) == PageFlags.Overflow)
                dataLength = overflowSize - (PageHeader.ChecksumOffset + sizeof(ulong));

            var ctx = new Hashing.Streamed.XXHash64Context
            {
                Seed = (ulong)pageNumber
            };
            Hashing.Streamed.XXHash64.BeginProcess(ref ctx);

            Hashing.Streamed.XXHash64.Process(ref ctx, ptr, PageHeader.ChecksumOffset);
            Hashing.Streamed.XXHash64.Process(ref ctx, ptr + PageHeader.ChecksumOffset + sizeof(ulong), dataLength);

            return Hashing.Streamed.XXHash64.EndProcess(ref ctx);
        }

        public IDisposable GetTemporaryPage(LowLevelTransaction tx, out TemporaryPage tmp)
        {
            if (tx.Flags != TransactionFlags.ReadWrite)
                throw new ArgumentException("Temporary pages are only available for write transactions");

            if (_tempPagesPool.TryDequeue(out tmp) == false)
            {
                tmp = new TemporaryPage(Options);
                try
                {
                    tmp.ReturnTemporaryPageToPool = new ReturnTemporaryPageToPool(this, tmp);
                }
                catch (Exception)
                {
                    tmp.Dispose();
                    throw;
                }
            }

            tmp.PinMemory();

            return tmp.ReturnTemporaryPageToPool;
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

            public unsafe void Dispose()
            {
                try
                {
                    if (_env.Options.Encryption.IsEnabled)
                        Sodium.sodium_memzero(_tmp.TempPagePointer, (UIntPtr)_tmp.PageSize);
                    
                    _tmp.UnpinMemory();
                    _env._tempPagesPool.Enqueue(_tmp);
                }
                catch (Exception)
                {
                    _tmp.Dispose();
                    throw;
                }
            }
        }

        public TransactionsModeResult SetTransactionMode(TransactionsMode mode, TimeSpan duration)
        {
            var transactionPersistentContext = new TransactionPersistentContext();
            using (var tx = NewLowLevelTransaction(transactionPersistentContext, TransactionFlags.ReadWrite))
            {
                var oldMode = Options.TransactionsMode;

                if (_log.IsOperationsEnabled)
                    _log.Operations($"Setting transaction mode to {mode}. Old mode is {oldMode}");

                if (oldMode == mode)
                    return TransactionsModeResult.ModeAlreadySet;

                Options.TransactionsMode = mode;
                if (duration == TimeSpan.FromMinutes(0)) // infinite
                    Options.NonSafeTransactionExpiration = null;
                else
                    Options.NonSafeTransactionExpiration = DateTime.Now + duration;

                if (oldMode == TransactionsMode.Lazy)
                {

                    tx.IsLazyTransaction = false;
                    // we only commit here, the rest of the of the options are without
                    // commit and we use the tx lock
                    tx.Commit();
                }

                if (oldMode == TransactionsMode.Danger)
                {
                    Journal.TruncateJournal();
                    _dataPager.Sync(Journal.Applicator.TotalWrittenButUnsyncedBytes);
                }

                if (mode == TransactionsMode.Danger)
                    Journal.TruncateJournal();

                if (mode != TransactionsMode.Lazy &&
                    mode != TransactionsMode.Safe &&
                    mode != TransactionsMode.Danger)
                {
                    throw new InvalidOperationException("Query string value 'mode' is not a valid mode: " + mode);
                }


                return TransactionsModeResult.SetModeSuccessfully;
            }
        }

        public void Cleanup(bool tryCleanupRecycledJournals = false)
        {
            CleanupMappedMemory();

            if (tryCleanupRecycledJournals)
                Options.TryCleanupRecycledJournals();
        }

        public void CleanupMappedMemory()
        {
            Journal.TryReduceSizeOfCompressionBufferIfNeeded();
            ScratchBufferPool.Cleanup();
            DecompressionBuffers.Cleanup();

            while (_tempPagesPool.TryDequeue(out var tempPage))
            {
                tempPage.Dispose();
            }
        }

        public override string ToString()
        {
            return Options.ToString();
        }

        public void LogsApplied()
        {
            OnLogsApplied?.Invoke();
        }

        public void ResetLastWorkTime()
        {
            LastWorkTime = DateTime.MinValue;
        }

        internal void AllowDisposeWithLazyTransactionRunning(LowLevelTransaction tx)
        {
            Debug.Assert(tx.Flags == TransactionFlags.Read);
            _envDispose.Signal();
            tx.AlreadyAllowedDisposeWithLazyTransactionRunning = true;
        }

        private static void ThrowSimulateFailureOnDbCreation()
        {
            throw new InvalidOperationException("Simulation of db creation failure");
        }
    }
    
    public class StorageEnvironmentWithType
    {
        public string Name { get; set; }
        public StorageEnvironmentType Type { get; set; }
        public StorageEnvironment Environment { get; set; }
        public DateTime? LastIndexQueryTime;

        public StorageEnvironmentWithType(string name, StorageEnvironmentType type, StorageEnvironment environment)
        {
            Name = name;
            Type = type;
            Environment = environment;
        }

        public enum StorageEnvironmentType
        {
            Documents,
            Index,
            Configuration,
            System
        }
    }
}

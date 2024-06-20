using Sparrow;
using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
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
using Voron.Data.CompactTrees;
using Voron.Data.Compression;
using Voron.Data.Fixed;
using Voron.Data.Lookups;
using Voron.Data.PostingLists;
using Voron.Data.RawData;
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
using NativeMemory = Sparrow.Utils.NativeMemory;

namespace Voron
{
    public delegate bool UpgraderDelegate(SchemaUpgradeTransactions transactions, int currentVersion, out int versionAfterUpgrade);

    public sealed class StorageEnvironment : IDisposable
    {
        internal sealed class IndirectReference
        {
            public StorageEnvironment Owner;

            public WeakReference<StorageEnvironment> WeekReference { get; set; }
        }

        internal Table.CompressionDictionariesHolder CompressionDictionariesHolder = new Table.CompressionDictionariesHolder();

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
        private static readonly ByteStringContext _staticContext = new ByteStringContext(SharedMultipleUseFlag.None, ByteStringContext.MinBlockSizeInBytes);

        public static IDisposable GetStaticContext(out ByteStringContext ctx)
        {
            Monitor.Enter(_staticContext);

            ctx = _staticContext;

            return new DisposableAction(() => Monitor.Exit(_staticContext));
        }

        private readonly StorageEnvironmentOptions _options;

        public readonly ActiveTransactions ActiveTransactions = new ActiveTransactions();

        private readonly Pager2 _dataPager;

        public Pager2 DataPager => _dataPager;

        internal readonly LowLevelTransaction.WriteTransactionPool WriteTransactionPool =
            new LowLevelTransaction.WriteTransactionPool();

        private readonly WriteAheadJournal _journal;
        internal readonly SemaphoreSlim _transactionWriter = new SemaphoreSlim(1, 1);
        internal NativeMemory.ThreadStats _currentWriteTransactionHolder;
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

        public bool Disposed;
        private readonly Logger _log;
        public static int MaxConcurrentFlushes = 10; // RavenDB-5221
        public int TimeToSyncAfterFlushInSec;

        public Guid DbId { get; set; }

        public StorageEnvironmentState State { get; private set; }

        public event Action OnLogsApplied;

        internal readonly long[] _validPagesAfterLoad;
        internal readonly long _lastValidPageAfterLoad;

        public bool IsNew { get; }

        public StorageEnvironment(StorageEnvironmentOptions options)
        {
            try
            {
                SelfReference.Owner = this;
                SelfReference.WeekReference = new WeakReference<StorageEnvironment>(this);
                _log = LoggingSource.Instance.GetLogger<StorageEnvironment>(options.BasePath.FullPath);
                _options = options;
                (_dataPager, var dataPagerState) = options.InitializeDataPager();
                _freeSpaceHandling = new FreeSpaceHandling();
                _headerAccessor = new HeaderAccessor(this);
                TimeToSyncAfterFlushInSec = options.TimeToSyncAfterFlushInSec;

                _currentStateRecordRecord = new EnvironmentStateRecord(dataPagerState);
                _lastValidPageAfterLoad = dataPagerState.NumberOfAllocatedPages;
                Debug.Assert(_lastValidPageAfterLoad != 0);

                var remainingBits = _lastValidPageAfterLoad % (8 * sizeof(long));

                _validPagesAfterLoad = new long[_lastValidPageAfterLoad / (8 * sizeof(long)) + (remainingBits == 0 ? 0 : 1)];
                _validPagesAfterLoad[^1] |= unchecked(((long)ulong.MaxValue << (int)remainingBits));

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

        ~StorageEnvironment()
        {
            if (_log.IsOperationsEnabled)
                _log.Operations($"Finalizer of storage environment was called. Env: {this}");

            try
            {
                Dispose();
            }
            catch (Exception e)
            {
                if (_log.IsOperationsEnabled)
                    _log.Operations($"Failed to dispose storage environment via finalizer. Env: {this}", e);
            }
        }

        private static async Task IdleFlushTimer(WeakReference<StorageEnvironment> weakRef, CancellationToken token)
        {
            while (token.IsCancellationRequested == false)
            {
                // IMPORTANT: we have two separate await calls here (await IdleFlushTimerInternal() and await TimeoutManager.WaitFor()) to ensure that
                // the reference to storage environment won't be copied to the state-machine produced by await TimeoutManager.WaitFor() call
                // otherwise the reference is hold and that prevents from running the finalizer of the environment

                var result = await IdleFlushTimerInternal(weakRef)
                                            .ConfigureAwait(false);
                switch (result)
                {
                    case true:
                        continue;
                    case false:
                        return;
                    case null:
                        await TimeoutManager.WaitFor(TimeSpan.FromMilliseconds(1000), token)
                                            .ConfigureAwait(false);
                        break;
                }
            }
        }

        private static async Task<bool?> IdleFlushTimerInternal(WeakReference<StorageEnvironment> weakRef)
        {
            if (weakRef.TryGetTarget(out var env) == false || env.Disposed || env.Options.ManualFlushing)
                return false;

            try
            {

                try
                {
                    var result = await env._writeTransactionRunning.WaitAsync(TimeSpan.FromMilliseconds(env.Options.IdleFlushTimeout))
                                                                       .ConfigureAwait(false);     
                    if (result == false)
                    {
                        if (env.Journal.Applicator.ShouldFlush)
                            GlobalFlushingBehavior.GlobalFlusher.Value.MaybeFlushEnvironment(env);
                        else if (env.Journal.Applicator.ShouldSync)
                            env.SuggestSyncDataFile();

                        return true;
                    }
                }
                catch (ObjectDisposedException)
                {
                    return false;
                }
                catch (OperationCanceledException)
                {
                    return false;
                }

                return null;
            }
            catch (Exception e)
            {
                int numberOfFailures = Interlocked.Increment(ref env._idleFlushTimerFailures);

                string message = $"{nameof(IdleFlushTimer)} failed (numberOfFailures: {numberOfFailures}), unable to schedule flush / syncs of data file. Will be restarted on new write transaction";

                if (env._log.IsOperationsEnabled)
                {
                    env._log.Operations(message, e);
                }

                try
                {
                    env._options.InvokeRecoverableFailure(message, e);
                }
                catch
                {
                    // ignored 
                }

                return false;
            }
        }

        public ScratchBufferPool ScratchBufferPool => _scratchBufferPool;

        private unsafe void LoadExistingDatabase()
        {
            var header = stackalloc TransactionHeader[1];

            Options.AddToInitLog?.Invoke("Starting Recovery");
            bool hadIntegrityIssues = _journal.RecoverDatabase(header, Options.AddToInitLog);
            var successString = hadIntegrityIssues ? "(with integrity issues)" : "(successfully)";
            Options.AddToInitLog?.Invoke($"Recovery Ended {successString}");

            if (hadIntegrityIssues)
            {
                var message = _journal.Files.Count == 0 ? "Unrecoverable database" : "Database recovered partially. Some data was lost.";

                _options.InvokeRecoveryError(this, message, null);
            }

            var entry = _headerAccessor.CopyHeader();
            var nextPageNumber = (header->TransactionId == 0 ? entry.LastPageNumber : header->LastPageNumber) + 1;
            State = new StorageEnvironmentState(nextPageNumber);

            Interlocked.Exchange(ref _transactionsCounter, header->TransactionId == 0 ? entry.TransactionId : header->TransactionId);
            var transactionPersistentContext = new TransactionPersistentContext(true);
            using (var tx = NewLowLevelTransaction(transactionPersistentContext, TransactionFlags.ReadWrite))
            using (var writeTx = new Transaction(tx))
            {
                var rootHeader = header->TransactionId == 0 ? entry.Root : header->Root;
                var root = Tree.Open(tx, null, Constants.RootTreeNameSlice, rootHeader);
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
                    Options.OnVersionReadingTransaction?.Invoke(readTx);
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

        [DoesNotReturn]
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
            var result = Base64.ConvertToBase64ArrayUnpadded(Base64Id, (byte*)&databaseGuidId, 0, 16);
            Debug.Assert(result == 22);
            _options.SetEnvironmentId(databaseGuidId);
        }

        public const int Base64IdLength = 22;

        public string Base64Id { get; } = new string(' ', Base64IdLength);

        private void CreateNewDatabase()
        {
            const int initialNextPageNumber = 0;
            State = new StorageEnvironmentState(initialNextPageNumber);

            if (Options.SimulateFailureOnDbCreation)
                ThrowSimulateFailureOnDbCreation();

            var transactionPersistentContext = new TransactionPersistentContext();
            using (var tx = NewLowLevelTransaction(transactionPersistentContext, TransactionFlags.ReadWrite))
            {
                var root = Tree.Create(tx, null, Constants.RootTreeNameSlice);
                
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
                    _options.OwnsPagers ? _dataPager : null,
                })
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

                GC.SuppressFinalize(this);

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

        [DoesNotReturn]
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

                    if (Options.ManualFlushing == false && _idleFlushTimer.IsCompleted) // on storage environment creation or if the task has failed
                    {
                        _idleFlushTimer = Task.Run(() => IdleFlushTimer(SelfReference.WeekReference, Token), Token);
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
                        tx.AfterCommitWhenNewTransactionsPrevented += AfterCommitWhenNewTransactionsPrevented;
                    }
                    else
                    {
                        tx.CurrentTransactionHolder = NativeMemory.CurrentThreadStats;
                    }

                    ActiveTransactions.Add(tx);

                    NewTransactionCreated?.Invoke(tx);
                }
                finally
                {
                    _txCreation.ExitReadLock();
                }

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

        internal void InvokeNewTransactionCreated(LowLevelTransaction tx)
        {
            NewTransactionCreated?.Invoke(tx);
        }

        internal void InvokeAfterCommitWhenNewTransactionsPrevented(LowLevelTransaction tx)
        {
            AfterCommitWhenNewTransactionsPrevented?.Invoke(tx);
        }

#if DEBUG
        public bool CaptureTransactionStackTrace;
#endif

        [Conditional("DEBUG")]
        private void ThrowOnWriteTransactionOpenedByTheSameThread()
        {
            var currentWriteTransactionHolder = _currentWriteTransactionHolder;
            if (currentWriteTransactionHolder != null &&
                currentWriteTransactionHolder == NativeMemory.CurrentThreadStats)
            {
                throw new InvalidOperationException($"A write transaction is already opened by thread name: " +
                                                    $"{currentWriteTransactionHolder.Name}, Id: {currentWriteTransactionHolder.ManagedThreadId}{Environment.NewLine}" +
                                                    $"{currentWriteTransactionHolder.CapturedStackTrace}");
            }
#if DEBUG
            if (currentWriteTransactionHolder != null && CaptureTransactionStackTrace)
            {
                currentWriteTransactionHolder.CapturedStackTrace = Environment.StackTrace;
            }
#endif
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

        [DoesNotReturn]
        private void ThrowCurrentlyDisposing()
        {
            throw new ObjectDisposedException("The environment " + Options.BasePath + " is currently being disposed");
        }

        [DoesNotReturn]
        private void ThrowCommittedAndFlushedTransactionNotFoundInActiveOnes(LowLevelTransaction llt)
        {
            throw new InvalidOperationException($"The transaction with ID '{llt.Id}' got committed and flushed but it wasn't found in the {nameof(ActiveTransactions)}. (Debug details: tx id of {nameof(llt.ActiveTransactionNode)} - {llt.ActiveTransactionNode?.Value?.Id}");
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

        [DoesNotReturn]
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
            if (_txCreation.IsWriteLockHeld)
                return default;

            _txCreation.EnterWriteLock();
            return new ExitWriteLock(_txCreation);
        }

        internal bool IsInPreventNewTransactionsMode => _txCreation.IsWriteLockHeld;

        internal bool TryPreventNewTransactions(TimeSpan timeout, out IDisposable exitWriteLock)
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
            private readonly ReaderWriterLockSlim _rwls;

            public ExitWriteLock(ReaderWriterLockSlim rwls)
            {
                _rwls = rwls;
            }

            public void Dispose()
            {
                _rwls?.ExitWriteLock();
            }
        }

        public event Action<LowLevelTransaction> NewTransactionCreated;
        public event Action<LowLevelTransaction> AfterCommitWhenNewTransactionsPrevented;

        internal void TransactionAfterCommit(LowLevelTransaction tx)
        {
            tx._forTestingPurposes?.ActionToCallOnTransactionAfterCommit?.Invoke();

            if (ActiveTransactions.Contains(tx) == false)
            {
                if (tx.Committed && tx.FlushedToJournal)
                    ThrowCommittedAndFlushedTransactionNotFoundInActiveOnes(tx);

                return;
            }

            using (PreventNewTransactions())
            {
                if (tx.Committed && tx.FlushedToJournal)
                    Interlocked.Exchange(ref _transactionsCounter, tx.Id);

                State = tx.State;

                Journal.Applicator.OnTransactionCommitted(tx);
                ScratchBufferPool.UpdateCacheForPagerStatesOfAllScratches();
                Journal.UpdateCacheForJournalSnapshots();

                tx.OnAfterCommitWhenNewTransactionsPrevented();
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

                    GlobalFlushingBehavior.GlobalFlusher.Value.MaybeFlushEnvironment(this);
                }

                if (tx.AsyncCommit != null)
                    return;

                _currentWriteTransactionHolder = null;
                _writeTransactionRunning.Reset();
                _transactionWriter.Release();

                Journal.Applicator.OnTransactionCompleted();

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
            return Math.Max(_currentStateRecordRecord.DataPagerState.NumberOfAllocatedPages, NextPageNumber - 1); // async apply to data file task
        }

        public StorageReport GenerateReport(Transaction tx)
        {
            var numberOfAllocatedPages = GetNumberOfAllocatedPages();
            var numberOfFreePages = _freeSpaceHandling.AllPages(tx.LowLevelTransaction).Count;

            var countOfTrees = 0;
            var countOfTables = 0;
            var countOfSets = 0;
            var countOfContainers = 0;
            var countOfPersistentDictionaries = 0;
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
                            case RootObjectType.FixedSizeTree:
                            case RootObjectType.VariableSizeTree:
                            case RootObjectType.Lookup:
                                countOfTrees++;
                                break;
                            case RootObjectType.EmbeddedFixedSizeTree:
                                break;
                            case RootObjectType.Table:
                                countOfTables++;
                                break;
                            case RootObjectType.Set:
                                countOfSets++;
                                break;
                            case RootObjectType.Container:
                                countOfContainers++;
                                break;
                            case RootObjectType.PersistentDictionary:
                                countOfPersistentDictionaries++;
                                break;
                            default:
                                throw new ArgumentOutOfRangeException(nameof(type), type.ToString());
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
                CountOfContainers = countOfContainers,
                CountOfSets = countOfSets,
                CountOfPersistentDictionaries = countOfPersistentDictionaries,
                Journals = Journal.Files.ToList(),
                FlushedJournals = Journal.Applicator.JournalsToDelete,
                TempPath = Options.TempPath,
                JournalPath = (Options as StorageEnvironmentOptions.DirectoryStorageEnvironmentOptions)?.JournalPath
            });
        }

        public unsafe DetailedStorageReport GenerateDetailedReport(Transaction tx, bool includeDetails = false)
        {
            DetailedReportInput detailedReportInput = CreateDetailedReportInput(tx, includeDetails);

            var generator = new StorageReportGenerator(tx.LowLevelTransaction);
            return generator.Generate(detailedReportInput);
        }

        public unsafe Dictionary<long, string> GetPageOwners(Transaction tx, Func<PostingList, List<long>> onPostingList = null)
        {
            var r = new Dictionary<long, string>();
            RegisterPages(_freeSpaceHandling.AllPages(tx.LowLevelTransaction), "Freed Page");
            for (long pageNumber = NextPageNumber; pageNumber < tx.LowLevelTransaction.DataPagerState.NumberOfAllocatedPages; pageNumber++)
            {
                r[pageNumber] = "Unused Page";
            }

            var globalAllocator = new NewPageAllocator(tx.LowLevelTransaction, tx.LowLevelTransaction.RootObjects);
            RegisterPages(globalAllocator.GetAllocationStorageFst().AllPages(), "Global/PreAllocatedPages/Bitmaps");
            RegisterPages(globalAllocator.AllPages(), "Global/PreAllocatedPages");
            RegisterPages(tx.LowLevelTransaction.RootObjects.AllPages(), "RootObjects");
            using (var rootIterator = tx.LowLevelTransaction.RootObjects.Iterate(false))
            {
                if (rootIterator.Seek(Slices.BeforeAllKeys))
                {
                    do
                    {
                        var currentKey = rootIterator.CurrentKey.Clone(tx.Allocator);
                        var type = tx.GetRootObjectType(currentKey);
                        string name = currentKey.ToString();
                        switch (type)
                        {
                            case RootObjectType.VariableSizeTree:
                                var tree = tx.ReadTree(currentKey);
                                RegisterPages(tree.AllPages(), name + " (VST)");
                                if (tree.State.Header.Flags.HasFlag(TreeFlags.CompactTrees) ||
                                    tree.State.Header.Flags.HasFlag(TreeFlags.Lookups))
                                {
                                    var it = tree.Iterate(false);
                                    if (it.Seek(Slices.BeforeAllKeys))
                                    {
                                        do
                                        {
                                            var rootObjectType = (RootObjectType)it.CreateReaderForCurrent().ReadByte();
                                            switch (rootObjectType)
                                            {
                                                case RootObjectType.Lookup:
                                                    var lookup = tree.LookupFor<Int64LookupKey>(it.CurrentKey);
                                                    RegisterLookup(lookup, name + $"/{it.CurrentKey}");
                                                    break;
                                                case RootObjectType.EmbeddedFixedSizeTree:
                                                    continue; // already accounted for
                                                case RootObjectType.FixedSizeTree:
                                                    var nestedFixedSizeHeader = (FixedSizeTreeHeader.Large*)it.CreateReaderForCurrent().Base;
                                                    var nestedSet = tree.FixedTreeFor(it.CurrentKey, (byte)nestedFixedSizeHeader->ValueSize);
                                                    RegisterPages(nestedSet.AllPages(), name);
                                                    break;
                                                default:
                                                    throw new ArgumentOutOfRangeException(rootObjectType.ToString());

                                            }

                                        } while (it.MoveNext());
                                    }
                                }
                                break;
                            case RootObjectType.EmbeddedFixedSizeTree:
                                break;
                            case RootObjectType.FixedSizeTree:
                                if (SliceComparer.AreEqual(currentKey, NewPageAllocator.AllocationStorage)) // will be counted inside pre allocated buffers report
                                    continue;
                                RegisterPages(tx.FixedTreeFor(currentKey).AllPages(), name);
                                break;
                            case RootObjectType.Table:
                                var tableTree = tx.ReadTree(currentKey, RootObjectType.Table);
                                RegisterPages(tableTree.AllPages(), name);
                                var writtenSchemaData = tableTree.DirectRead(TableSchema.SchemasSlice);
                                var writtenSchemaDataSize = tableTree.GetDataSize(TableSchema.SchemasSlice);
                                var tableSchema = TableSchema.ReadFrom(tx.Allocator, writtenSchemaData, writtenSchemaDataSize);
                                var table = tx.OpenTable(tableSchema, currentKey);
                                RegisterPages(table.TablePageAllocator.GetAllocationStorageFst().AllPages(), name + "/PreAllocatedPages/Bitmaps");
                                RegisterPages(table.TablePageAllocator.AllPages(), name + "/PreAllocatedPages");
                                if (tableSchema.Key is { IsGlobal: false })
                                {
                                    Tree t = GetTableTree(tableTree, tableSchema, tableSchema.Key.Name);
                                    RegisterPages(t.AllPages(), name + "/" + tableSchema.Key.Name);
                                }

                                foreach (var index in tableSchema.FixedSizeIndexes.Values)
                                {
                                    if (index.IsGlobal)
                                        continue;

                                    var fixedSizeTree = new FixedSizeTree(tx.LowLevelTransaction, tableTree, index.Name, sizeof(long));
                                    RegisterPages(fixedSizeTree.AllPages(), name + "/" + index.Name);
                                }

                                foreach (var index in tableSchema.Indexes.Values)
                                {
                                    if (index.IsGlobal)
                                        continue;

                                    Tree t = GetTableTree(tableTree, tableSchema, index.Name);
                                    RegisterPages(t.AllPages(), name + "/" + index.Name);
                                }

                                RegisterTableSection(tableTree, name, TableSchema.ActiveCandidateSectionSlice);
                                RegisterTableSection(tableTree, name, TableSchema.InactiveSectionSlice);
                                var readResult = tableTree.Read(TableSchema.ActiveSectionSlice);
                                long pageNumber = readResult.Reader.ReadLittleEndianInt64();
                                var activeDataSmallSection = new ActiveRawDataSmallSection(tx, pageNumber);
                                // off by one here because of the section header
                                r.Add(activeDataSmallSection.PageNumber, name + "/" + TableSchema.ActiveSectionSlice + "/header");
                                for (long page = 0; page < activeDataSmallSection.NumberOfPages; page++)
                                {
                                    r.Add(activeDataSmallSection.PageNumber + page + 1, name + "/" + TableSchema.ActiveSectionSlice + "/page");
                                }
                                break;
                            case RootObjectType.Container:
                                long container = tx.OpenContainer(currentKey);
                                RegisterContainer(container, name);
                                break;
                            case RootObjectType.Set:
                                var set = tx.OpenPostingList(currentKey);
                                RegisterPages(set.AllPages(), name);
                                var nestedPages = onPostingList?.Invoke(set);
                                if (nestedPages != null)
                                {
                                    RegisterPages(nestedPages, name);
                                }
                                break;
                            case RootObjectType.Lookup:
                                // Here is may be int64, double or compact key, we aren't sure
                                var numeric = tx.LookupFor<Int64LookupKey>(currentKey);
                                RegisterLookup(numeric, name);
                                break;
                            case RootObjectType.PersistentDictionary:
                                var header = *(PersistentDictionaryRootHeader*)rootIterator.CreateReaderForCurrent().Base;
                                Page dicPage = tx.LowLevelTransaction.GetPage(header.PageNumber);
                                var pages = Pager.GetNumberOfOverflowPages(dicPage.OverflowSize);
                                for (long i = 0; i < pages; i++)
                                {
                                    r.Add(header.PageNumber + i, name);
                                }
                                break;
                            default:
                                throw new ArgumentOutOfRangeException(nameof(type), type.ToString());
                        }
                    } while (rootIterator.MoveNext());
                }
            }

            return r;

            void RegisterPages(List<long> allPages, string name)
            {
                foreach (long page in allPages)
                {
                    r.Add(page, name);
                }
            }

            Tree GetTableTree(Tree tableTree, TableSchema tableSchema, Slice treeName)
            {
                var treeHeader = (TreeRootHeader*)tableTree.DirectRead(treeName);
                var t = Tree.Open(tx.LowLevelTransaction, tx, treeName, *treeHeader);
                return t;
            }

            void RegisterTableSection(Tree tableTree, string name, Slice sectionName)
            {
                var fixedSizeTree = new FixedSizeTree(tx.LowLevelTransaction, tableTree, sectionName, 0);
                RegisterPages(fixedSizeTree.AllPages(), name + "/" + sectionName);
                using var it = fixedSizeTree.Iterate();
                if (it.Seek(long.MinValue))
                {
                    do
                    {
                        var section = new RawDataSection(tx.LowLevelTransaction, it.CurrentKey);
                        r.Add(section.PageNumber, name + "/" + TableSchema.ActiveSectionSlice + "/header");
                        for (long page = 0; page < section.NumberOfPages; page++)
                        {
                            r.Add(section.PageNumber + page + 1, name + "/" + TableSchema.ActiveSectionSlice + "/page");
                        }
                    } while (it.MoveNext());
                }
            }

            void RegisterContainer(long container, string name)
            {
                r.Add(container, name);
                var overflowName = $"{name}/OverflowPage";
                var (allPages, freePages) = Voron.Data.Containers.Container.GetPagesFor(tx.LowLevelTransaction, container);
                RegisterPages(allPages.AllPages(), name + "/AllPagesSet");
                RegisterPages(freePages.AllPages(), name + "/FreePagesSet");
                var iterator = Voron.Data.Containers.Container.GetAllPagesIterator(tx.LowLevelTransaction, container);
                while (iterator.MoveNext(out var page))
                {
                    var pageObject = tx.LowLevelTransaction.GetPage(page);
                    r.Add(page, name);
                    if (pageObject.IsOverflow == false)
                        continue;


                    var numberOfOverflowPages = Pager.GetNumberOfOverflowPages(pageObject.OverflowSize);
                    for (int overflowPage = 1; overflowPage < numberOfOverflowPages; ++overflowPage)
                        r.Add(page + overflowPage, overflowName);
                }
            }

            void RegisterLookup(Lookup<Int64LookupKey> numeric, string name)
            {
                RegisterPages(numeric.AllPages(), name);
                if (numeric.State.TermsContainerId > 0)
                {
                    RegisterContainer(numeric.State.TermsContainerId, name + "/TermsContainer");
                }
            }
        }

        public unsafe DetailedReportInput CreateDetailedReportInput(Transaction tx, bool includeDetails)
        {
            var numberOfAllocatedPages = Math.Max(tx.LowLevelTransaction.DataPagerState.NumberOfAllocatedPages, NextPageNumber - 1); // async apply to data file task
            var numberOfFreePages = _freeSpaceHandling.AllPages(tx.LowLevelTransaction).Count;

            var totalCryptoBufferSize = GetTotalCryptoBufferSize();

            var detailedReportInput = new DetailedReportInput
            {
                NumberOfAllocatedPages = numberOfAllocatedPages,
                NumberOfFreePages = numberOfFreePages,
                NextPageNumber = NextPageNumber,
                Journals = Journal.Files.ToList(),
                FlushedJournals = Journal.Applicator.JournalsToDelete,
                LastFlushedTransactionId = Journal.Applicator.LastFlushedTransactionId,
                LastFlushedJournalId = Journal.Applicator.LastFlushedJournalId,
                TotalWrittenButUnsyncedBytes = Journal.Applicator.TotalWrittenButUnsyncedBytes,
                Trees = new(),
                FixedSizeTrees = new(),
                Tables = new(),
                Containers = new(),
                PostingLists = new(),
                PersistentDictionaries = new(),
                NumericLookups = new(),
                TextualLookups = new(),
                IncludeDetails = includeDetails,
                TempPath = Options.TempPath,
                JournalPath = (Options as StorageEnvironmentOptions.DirectoryStorageEnvironmentOptions)?.JournalPath,
                TotalEncryptionBufferSize = totalCryptoBufferSize,
                InMemoryStorageState = GetInMemoryStorageState(tx.LowLevelTransaction)
            };

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
                                detailedReportInput.Trees.Add(tree);
                                break;
                            case RootObjectType.EmbeddedFixedSizeTree:
                                break;
                            case RootObjectType.FixedSizeTree:
                                if (SliceComparer.AreEqual(currentKey, NewPageAllocator.AllocationStorage)) // will be counted inside pre allocated buffers report
                                    continue;

                                detailedReportInput.FixedSizeTrees.Add(tx.FixedTreeFor(currentKey));
                                break;
                            case RootObjectType.Table:
                                var tableTree = tx.ReadTree(currentKey, RootObjectType.Table);
                                var writtenSchemaData = tableTree.DirectRead(TableSchema.SchemasSlice);
                                var writtenSchemaDataSize = tableTree.GetDataSize(TableSchema.SchemasSlice);
                                var tableSchema = TableSchema.ReadFrom(tx.Allocator, writtenSchemaData, writtenSchemaDataSize);

                                var table = tx.OpenTable(tableSchema, currentKey);
                                detailedReportInput.Tables.Add(table);
                                break;
                            case RootObjectType.Container:
                                long container = tx.OpenContainer(currentKey);
                                detailedReportInput.Containers[currentKey] = container;
                                break;
                            case RootObjectType.Set:
                                var set = tx.OpenPostingList(currentKey);
                                detailedReportInput.PostingLists.Add(set);
                                break;
                            case RootObjectType.Lookup:
                                // Here is may be int64, double or compact key, we aren't sure
                                var numeric = tx.LookupFor<Int64LookupKey>(currentKey);
                                if (numeric.State.DictionaryId == -1)
                                {
                                    // we don't care if it is double or long, same size for the report
                                    detailedReportInput.NumericLookups.Add(numeric);
                                }
                                else
                                {
                                    tx.Forget(currentKey);
                                    var txt = tx.LookupFor<CompactTree.CompactKeyLookup>(currentKey);
                                    detailedReportInput.TextualLookups.Add(txt);
                                }
                                break;
                            case RootObjectType.PersistentDictionary:
                                var header = *(PersistentDictionaryRootHeader*)rootIterator.CreateReaderForCurrent().Base;
                                detailedReportInput.PersistentDictionaries.Add(header);
                                break;
                            default:
                                throw new ArgumentOutOfRangeException(nameof(type), type.ToString());
                        }
                    } while (rootIterator.MoveNext());
                }
            }

            return detailedReportInput;
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
                var numberOfAllocatedPages = Math.Max(tx.DataPagerState.NumberOfAllocatedPages, State.NextPageNumber - 1); // async apply to data file task

                return new EnvironmentStats
                {
                    FreePagesOverhead = FreeSpaceHandling.GetFreePagesOverhead(tx),
                    RootPages = tx.RootObjects.State.Header.PageCount,
                    UnallocatedPagesAtEndOfFile = tx.DataPagerState.NumberOfAllocatedPages - NextPageNumber,
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

        public unsafe void ValidateInMemoryPageChecksum(long pageNumber, PageHeader* current)
        {
            // Since we are forcing the validation there is no need to update the _validPages 
            if (pageNumber != current->PageNumber)
                ThrowInvalidPageNumber(pageNumber, current);

            ulong checksum = CalculatePageChecksum((byte*)current, current->PageNumber, current->Flags, current->OverflowSize);

            if (checksum != current->Checksum)
                ThrowInvalidChecksum(pageNumber, current, checksum);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public unsafe void ValidatePageChecksum(long pageNumber, PageHeader* current)
        {
            // If the page is beyond the initial size of the file we don't validate it. 
            // We assume that it is valid since we wrote it in this run.
            if (pageNumber >= _lastValidPageAfterLoad)
                return;

            var index = pageNumber / (8 * sizeof(long));

            long old = _validPagesAfterLoad[index];
            var bitToSet = 1L << (int)(pageNumber % (8 * sizeof(long)));
            if ((old & bitToSet) != 0)
                return;

            UnlikelyValidatePage(pageNumber, current, index, old, bitToSet);
        }

        private unsafe void UnlikelyValidatePage(long pageNumber, PageHeader* current, long index, long old, long bitToSet)
        {
            // No need to call EnsureMapped here. ValidatePageChecksum is only called for pages in the datafile, 
            // which we already got using AcquirePagePointerWithOverflowHandling()
            if (pageNumber != current->PageNumber)
                ThrowInvalidPageNumber(pageNumber, current);

            ulong checksum = CalculatePageChecksum((byte*)current, current->PageNumber, current->Flags, current->OverflowSize);

            if (checksum != current->Checksum)
                ThrowInvalidChecksum(pageNumber, current, checksum);

            while (true)
            {
                // PERF: This code used to have a spin-wait. While it makes sense where threads are competing on tight loops for
                // for resources, the spin-wait here serves no purpose as the thread is going to bail out immediately after completion.
                long modified = Interlocked.CompareExchange(ref _validPagesAfterLoad[index], old | bitToSet, old);
                if (modified == old || (modified & bitToSet) != 0)
                    break;

                old = modified;
            }
        }

        [DoesNotReturn]
        private static unsafe void ThrowInvalidPageNumber(long pageNumber, PageHeader* current)
        {
            var message = $"When reading page {pageNumber}, we read a page with header of page {current->PageNumber}. ";

            message += $"Page flags: {current->Flags}. ";

            if ((current->Flags & PageFlags.Overflow) == PageFlags.Overflow)
                message += $"Overflow size: {current->OverflowSize}. ";

            throw new InvalidDataException(message);
        }

        [DoesNotReturn]
        private unsafe void ThrowInvalidChecksum(long pageNumber, PageHeader* current, ulong checksum)
        {
            var message = $"Invalid checksum for page {pageNumber}, data file {_dataPager?.FileName ?? "unknown"} might be corrupted, expected hash to be {current->Checksum} but was {checksum}. ";

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
            Hashing.Streamed.XXHash64.Begin(ref ctx);

            Hashing.Streamed.XXHash64.Process(ref ctx, ptr, PageHeader.ChecksumOffset);
            Hashing.Streamed.XXHash64.Process(ref ctx, ptr + PageHeader.ChecksumOffset + sizeof(ulong), dataLength);

            return Hashing.Streamed.XXHash64.End(ref ctx);
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

        [DoesNotReturn]
        private static void ThrowSimulateFailureOnDbCreation()
        {
            throw new InvalidOperationException("Simulation of db creation failure");
        }

        internal TestingStuff _forTestingPurposes;
        private EnvironmentStateRecord _currentStateRecordRecord;

        public EnvironmentStateRecord CurrentStateRecord => _currentStateRecordRecord;

        internal TestingStuff ForTestingPurposesOnly()
        {
            if (_forTestingPurposes != null)
                return _forTestingPurposes;

            return _forTestingPurposes = new TestingStuff();
        }

        internal sealed class TestingStuff
        {
            internal Action ActionToCallDuringFullBackupRighAfterCopyHeaders;
        }


        // We create a single thread-safe persistent dictionary locator with enough state to deal with almost any scenario.
        internal PersistentDictionaryLocator DictionaryLocator { get; } = new PersistentDictionaryLocator(1024);

        public PersistentDictionary CreateEncodingDictionary(Page dictionaryPage)
        {
            // Since when a dictionary is created it will never be removed or reclaimed, we can create a cache of dictionaries
            // as soon as we are asked to retrieve them. 
            var dictionary = new PersistentDictionary(dictionaryPage);

            // Since the construction of new dictionaries happen at the end of the commit phase, we can safely
            // add the dictionary to the global shared cache as it is the current one. 
            DictionaryLocator.Set(dictionary.DictionaryId, dictionary);

            return dictionary;
        }

        public void UpdateDataPagerState(Pager2.State dataPagerState)
        {
            dataPagerState.BeforePublishing();
            while (true)
            {
                var currentState = _currentStateRecordRecord!;
                var updatedState = currentState with {DataPagerState = dataPagerState};
                if (Interlocked.CompareExchange(ref _currentStateRecordRecord, updatedState, currentState) == currentState)
                    break;
            }
        }
    }

    public sealed class StorageEnvironmentWithType
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

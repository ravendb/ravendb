using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
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
using Voron.Trees;
using Voron.Util;

namespace Voron
{
    public class StorageEnvironment : IDisposable
    {
        private readonly StorageEnvironmentOptions _options;

        private readonly ConcurrentSet<Transaction> _activeTransactions = new ConcurrentSet<Transaction>();

        private readonly IVirtualPager _dataPager;
        internal readonly SliceComparer _sliceComparer;

        private readonly WriteAheadJournal _journal;
        private readonly SemaphoreSlim _txWriter = new SemaphoreSlim(1);
		private readonly ManualResetEventSlim _flushWriter = new ManualResetEventSlim();

        private readonly ReaderWriterLockSlim _txCommit = new ReaderWriterLockSlim();

        private long _transactionsCounter;
        private readonly IFreeSpaceHandling _freeSpaceHandling;
        private Task _flushingTask;
        private readonly HeaderAccessor _headerAccessor;

        private CancellationTokenSource _cancellationTokenSource = new CancellationTokenSource();
        private ScratchBufferPool _scratchBufferPool;
	    private DebugJournal _debugJournal;
	    private EndOfDiskSpaceEvent _endOfDiskSpace;
	    private int _sizeOfUnflushedTransactionsInJournalFile;


	    public TemporaryPage TemporaryPage { get; private set; }


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

        public unsafe StorageEnvironment(StorageEnvironmentOptions options)
        {
            try
            {
                TemporaryPage = new TemporaryPage();
                _options = options;
                _dataPager = options.DataPager;
                _freeSpaceHandling = new FreeSpaceHandling(this);
                _sliceComparer = NativeMethods.memcmp;
                _headerAccessor = new HeaderAccessor(this);
                var isNew = _headerAccessor.Initialize();

                _scratchBufferPool = new ScratchBufferPool(this);

                _journal = new WriteAheadJournal(this);

                if (isNew)
                    CreateNewDatabase();
                else // existing db, let us load it
                    LoadExistingDatabase();

                State.FreeSpaceRoot.Name = Constants.FreeSpaceTreeName;
                State.Root.Name = Constants.RootTreeName;

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
                NextPageNumber = nextPageNumber
            };

            _transactionsCounter = (header->TransactionId == 0 ? entry.TransactionId : header->TransactionId);

            using (var tx = NewTransaction(TransactionFlags.ReadWrite))
            {
                var root = Tree.Open(tx, _sliceComparer, header->TransactionId == 0 ? &entry.Root : &header->Root);
                var freeSpace = Tree.Open(tx, _sliceComparer, header->TransactionId == 0 ? &entry.FreeSpace : &header->FreeSpace);

                tx.UpdateRootsIfNeeded(root, freeSpace);
                tx.Commit();
            }
        }

        private unsafe void CreateNewDatabase()
        {
            const int initialNextPageNumber = 0;
            State = new StorageEnvironmentState(null, null, initialNextPageNumber);
            using (var tx = NewTransaction(TransactionFlags.ReadWrite))
            {
                var root = Tree.Create(tx, _sliceComparer);
                var freeSpace = Tree.Create(tx, _sliceComparer);

                // important to first create the two trees, then set them on the env
                tx.UpdateRootsIfNeeded(root, freeSpace);

                tx.Commit();
            }
        }

        public IFreeSpaceHandling FreeSpaceHandling
        {
            get { return _freeSpaceHandling; }
        }

        public unsafe SliceComparer SliceComparer
        {
            get { return _sliceComparer; }
        }

        public HeaderAccessor HeaderAccessor
        {
            get { return _headerAccessor; }
        }

        public long OldestTransaction
        {
            get
            {
                return Math.Min(_activeTransactions.OrderBy(x => x.Id).Select(x => x.Id).FirstOrDefault(), _transactionsCounter);
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

	    public void DeleteTree(Transaction tx, string name)
        {
            if (tx.Flags == (TransactionFlags.ReadWrite) == false)
                throw new ArgumentException("Cannot create a new newRootTree with a read only transaction");

	        Tree tree = tx.ReadTree(name);
	        if (tree == null)
	            return;

            foreach (var page in tree.AllPages(tx))
            {
                tx.FreePage(page);
            }

            tx.State.Root.Delete(tx, name);

            tx.RemoveTree(name);
        }

        public unsafe Tree CreateTree(Transaction tx, string name)
        {
            if (tx.Flags == (TransactionFlags.ReadWrite) == false)
                throw new ArgumentException("Cannot create a new tree with a read only transaction");

            Tree tree = tx.ReadTree(name);
            if (tree != null)
                return tree;

            Slice key = name;

            // we are in a write transaction, no need to handle locks
            var header = (TreeRootHeader*)tx.State.Root.DirectRead(tx, key);
            if (header != null)
            {
                tree = Tree.Open(tx, _sliceComparer, header);
                tree.Name = name;
                tx.AddTree(name, tree);
                return tree;
            }

            tree = Tree.Create(tx, _sliceComparer);
            tree.Name = name;
            var space = tx.State.Root.DirectAdd(tx, key, sizeof(TreeRootHeader));

            tree.State.CopyTo((TreeRootHeader*)space);
            tree.State.IsModified = true;
            tx.AddTree(name, tree);

			if(IsDebugRecording)
				DebugJournal.RecordAction(DebugActionType.CreateTree, Slice.Empty,name,Stream.Null);

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
                    _journal, TemporaryPage
                })
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
                    if (_txWriter.Wait(wait) == false)
                    {
                        throw new TimeoutException("Waited for " + wait +
                                                   " for transaction write lock, but could not get it");
                    }
                    txLockTaken = true;

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

                long txId;
                Transaction tx;

                _txCommit.EnterReadLock();
                try
                {
                    txId = flags == TransactionFlags.ReadWrite ? _transactionsCounter + 1 : _transactionsCounter;
                    tx = new Transaction(this, txId, flags, _freeSpaceHandling);
                }
                finally
                {
                    _txCommit.ExitReadLock();
                }

                _activeTransactions.Add(tx);
                var state = _dataPager.TransactionBegan();
                tx.AddPagerState(state);

                if (flags == TransactionFlags.ReadWrite)
                {
                    tx.AfterCommit = TransactionAfterCommit;
                }

                return tx;
            }
            catch (Exception)
            {
                if (txLockTaken)
                    _txWriter.Release();
                throw;
            }
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

            Interlocked.Add(ref _sizeOfUnflushedTransactionsInJournalFile, tx.GetTransactionPages().Count);
			_flushWriter.Set();
        }

        internal void TransactionCompleted(Transaction tx)
        {
            if (_activeTransactions.TryRemove(tx) == false)
                return;

            if (tx.Flags != (TransactionFlags.ReadWrite))
                return;
            
            _txWriter.Release();
        }

        public Dictionary<string, List<long>> AllPages(Transaction tx)
        {
            var results = new Dictionary<string, List<long>>(StringComparer.OrdinalIgnoreCase)
				{
					{"Root", State.Root.AllPages(tx)},
					{"Free Space Overhead", State.FreeSpaceRoot.AllPages(tx)},
					{"Free Pages", _freeSpaceHandling.AllPages(tx)}
				};

            foreach (var tree in tx.Trees)
            {
                results.Add(tree.Name, tree.AllPages(tx));
            }

            return results;
        }

		public EnvironmentStats Stats()
		{
			var numberOfAllocatedPages = Math.Max(_dataPager.NumberOfAllocatedPages, State.NextPageNumber - 1); // async apply to data file task

			return new EnvironmentStats
			{
				FreePages = _freeSpaceHandling.GetFreePageCount(),
				FreePagesOverhead = State.FreeSpaceRoot.State.PageCount,
				RootPages = State.Root.State.PageCount,
				UnallocatedPagesAtEndOfFile = _dataPager.NumberOfAllocatedPages - NextPageNumber,
				UsedDataFileSizeInBytes = (State.NextPageNumber - 1) * AbstractPager.PageSize,
				AllocatedDataFileSizeInBytes = numberOfAllocatedPages * AbstractPager.PageSize
			};
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

				            try
				            {
                                _journal.Applicator.ApplyLogsToDataFile(OldestTransaction, _cancellationTokenSource.Token);
				            }
				            catch (TimeoutException)
				            {
                                // we can ignore this, we'll try next time
				            }
				        }
			        }
		        }, TaskCreationOptions.LongRunning);
        }

        public void FlushLogToDataFile(Transaction tx = null)
        {
            if (_options.ManualFlushing == false)
                throw new NotSupportedException("Manual flushes are not set in the storage options, cannot manually flush!");

           _journal.Applicator.ApplyLogsToDataFile(OldestTransaction, _cancellationTokenSource.Token, tx);
        }

        public void AssertFlushingNotFailed()
        {
	        var flushingTaskCopy = _flushingTask;
	        if (flushingTaskCopy == null || flushingTaskCopy.IsFaulted == false)
                return;

            flushingTaskCopy.Wait();// force re-throw of error
        }

	    public void HandleDataDiskFullException(DiskFullException exception)
	    {
			if(_options.ManualFlushing)
				return;

		    _cancellationTokenSource.Cancel();
			_endOfDiskSpace = new EndOfDiskSpaceEvent(exception.DriveInfo);
	    }
    }
}

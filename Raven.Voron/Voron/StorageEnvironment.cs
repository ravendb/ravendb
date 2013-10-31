using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using Voron.Debugging;
using Voron.Impl;
using Voron.Impl.Backup;
using Voron.Impl.FileHeaders;
using Voron.Impl.FreeSpace;
using Voron.Impl.Journal;
using Voron.Trees;

namespace Voron
{
	using System.Diagnostics;

	public unsafe class StorageEnvironment : IDisposable
	{
	    private readonly StorageEnvironmentOptions _options;

	    private readonly ConcurrentDictionary<long, Transaction> _activeTransactions =
			new ConcurrentDictionary<long, Transaction>();

		private readonly IVirtualPager _dataPager;
		private readonly SliceComparer _sliceComparer;

        internal WriteAheadJournal _journal;
        private readonly SemaphoreSlim _txWriter = new SemaphoreSlim(1);

		private long _transactionsCounter;
		private readonly IFreeSpaceHandling _freeSpaceHandling;

		public TransactionMergingWriter Writer { get; private set; }

        public StorageEnvironmentState State { get; private set; }

		public SnapshotReader CreateSnapshot()
		{
			return new SnapshotReader(NewTransaction(TransactionFlags.Read));
		}

		public StorageEnvironment(StorageEnvironmentOptions options)
		{
		    try
			{
                _options = options;
                _dataPager = options.DataPager;
				_freeSpaceHandling = new FreeSpaceHandling(this);
				_sliceComparer = NativeMethods.memcmp;
                _journal = new WriteAheadJournal(this);

				Setup(_dataPager);

				State.FreeSpaceRoot.Name = Constants.FreeSpaceTreeName;
				State.Root.Name = Constants.RootTreeName;

				Writer = new TransactionMergingWriter(this);
			}
			catch (Exception)
			{
				Dispose();
				throw;
			}
		}

	    private void Setup(IVirtualPager pager)
		{
			if (pager.NumberOfAllocatedPages == 0)
			{
				_dataPager.EnsureContinuous(null, 0, 2); // for file headers

                _journal.WriteFileHeader(0);
                _journal.WriteFileHeader(1);

                _dataPager.Sync();

				const int initialNextPageNumber = 2;
				State = new StorageEnvironmentState(null, null, initialNextPageNumber);
				using (var tx = NewTransaction(TransactionFlags.ReadWrite))
				{
					var root = Tree.Create(tx, _sliceComparer);
					var freeSpace = Tree.Create(tx, _sliceComparer);

					// important to first create the two trees, then set them on the env
					tx.UpdateRootsIfNeeded(root, freeSpace);

					tx.Commit();
				}
				return;
			}
			// existing db, let us load it

			// the first two pages are allocated for double buffering tx commits
			var entry = FindLatestFileHeaderEntry();
            TransactionHeader* header;
	        _journal.RecoverDatabase(entry, out header);

	        var nextPageNumber = (header == null ? entry->LastPageNumber : header->LastPageNumber) + 1; 
            State = new StorageEnvironmentState(null, null, nextPageNumber)
                {
                    NextPageNumber = nextPageNumber
                };

	        _transactionsCounter = (header == null ? entry->TransactionId : header->TransactionId) + 1;
            
			using (var tx = NewTransaction(TransactionFlags.ReadWrite))
			{
                var root = Tree.Open(tx, _sliceComparer, header == null ? &entry->Root : &header->Root);
                var freeSpace = Tree.Open(tx, _sliceComparer, header == null ? &entry->FreeSpace : &header->FreeSpace);

				tx.UpdateRootsIfNeeded(root, freeSpace);
				tx.Commit();
			}
		}

		public IFreeSpaceHandling FreeSpaceHandling
		{
			get { return _freeSpaceHandling; }
		}

		public SliceComparer SliceComparer
		{
			get { return _sliceComparer; }
		}

		public long OldestTransaction
		{
			get { return _activeTransactions.Keys.OrderBy(x => x).FirstOrDefault(); }
		}

		public int PageSize
		{
			get { return _dataPager.PageSize; }
		}

		public IEnumerable<Tree> Trees
		{
			get { return State.Trees.Values; }
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

		public IncrementalBackupInfo BackupInfo
		{
			get { return _journal.FileHeader->BackupInfo; }
		}

	    public void DeleteTree(Transaction tx, string name)
		{
			if (tx.Flags == (TransactionFlags.ReadWrite) == false)
				throw new ArgumentException("Cannot create a new newRootTree with a read only transaction");

			Tree tree;
			if (tx.State.Trees.TryGetValue(name, out tree) == false)
				return;

			foreach (var page in tree.AllPages(tx))
			{
				tx.FreePage(page);
			}

			tx.State.Root.Delete(tx, name);

		    tx.DeletedTree(name);
		}

		public Tree CreateTree(Transaction tx, string name)
		{
			if (tx.Flags == (TransactionFlags.ReadWrite) == false)
				throw new ArgumentException("Cannot create a new tree with a read only transaction");

			Tree tree;
			if (tx.State.Trees.TryGetValue(name, out tree))
				return tree;

			Slice key = name;

			// we are in a write transaction, no need to handle locks
			var header = (TreeRootHeader*)tx.State.Root.DirectRead(tx, key);
			if (header != null)
			{
				tree = Tree.Open(tx, _sliceComparer, header);
				tree.Name = name;
				tx.State.AddTree(name, tree);
				return tree;
			}

			tree = Tree.Create(tx, _sliceComparer);
			tree.Name = name;

			var ptr = tx.State.Root.DirectAdd(tx, key, sizeof(TreeRootHeader));
			Debug.Assert(ptr.Count == 1);
			tree.State.CopyTo((TreeRootHeader*)ptr.FirstPointer);
		    tree.State.IsModified = true;
            tx.State.AddTree(name, tree);

			return tree;
		}

		public void Dispose()
		{
			if (_options.OwnsPagers)
				_options.Dispose();

			if(_journal != null)
				_journal.Dispose();
		}

		private FileHeader* FindLatestFileHeaderEntry()
		{
			Page fst = _dataPager.Read(0);
			Page snd = _dataPager.Read(1);

			FileHeader* e1 = GetFileHeaderFrom(fst);
			FileHeader* e2 = GetFileHeaderFrom(snd);

			FileHeader* entry = e1;
			if (e2->LogInfo.DataFlushCounter > e1->LogInfo.DataFlushCounter)
			{
				entry = e2;
			}
			return entry;
		}

		private FileHeader* GetFileHeaderFrom(Page p)
		{
			var fileHeader = ((FileHeader*)p.Base);
			if (fileHeader->MagicMarker != Constants.MagicMarker)
				throw new InvalidDataException(
					"The header page did not start with the magic marker, probably not a db file");
			if (fileHeader->Version != Constants.CurrentVersion)
				throw new InvalidDataException("This is a db file for version " + fileHeader->Version +
											   ", which is not compatible with the current version " +
											   Constants.CurrentVersion);
			if (fileHeader->LastPageNumber >= _dataPager.NumberOfAllocatedPages)
				throw new InvalidDataException("The last page number is beyond the number of allocated pages");
			if (fileHeader->TransactionId < 0)
				throw new InvalidDataException("The transaction number cannot be negative");
			return fileHeader;
		}

		public Transaction NewTransaction(TransactionFlags flags)
		{
			bool txLockTaken = false;
			try
			{
				long txId = _transactionsCounter;
				if (flags == (TransactionFlags.ReadWrite))
				{
					txId = _transactionsCounter + 1;
					_txWriter.Wait();
					txLockTaken = true;
				}
				var tx = new Transaction(this, txId, flags, _freeSpaceHandling);
				_activeTransactions.TryAdd(txId, tx);
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

		private void TransactionAfterCommit(long txId)
		{
			Transaction tx;
			_activeTransactions.TryGetValue(txId, out tx);
		}

		internal void TransactionCompleted(long txId)
		{
			Transaction tx;
			if (_activeTransactions.TryRemove(txId, out tx) == false)
				return;

			if (tx.Flags != (TransactionFlags.ReadWrite))
				return;
		    if (tx.Committed)
		    {
		        _transactionsCounter = txId;
		    }
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

			foreach (var tree in State.Trees)
			{
				results.Add(tree.Key, tree.Value.AllPages(tx));
			}

			return results;
		}

		public EnvironmentStats Stats()
		{
			return new EnvironmentStats
				{
					FreePages = _freeSpaceHandling.GetFreePageCount(),
					FreePagesOverhead = State.FreeSpaceRoot.State.PageCount,
					RootPages = State.Root.State.PageCount,
					HeaderPages = 2,
					UnallocatedPagesAtEndOfFile = _dataPager.NumberOfAllocatedPages - NextPageNumber
				};
		}

	    public void SetStateAfterTransactionCommit(StorageEnvironmentState state)
	    {
	        State = state;
	    }

	    public void FlushLogToDataFile(object state = null)
	    {
	        var journalApplicator = new WriteAheadJournal.JournalApplicator(_journal, OldestTransaction);
	        journalApplicator.ApplyLogsToDataFile();
	    }
	}
}
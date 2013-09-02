using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using Voron.Debugging;
using Voron.Impl;
using Voron.Impl.FileHeaders;
using Voron.Trees;

namespace Voron
{
	public unsafe class StorageEnvironment : IDisposable
	{
		private readonly ConcurrentDictionary<long, Transaction> _activeTransactions = 
			new ConcurrentDictionary<long, Transaction>();

		private readonly ConcurrentDictionary<string, Tree> _trees
			= new ConcurrentDictionary<string, Tree>(StringComparer.OrdinalIgnoreCase);

		private readonly bool _ownsPager;
		private readonly IVirtualPager _pager;
		private readonly SliceComparer _sliceComparer;

		private readonly SemaphoreSlim _txWriter = new SemaphoreSlim(1);

		private long _transactionsCounter;
		private readonly IFreeSpaceRepository _freeSpaceRepository;

		public TransactionMergingWriter Writer { get; private set; }

		public SnapshotReader CreateSnapshot()
		{
			return new SnapshotReader(NewTransaction(TransactionFlags.Read));
		}

		public StorageEnvironment(IVirtualPager pager, bool ownsPager = true)
		{
			try
			{
				_pager = pager;
				_ownsPager = ownsPager;
				_freeSpaceRepository = new NoFreeSpaceRepository();
				_sliceComparer = NativeMethods.memcmp;

				Setup(pager);

				FreeSpaceRoot.Name = "Free Space";
				Root.Name = "Root";

				Writer = new TransactionMergingWriter(this);
			}
			catch (Exception)
			{
				Dispose();
			}
		}

		private void Setup(IVirtualPager pager)
		{
			if (pager.NumberOfAllocatedPages == 0)
			{
				WriteEmptyHeaderPage(_pager.Get(null, 0));
				WriteEmptyHeaderPage(_pager.Get(null, 1));

				NextPageNumber = 2;
				using (var tx = new Transaction(_pager, this, _transactionsCounter + 1, TransactionFlags.ReadWrite, _freeSpaceRepository))
				{
					var root = Tree.Create(tx, _sliceComparer);
					var freeSpace = Tree.Create(tx, _sliceComparer);

					// important to first create the two trees, then set them on the env

					FreeSpaceRoot = freeSpace;
					Root = root;

					tx.UpdateRoots(root, freeSpace);

					tx.Commit();
				}
				return;
			}
			// existing db, let us load it

			// the first two pages are allocated for double buffering tx commits
			FileHeader* entry = FindLatestFileHeadeEntry();
			NextPageNumber = entry->LastPageNumber + 1;
			_transactionsCounter = entry->TransactionId + 1;
			using (var tx = new Transaction(_pager, this, _transactionsCounter + 1, TransactionFlags.ReadWrite, _freeSpaceRepository))
			{
				var root = Tree.Open(tx, _sliceComparer, &entry->Root);
				var freeSpace = Tree.Open(tx, _sliceComparer, &entry->FreeSpace);

				// important to first create the two trees, then set them on the env
				FreeSpaceRoot = freeSpace;
				Root = root;

				tx.Commit();
			}
		}

		public long NextPageNumber { get; set; }

		public IFreeSpaceRepository FreeSpaceRepository
		{
			get { return _freeSpaceRepository; }
		}

		public SliceComparer SliceComparer
		{
			get { return _sliceComparer; }
		}

		public Tree Root { get; private set; }
		public Tree FreeSpaceRoot { get; private set; }

		public long OldestTransaction
		{
			get { return _activeTransactions.Keys.OrderBy(x => x).FirstOrDefault(); }
		}

		public int PageSize
		{
			get { return _pager.PageSize; }
		}

		public Tree GetTree(Transaction tx, string name)
		{
			Tree tree;
			if (_trees.TryGetValue(name, out tree))
				return tree;

			if (tx != null && tx.ModifiedTrees.TryGetValue(name, out tree))
				return tree;

			throw new InvalidOperationException("No such tree: " + name);
		}


		public void DeleteTree(Transaction tx, string name)
		{
			if (tx.Flags == (TransactionFlags.ReadWrite) == false)
				throw new ArgumentException("Cannot create a new tree with a read only transaction");

			Tree tree;
			if (_trees.TryGetValue(name, out tree) == false)
				return;

			foreach (var page in tree.AllPages(tx))
			{
				tx.FreePage(page);
			}

			Root.Delete(tx, name);

			tx.ModifiedTrees.Add(name, null);
		}

		public Tree CreateTree(Transaction tx, string name)
		{
			if (tx.Flags == (TransactionFlags.ReadWrite) == false)
				throw new ArgumentException("Cannot create a new tree with a read only transaction");

			Tree tree;
			if (_trees.TryGetValue(name, out tree) ||
				tx.ModifiedTrees.TryGetValue(name, out tree))
				return tree;

			Slice key = name;

			// we are in a write transaction, no need to handle locks
			var header = (TreeRootHeader*)Root.DirectRead(tx, key);
			if (header != null)
			{
				tree = Tree.Open(tx, _sliceComparer, header);
				tree.Name = name;
				tx.ModifiedTrees.Add(name, tree);
				return tree;
			}

			tree = Tree.Create(tx, _sliceComparer);
			tree.Name = name;
			var space = Root.DirectAdd(tx, key, sizeof(TreeRootHeader));
			tree.State.CopyTo((TreeRootHeader*)space);

			tx.ModifiedTrees.Add(name, tree);

			return tree;
		}

		public void Dispose()
		{
			if (_ownsPager)
				_pager.Dispose();
		}

		private void WriteEmptyHeaderPage(Page pg)
		{
			var fileHeader = ((FileHeader*)pg.Base);
			fileHeader->MagicMarker = Constants.MagicMarker;
			fileHeader->Version = Constants.CurrentVersion;
			fileHeader->TransactionId = 0;
			fileHeader->LastPageNumber = 1;
			fileHeader->FreeSpace.RootPageNumber = -1;
			fileHeader->Root.RootPageNumber = -1;
		}

		private FileHeader* FindLatestFileHeadeEntry()
		{
			Page fst = _pager.Get(null, 0);
			Page snd = _pager.Get(null, 1);

			FileHeader* e1 = GetFileHeaderFrom(fst);
			FileHeader* e2 = GetFileHeaderFrom(snd);

			FileHeader* entry = e1;
			if (e2->TransactionId > e1->TransactionId)
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
			if (fileHeader->LastPageNumber >= _pager.NumberOfAllocatedPages)
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
				var tx = new Transaction(_pager, this, txId, flags, _freeSpaceRepository);
				_activeTransactions.TryAdd(txId, tx);
				var state = _pager.TransactionBegan();
				tx.AddPagerState(state);

				if (flags == TransactionFlags.ReadWrite)
				{
					_freeSpaceRepository.UpdateSections(tx, OldestTransaction);
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
			try
			{
				if (tx.Committed == false)
					return;
				_transactionsCounter = txId;
				if (tx.HasModifiedTrees == false)
					return;
				foreach (var tree in tx.ModifiedTrees)
				{
					Tree val = tree.Value;
					if (val == null)
						_trees.TryRemove(tree.Key, out val);
					else
						_trees.AddOrUpdate(tree.Key, val, (s, tree1) => val);
				}
			}
			finally
			{
				_txWriter.Release();

			}
		}

		public Dictionary<string, List<long>> AllPages(Transaction tx)
		{
			var results = new Dictionary<string, List<long>>(StringComparer.OrdinalIgnoreCase)
				{
					{"Root", Root.AllPages(tx)},
					{"Free Space Overhead", FreeSpaceRoot.AllPages(tx)},
					{"Free Pages", _freeSpaceRepository.AllPages(tx)}
				};

			foreach (var tree in _trees)
			{
				results.Add(tree.Key, tree.Value.AllPages(tx));
			}

			return results;
		}

		public EnvironmentStats Stats()
		{
			return new EnvironmentStats
				{
					FreePages = _freeSpaceRepository.GetFreePageCount(),
					FreePagesOverhead = FreeSpaceRoot.State.PageCount,
					RootPages = Root.State.PageCount,
					HeaderPages = 2,
					UnallocatedPagesAtEndOfFile = _pager.NumberOfAllocatedPages - NextPageNumber
				};
		}
	}
}
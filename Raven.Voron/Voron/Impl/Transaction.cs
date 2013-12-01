using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Security;
using System.Threading.Tasks;
using Voron.Exceptions;
using Voron.Impl.FileHeaders;
using Voron.Impl.FreeSpace;
using Voron.Impl.Journal;
using Voron.Impl.Paging;
using Voron.Trees;
using Voron.Util;

namespace Voron.Impl
{
	public unsafe class Transaction : IDisposable
	{
		private const int PagesTakenByHeader = 1;
		private readonly IVirtualPager _dataPager;
		private readonly StorageEnvironment _env;
		private readonly long _id;

		private readonly WriteAheadJournal _journal;
		private Dictionary<Tuple<Tree, Slice>, Tree> _multiValueTrees;
		private readonly HashSet<long> _dirtyPages = new HashSet<long>();
		private readonly HashSet<PagerState> _pagerStates = new HashSet<PagerState>();
		private readonly IFreeSpaceHandling _freeSpaceHandling;

		private readonly Dictionary<long, PageFromScratchBuffer> _scratchPagesTable = new Dictionary<long, PageFromScratchBuffer>();

		internal readonly List<JournalSnapshot> JournalSnapshots = new List<JournalSnapshot>();

		private List<string> _deletedTrees;

		public TransactionFlags Flags { get; private set; }

		internal StorageEnvironment Environment
		{
			get { return _env; }
		}

		public IVirtualPager DataPager
		{
			get { return _dataPager; }
		}

		public long Id
		{
			get { return _id; }
		}

		internal Action<long> AfterCommit = delegate { };
		private readonly StorageEnvironmentState _state;
		private int _allocatedPagesInTransaction;
		private int _overflowPagesInTransaction;
		private TransactionHeader* _txHeader;
		private readonly List<PageFromScratchBuffer> _transactionPages = new List<PageFromScratchBuffer>();

		public Page TempPage
		{
			get { return _dataPager.TempPage; }
		}

		public bool Committed { get; private set; }

		public bool RolledBack { get; private set; }

		public PagerState LatestPagerState { get; private set; }

		public StorageEnvironmentState State
		{
			get { return _state; }
		}

		public bool FlushedToJournal { get; private set; }

		public Transaction(StorageEnvironment env, long id, TransactionFlags flags, IFreeSpaceHandling freeSpaceHandling)
		{
			_dataPager = env.Options.DataPager;
			_env = env;
			_journal = env.Journal;
			_id = id;
			_freeSpaceHandling = freeSpaceHandling;
			Flags = flags;

			if (flags.HasFlag(TransactionFlags.ReadWrite) == false)
			{
				_state = env.State;
				_journal.GetSnapshots().ForEach(AddJournalSnapshot);
				return;
			}

			_state = env.State.Clone();

			var scratchPagerState = env.ScratchBufferPool.PagerState;
			scratchPagerState.AddRef();
			_pagerStates.Add(scratchPagerState);

			InitTransactionHeader();

			MarkTreesForWriteTransaction();
		}

		private void InitTransactionHeader()
		{
			var allocation = _env.ScratchBufferPool.Allocate(this, 1);
			var page = _env.ScratchBufferPool.ReadPage(allocation.PositionInScratchBuffer);
			_transactionPages.Add(allocation);
			NativeMethods.memset(page.Base, 0, AbstractPager.PageSize);
			_txHeader = (TransactionHeader*)page.Base;
			_txHeader->HeaderMarker = Constants.TransactionHeaderMarker;

			_txHeader->TransactionId = _id;
			_txHeader->NextPageNumber = _state.NextPageNumber;
			_txHeader->LastPageNumber = -1;
			_txHeader->PageCount = -1;
			_txHeader->Crc = 0;
			_txHeader->TxMarker = TransactionMarker.None;

			_allocatedPagesInTransaction = 0;
			_overflowPagesInTransaction = 0;
			_scratchPagesTable.Clear();
		}

		private void MarkTreesForWriteTransaction()
		{
			if (_state.Root != null)
				_state.Root.State.InWriteTransaction = true;
			if (_state.FreeSpaceRoot != null)
				_state.FreeSpaceRoot.State.InWriteTransaction = true;
			foreach (var tree in _state.Trees.Values)
			{
				tree.State.InWriteTransaction = true;
			}
		}

		public Tree GetTree(string treeName)
		{
			return State.GetTree(treeName, this);
		}

		public Page ModifyPage(long p, Cursor c)
		{
			_env.AssertFlushingNotFailed();

			Page page;
			if (_dirtyPages.Contains(p))
			{
				page = GetPageForModification(p, c);
				page.Dirty = true;

				return page;
			}

			page = GetPageForModification(p, c);

			var newPage = AllocatePage(1, p); // allocate new page in a log file but with the same number

			NativeMethods.memcpy(newPage.Base, page.Base, AbstractPager.PageSize);
			newPage.LastSearchPosition = page.LastSearchPosition;
			newPage.LastMatch = page.LastMatch;

			return newPage;
		}

		private Page GetPageForModification(long p, Cursor c)
		{
			var page = c.GetPage(p);
			if (page != null)
				return page;

			PageFromScratchBuffer value;
			if (_scratchPagesTable.TryGetValue(p, out value))
			{
				return _env.ScratchBufferPool.ReadPage(value.PositionInScratchBuffer);
			}
			return _journal.ReadPage(this, p) ?? _dataPager.Read(p);
		}

		public Page GetReadOnlyPage(long n)
		{
			PageFromScratchBuffer value;
			if (_scratchPagesTable.TryGetValue(n, out value))
			{
				return _env.ScratchBufferPool.ReadPage(value.PositionInScratchBuffer);
			}

			return _journal.ReadPage(this, n) ?? _dataPager.Read(n);
		}

		public Page AllocatePage(int numberOfPages, long? pageNumber = null)
		{
			if (pageNumber == null)
			{
				pageNumber = _freeSpaceHandling.TryAllocateFromFreeSpace(this, numberOfPages);
				if (pageNumber == null) // allocate from end of file
				{
					pageNumber = State.NextPageNumber;
					State.NextPageNumber += numberOfPages;
				}
			}

			if (_env.Options.MaxStorageSize.HasValue) // check against quota
			{
				var maxAvailablePageNumber = _env.Options.MaxStorageSize / AbstractPager.PageSize;

				if(pageNumber.Value > maxAvailablePageNumber)
					throw new QuotaException(
						string.Format(
							"The maximum storage size quota ({0} bytes) has been reached. " +
							"Currently configured storage quota is allowing to allocate the following maximum page number {1}, while the requested page number is {2}. " +
							"To increase the quota, use the MaxStorageSize property on the storage environment options.",
							_env.Options.MaxStorageSize, maxAvailablePageNumber, pageNumber.Value));
			}


			Debug.Assert(pageNumber < State.NextPageNumber);

			var pageFromScratchBuffer = _env.ScratchBufferPool.Allocate(this, numberOfPages);
			_transactionPages.Add(pageFromScratchBuffer);

			var page = _env.ScratchBufferPool.ReadPage(pageFromScratchBuffer.PositionInScratchBuffer);
			page.PageNumber = pageNumber.Value;

			_allocatedPagesInTransaction++;
			if (numberOfPages > 1)
			{
				_overflowPagesInTransaction += (numberOfPages - 1);
			}

			_scratchPagesTable[pageNumber.Value] = pageFromScratchBuffer;

			page.Lower = (ushort)Constants.PageHeaderSize;
			page.Upper = AbstractPager.PageSize;
			page.Dirty = true;

			_dirtyPages.Add(page.PageNumber);
			return page;
		}



		internal int GetNumberOfFreePages(NodeHeader* node)
		{
			return GetNodeDataSize(node) / Constants.PageNumberSize;
		}

		internal int GetNodeDataSize(NodeHeader* node)
		{
			if (node->Flags == (NodeFlags.PageRef)) // lots of data, enough to overflow!
			{
				var overflowPage = GetReadOnlyPage(node->PageNumber);
				return overflowPage.OverflowSize;
			}
			return node->DataSize;
		}

		public Task Commit()
		{
			if (Flags != (TransactionFlags.ReadWrite) || RolledBack)
				return Task.FromResult(1); // nothing to do

			FlushAllMultiValues();

			if (_deletedTrees != null)
			{
				foreach (var deletedTree in _deletedTrees)
				{
					State.RemoveTree(deletedTree);
				}
			}

			State.Root.State.InWriteTransaction = false;
			State.FreeSpaceRoot.State.InWriteTransaction = false;

			foreach (var treeKvp in State.Trees)
			{
				treeKvp.Value.State.InWriteTransaction = false;
				var treeState = treeKvp.Value.State;
				if (treeState.IsModified)
				{
					var treePtr = (TreeRootHeader*)State.Root.DirectAdd(this, treeKvp.Key, sizeof(TreeRootHeader));
					treeState.CopyTo(treePtr);
				}
			}

#if DEBUG
			if (State.Root != null && State.FreeSpaceRoot != null)
			{
				Debug.Assert(State.Root.State.RootPageNumber != State.FreeSpaceRoot.State.RootPageNumber);
			}
#endif

			_txHeader->LastPageNumber = _state.NextPageNumber - 1;
			_txHeader->PageCount = _allocatedPagesInTransaction;
			_txHeader->OverflowPageCount = _overflowPagesInTransaction;
			_state.Root.State.CopyTo(&_txHeader->Root);
			_state.FreeSpaceRoot.State.CopyTo(&_txHeader->FreeSpace);


			uint crc = 0;

			for (int i = 1; i < _transactionPages.Count; i++)
			{
				var txPage = _transactionPages[i];
				crc = Crc.Extend(crc, _env.ScratchBufferPool.ReadPage(txPage.PositionInScratchBuffer).Base, 0,
					txPage.NumberOfPages * AbstractPager.PageSize);
			}

			_txHeader->Crc = crc;

			_txHeader->TxMarker |= TransactionMarker.Commit;

			Task task;
			if (_allocatedPagesInTransaction + _overflowPagesInTransaction > 0) // nothing changed in this transaction
			{
				task = _journal.WriteToJournal(this, _allocatedPagesInTransaction + _overflowPagesInTransaction + PagesTakenByHeader);
				FlushedToJournal = true;
			}
			else
			{
				task = Task.FromResult(1);
			}

			_env.SetStateAfterTransactionCommit(State);
			Committed = true;
			AfterCommit(_id);

			return task;
		}


		public void Rollback()
		{
			if (Committed || RolledBack || Flags != (TransactionFlags.ReadWrite))
				return;

			foreach (var pageFromScratch in _transactionPages)
			{
				_env.ScratchBufferPool.Free(pageFromScratch.PositionInScratchBuffer);
			}

			RolledBack = true;
		}


		private unsafe void FlushAllMultiValues()
		{
			if (_multiValueTrees == null)
				return;

			foreach (var multiValueTree in _multiValueTrees)
			{
				var parentTree = multiValueTree.Key.Item1;
				var key = multiValueTree.Key.Item2;
				var childTree = multiValueTree.Value;

				var trh = (TreeRootHeader*)parentTree.DirectAdd(this, key, sizeof(TreeRootHeader), NodeFlags.MultiValuePageRef);
				childTree.State.CopyTo(trh);

				//parentTree.SetAsMultiValueTreeRef(this, key);
			}
		}

		public void Dispose()
		{
			if (!Committed && !RolledBack && Flags == TransactionFlags.ReadWrite)
				Rollback();

			_env.TransactionCompleted(_id);
			foreach (var pagerState in _pagerStates)
			{
				pagerState.Release();
			}
		}


		public void FreePage(long pageNumber)
		{
			Debug.Assert(pageNumber >= 2);
			_dirtyPages.Remove(pageNumber);
			_freeSpaceHandling.FreePage(this, pageNumber);
		}

		internal void UpdateRootsIfNeeded(Tree root, Tree freeSpace)
		{
			//can only happen during initial transaction that creates Root and FreeSpaceRoot trees
			if (State.Root == null && State.FreeSpaceRoot == null && State.Trees.Count == 0)
			{
				State.Root = root;
				State.FreeSpaceRoot = freeSpace;
			}
		}

		public void AddPagerState(PagerState state)
		{
			LatestPagerState = state;
			_pagerStates.Add(state);
		}

		public Cursor NewCursor(Tree tree)
		{
			return new Cursor();
		}

		public void AddMultiValueTree(Tree tree, Slice key, Tree mvTree)
		{
			if (_multiValueTrees == null)
				_multiValueTrees = new Dictionary<Tuple<Tree, Slice>, Tree>(new TreeAndSliceComparer(_env.SliceComparer));
			mvTree.IsMultiValueTree = true;
			_multiValueTrees.Add(Tuple.Create(tree, key), mvTree);
		}

		public bool TryGetMultiValueTree(Tree tree, Slice key, out Tree mvTree)
		{
			mvTree = null;
			if (_multiValueTrees == null)
				return false;
			return _multiValueTrees.TryGetValue(Tuple.Create(tree, key), out mvTree);
		}

		public bool TryRemoveMultiValueTree(Tree parentTree, Slice key)
		{
			var keyToRemove = Tuple.Create(parentTree, key);
			if (_multiValueTrees == null || !_multiValueTrees.ContainsKey(keyToRemove))
				return false;

			return _multiValueTrees.Remove(keyToRemove);
		}

		public void DeletedTree(string name)
		{
			if (_deletedTrees == null)
				_deletedTrees = new List<string>();
			_deletedTrees.Add(name);
		}


		private void AddJournalSnapshot(JournalSnapshot snapshot)
		{
			if (JournalSnapshots.Any(x => x.Number == snapshot.Number))
				throw new InvalidOperationException("Cannot add a snapshot of log file with number " + snapshot.Number +
													" to the transaction, because it already exists in a snapshot collection");

			JournalSnapshots.Add(snapshot);
		}

		public List<PageFromScratchBuffer> GetTransactionPages()
		{
			return _transactionPages;
		}
	}
}
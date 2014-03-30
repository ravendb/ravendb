using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
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
		private readonly IDictionary<Tree, RecentlyFoundPages> _recentlyFoundPages = new Dictionary<Tree, RecentlyFoundPages>();

		internal readonly List<JournalSnapshot> JournalSnapshots = new List<JournalSnapshot>();

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

		internal Action<Transaction> AfterCommit = delegate { };
		private readonly StorageEnvironmentState _state;
		private int _allocatedPagesInTransaction;
		private int _overflowPagesInTransaction;
		private TransactionHeader* _txHeader;
		private readonly List<PageFromScratchBuffer> _transactionPages = new List<PageFromScratchBuffer>();
	    private readonly Dictionary<string, Tree> _trees = new Dictionary<string, Tree>();
	    private readonly PagerState _scratchPagerState;

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
            var scratchPagerState = env.ScratchBufferPool.PagerState;
            scratchPagerState.AddRef();
            _pagerStates.Add(scratchPagerState);
			if (flags.HasFlag(TransactionFlags.ReadWrite) == false)
			{
                // for read transactions, we need to keep the pager state frozen
                // for write transactions, we can use the current one (which == null)
			    _scratchPagerState = scratchPagerState;

				_state = env.State;
				_journal.GetSnapshots().ForEach(AddJournalSnapshot);
				return;
			}

			_state = env.State.Clone();

			InitTransactionHeader();

			MarkTreesForWriteTransaction();
		}

		private void InitTransactionHeader()
		{
			var allocation = _env.ScratchBufferPool.Allocate(this, 1);
			var page = _env.ScratchBufferPool.ReadPage(allocation.PositionInScratchBuffer, _scratchPagerState);
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
			_txHeader->Compressed = false;
			_txHeader->CompressedSize = 0;
			_txHeader->UncompressedSize = 0;

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
			foreach (var tree in Trees)
			{
				tree.State.InWriteTransaction = true;
			}
		}

		public Tree ReadTree(string treeName)
		{
		    Tree tree;
		    if (_trees.TryGetValue(treeName, out tree))
		        return tree;

            var header = (TreeRootHeader*)State.Root.DirectRead(this, treeName);
		    if (header != null)
		    {
		        tree = Tree.Open(this, _env.SliceComparer, header);
		        tree.Name = treeName;
		        _trees.Add(treeName, tree);
		        return tree;
		    }

		    _trees.Add(treeName, null);
		    return null;
		}

	    public Page ModifyPage(long num, Page page)
		{
			_env.AssertFlushingNotFailed();

			if (_dirtyPages.Contains(num))
			{
				page = GetPageForModification(num, page);
				page.Dirty = true;

				return page;
			}

			page = GetPageForModification(num, page);

			var newPage = AllocatePage(1, num); // allocate new page in a log file but with the same number

			NativeMethods.memcpy(newPage.Base, page.Base, AbstractPager.PageSize);
			newPage.LastSearchPosition = page.LastSearchPosition;
			newPage.LastMatch = page.LastMatch;

			return newPage;
		}

		private Page GetPageForModification(long p, Page page)
		{
		    return page ?? GetReadOnlyPage(p);
		}

	    public Page GetReadOnlyPage(long pageNumber)
		{
			PageFromScratchBuffer value;
		    Page p;
			if (_scratchPagesTable.TryGetValue(pageNumber, out value))
			{
			    p = _env.ScratchBufferPool.ReadPage(value.PositionInScratchBuffer, _scratchPagerState);
			}
			else
			{
			    p =  _journal.ReadPage(this, pageNumber, _scratchPagerState) ?? _dataPager.Read(pageNumber);
			}

            Debug.Assert(p != null && p.PageNumber == pageNumber, string.Format("Requested ReadOnly page #{0}. Got #{1} from {2}", pageNumber, p.PageNumber, p.Source));

		    return p;
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

	    public IEnumerable<Tree> Trees
	    {
            get { return _trees.Values; }
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

		public void Commit()
		{
			if (Flags != (TransactionFlags.ReadWrite))
				return; // nothing to do

            if (Committed)
                throw new InvalidOperationException("Cannot commit already commited transaction.");

            if (RolledBack)
                throw new InvalidOperationException("Cannot commit rolledback transaction.");

			FlushAllMultiValues();

			State.Root.State.InWriteTransaction = false;
			State.FreeSpaceRoot.State.InWriteTransaction = false;

			foreach (var tree in Trees)
			{
				tree.State.InWriteTransaction = false;
				var treeState = tree.State;
				if (treeState.IsModified)
				{
					var treePtr = (TreeRootHeader*)State.Root.DirectAdd(this, tree.Name, sizeof(TreeRootHeader));
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

			_txHeader->TxMarker |= TransactionMarker.Commit;

			if (_allocatedPagesInTransaction + _overflowPagesInTransaction > 0) // nothing changed in this transaction
			{
				_journal.WriteToJournal(this, _allocatedPagesInTransaction + _overflowPagesInTransaction + PagesTakenByHeader);
				FlushedToJournal = true;
			}

			Committed = true;
			AfterCommit(this);
		}


		public void Rollback()
		{
			if (Committed || RolledBack || Flags != (TransactionFlags.ReadWrite))
				return;

			foreach (var pageFromScratch in _transactionPages)
			{
				_env.ScratchBufferPool.Free(pageFromScratch.PositionInScratchBuffer, -1);
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

			_env.TransactionCompleted(this);
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
			if (State.Root == null && State.FreeSpaceRoot == null)
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

		public bool RemoveTree(string name)
		{
		    return _trees.Remove(name);
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

		public RecentlyFoundPages GetRecentlyFoundPages(Tree tree)
		{
			RecentlyFoundPages pages;
			if (_recentlyFoundPages.TryGetValue(tree, out pages))
				return pages;

			return null;
		}

	    public void ClearRecentFoundPages(Tree tree)
	    {
	        _recentlyFoundPages.Remove(tree);
	    }

		public void AddRecentlyFoundPage(Tree tree, RecentlyFoundPages.FoundPage foundPage)
		{
			RecentlyFoundPages pages;
		    if (_recentlyFoundPages.TryGetValue(tree, out pages) == false)
		        _recentlyFoundPages[tree] = pages = new RecentlyFoundPages(Flags == TransactionFlags.Read ? 8 : 2);

			pages.Add(foundPage);
		}

	    public void AddTree(string name, Tree tree)
	    {
	        Tree value;
	        if (_trees.TryGetValue(name, out value) && value != null)
	        {
	            throw new InvalidOperationException("Tree already exists: " + name);
	        }
	        _trees[name] = tree;
	    }
	}
}

using System;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.IO;
using Voron.Debugging;
using Voron.Exceptions;
using Voron.Impl;
using Voron.Impl.FileHeaders;
using Voron.Impl.Paging;

namespace Voron.Trees
{
	public unsafe partial class Tree
	{
		private TreeMutableState _state = new TreeMutableState();

		public string Name { get; set; }

		public TreeMutableState State
		{
			get { return _state; }
		}

		private readonly Transaction _tx;

		private readonly SliceComparer _cmp;

		private Tree(Transaction tx, SliceComparer cmp, long root)
		{
			_tx = tx;
			_cmp = cmp;
			_state.RootPageNumber = root;
		}

		private Tree(Transaction tx, SliceComparer cmp, TreeMutableState state)
		{
			_tx = tx;
			_cmp = cmp;
			_state = state;
		}

		public static Tree Open(Transaction tx, SliceComparer cmp, TreeRootHeader* header)
		{
			return new Tree(tx, cmp, header->RootPageNumber)
			{
				_state =
				{
					PageCount = header->PageCount,
					BranchPages = header->BranchPages,
					Depth = header->Depth,
					OverflowPages = header->OverflowPages,
					LeafPages = header->LeafPages,
					EntriesCount = header->EntriesCount,
					Flags = header->Flags,
                    InWriteTransaction = tx.Flags.HasFlag(TransactionFlags.ReadWrite)
				}
			};
		}

		public static Tree Create(Transaction tx, SliceComparer cmp, TreeFlags flags = TreeFlags.None)
		{
			var newRootPage = NewPage(tx, PageFlags.Leaf, 1);
			var tree = new Tree(tx, cmp, newRootPage.PageNumber)
			{
				_state =
				{
					Depth = 1,
					Flags = flags,
                    InWriteTransaction = true
				}
			};
			
			tree.State.RecordNewPage(newRootPage, 1);
			return tree;
		}

		public void Add(Slice key, Stream value, ushort? version = null)
		{
		    if (value == null) throw new ArgumentNullException("value");
		    if (value.Length > int.MaxValue)
		        throw new ArgumentException("Cannot add a value that is over 2GB in size", "value");

			State.IsModified = true;
			var pos = DirectAdd(key, (int)value.Length, version: version);
		    
		    CopyStreamToPointer(_tx, value, pos);
		}

		public long Increment(Slice key, long delta, ushort? version = null)
		{
			long currentValue = 0;

			var read = Read(key);
			if (read != null)
				currentValue = read.Reader.ReadLittleEndianInt64();

			var value = currentValue + delta;
			Add(key, BitConverter.GetBytes(value), version);

			return value;
		}

		public void Add(Slice key, byte[] value, ushort? version = null)
		{
			if (value == null) throw new ArgumentNullException("value");

			State.IsModified = true;
			var pos = DirectAdd(key, value.Length, version: version);

			fixed (byte* src = value)
			{
				NativeMethods.memcpy(pos, src, value.Length);
			}
		}

		public void Add(Slice key, Slice value, ushort? version = null)
		{
			if (value == null) throw new ArgumentNullException("value");

			State.IsModified = true;
			var pos = DirectAdd(key, value.Size, version: version);

			value.CopyTo(pos);
		}

		private static void CopyStreamToPointer(Transaction tx, Stream value, byte* pos)
		{
			TemporaryPage tmp;
			using (tx.Environment.GetTemporaryPage(tx, out tmp))
			{
				var tempPageBuffer = tmp.TempPageBuffer;
				var tempPagePointer = tmp.TempPagePointer;
				while (true)
				{
					var read = value.Read(tempPageBuffer, 0, AbstractPager.PageSize);
					if (read == 0)
						break;
					NativeMethods.memcpy(pos, tempPagePointer, read);
					pos += read;
				}
			}
		}

		internal byte* DirectAdd(Slice key, int len, NodeFlags nodeType = NodeFlags.Data, ushort? version = null)
		{
			Debug.Assert(nodeType == NodeFlags.Data || nodeType == NodeFlags.MultiValuePageRef);

			if (_tx.Flags == (TransactionFlags.ReadWrite) == false)
				throw new ArgumentException("Cannot add a value in a read only transaction");

			if (key.Size > _tx.DataPager.MaxNodeSize)
				throw new ArgumentException(
					"Key size is too big, must be at most " + _tx.DataPager.MaxNodeSize + " bytes, but was " + key.Size, "key");

			Lazy<Cursor> lazy;
			var foundPage = FindPageFor(key, out lazy);

			var page = _tx.ModifyPage(foundPage.PageNumber, foundPage);

			ushort nodeVersion = 0;
			bool? shouldGoToOverflowPage = null;
			if (page.LastMatch == 0) // this is an update operation
			{
				var node = page.GetNode(page.LastSearchPosition);

				Debug.Assert(node->KeySize == key.Size && new Slice(node).Equals(key));

				shouldGoToOverflowPage = _tx.DataPager.ShouldGoToOverflowPage(len);

				byte* pos;
				if (shouldGoToOverflowPage == false)
				{
					// optimization for Data and MultiValuePageRef - try to overwrite existing node space
					if (TryOverwriteDataOrMultiValuePageRefNode(node, key, len, nodeType, version, out pos))
						return pos;
				}
				else
				{
					// optimization for PageRef - try to overwrite existing overflows
					if (TryOverwriteOverflowPages(State, node, key, len, version, out pos))
						return pos;
				}

				RemoveLeafNode(page, out nodeVersion);
			}
			else // new item should be recorded
			{
				State.EntriesCount++;
			}

			CheckConcurrency(key, version, nodeVersion, TreeActionType.Add);

			var lastSearchPosition = page.LastSearchPosition; // searching for overflow pages might change this
			byte* overFlowPos = null;
			var pageNumber = -1L;
			if (shouldGoToOverflowPage ?? _tx.DataPager.ShouldGoToOverflowPage(len))
			{
				pageNumber = WriteToOverflowPages(State, len, out overFlowPos);
				len = -1;
				nodeType = NodeFlags.PageRef;
			}

			byte* dataPos;
			if (page.HasSpaceFor(_tx, key, len) == false)
			{
			    var cursor = lazy.Value;
			    cursor.Update(cursor.Pages.First, page);

				var pageSplitter = new PageSplitter(_tx, this, _cmp, key, len, pageNumber, nodeType, nodeVersion, cursor, State);
			    dataPos = pageSplitter.Execute();

				DebugValidateTree(State.RootPageNumber);
			}
			else
			{
				switch (nodeType)
				{
					case NodeFlags.PageRef:
						dataPos = page.AddPageRefNode(lastSearchPosition, key, pageNumber);
						break;
					case NodeFlags.Data:
						dataPos = page.AddDataNode(lastSearchPosition, key, len, nodeVersion);
						break;
					case NodeFlags.MultiValuePageRef:
						dataPos = page.AddMultiValueNode(lastSearchPosition, key, len, nodeVersion);
						break;
					default:
						throw new NotSupportedException("Unknown node type for direct add operation: " + nodeType);
				}
				page.DebugValidate(_tx, _cmp, State.RootPageNumber);
			}
			if (overFlowPos != null)
				return overFlowPos;
			return dataPos;
		}

		private long WriteToOverflowPages(TreeMutableState txInfo, int overflowSize, out byte* dataPos)
		{
			var numberOfPages = _tx.DataPager.GetNumberOfOverflowPages(overflowSize);
			var overflowPageStart = _tx.AllocatePage(numberOfPages);
			overflowPageStart.Flags = PageFlags.Overflow;
			overflowPageStart.OverflowSize = overflowSize;
			dataPos = overflowPageStart.Base + Constants.PageHeaderSize;
			txInfo.OverflowPages += numberOfPages;
			txInfo.PageCount += numberOfPages;
			return overflowPageStart.PageNumber;
		}

		private void RemoveLeafNode(Page page, out ushort nodeVersion)
		{
			var node = page.GetNode(page.LastSearchPosition);
			nodeVersion = node->Version;
			if (node->Flags == (NodeFlags.PageRef)) // this is an overflow pointer
			{
				var overflowPage = _tx.GetReadOnlyPage(node->PageNumber);
				var numberOfPages = _tx.DataPager.GetNumberOfOverflowPages(overflowPage.OverflowSize);
				for (int i = 0; i < numberOfPages; i++)
				{
					_tx.FreePage(overflowPage.PageNumber + i);
				}

				State.OverflowPages -= numberOfPages;
				State.PageCount -= numberOfPages;
			}
			page.RemoveNode(page.LastSearchPosition);
		}

		[Conditional("VALIDATE")]
		public void DebugValidateTree(long rootPageNumber)
		{
			var pages = new HashSet<long>();
			var stack = new Stack<Page>();
			var root = _tx.GetReadOnlyPage(rootPageNumber);
			stack.Push(root);
			pages.Add(rootPageNumber);
			while (stack.Count > 0)
			{
				var p = stack.Pop();
				if (p.NumberOfEntries == 0 && p != root)
				{
					DebugStuff.RenderAndShow(_tx, rootPageNumber, 1);
					throw new InvalidOperationException("The page " + p.PageNumber + " is empty");

				}
				p.DebugValidate(_tx, _cmp, rootPageNumber);
				if (p.IsBranch == false)
					continue;
				for (int i = 0; i < p.NumberOfEntries; i++)
				{
					var page = p.GetNode(i)->PageNumber;
					if (pages.Add(page) == false)
					{
						DebugStuff.RenderAndShow(_tx, rootPageNumber, 1);
						throw new InvalidOperationException("The page " + page + " already appeared in the tree!");
					}
					stack.Push(_tx.GetReadOnlyPage(page));
				}
			}
		}

		internal Page FindPageFor(Slice key, out Lazy<Cursor> cursor)
		{
			Page p;

			if (TryUseRecentTransactionPage(key, out cursor, out p))
		    {
		        return p;
		    }

			return SearchForPage(key, ref cursor);
		}

	    private Page SearchForPage(Slice key, ref Lazy<Cursor> cursor)
	    {
			var p = _tx.GetReadOnlyPage(State.RootPageNumber);
	        var c = new Cursor();
	        c.Push(p);

	        bool rightmostPage = true;
	        bool leftmostPage = true;

	        while (p.Flags == (PageFlags.Branch))
	        {
	            int nodePos;
	            if (key.Options == SliceOptions.BeforeAllKeys)
	            {
	                p.LastSearchPosition = nodePos = 0;
	                rightmostPage = false;
	            }
	            else if (key.Options == SliceOptions.AfterAllKeys)
	            {
	                p.LastSearchPosition = nodePos = (ushort) (p.NumberOfEntries - 1);
	                leftmostPage = false;
	            }
	            else
	            {
	                if (p.Search(key, _cmp) != null)
	                {
	                    nodePos = p.LastSearchPosition;
	                    if (p.LastMatch != 0)
	                    {
	                        nodePos--;
	                        p.LastSearchPosition--;
	                    }

	                    if (nodePos != 0)
	                        leftmostPage = false;

	                    rightmostPage = false;
	                }
	                else
	                {
	                    nodePos = (ushort) (p.LastSearchPosition - 1);

	                    leftmostPage = false;
	                }
	            }

	            var node = p.GetNode(nodePos);
				p = _tx.GetReadOnlyPage(node->PageNumber);
	            Debug.Assert(node->PageNumber == p.PageNumber,
	                string.Format("Requested Page: #{0}. Got Page: #{1}", node->PageNumber, p.PageNumber));

	            c.Push(p);
	        }

	        if (p.IsLeaf == false)
	            throw new DataException("Index points to a non leaf page");

	        p.Search(key, _cmp); // will set the LastSearchPosition

	        AddToRecentlyFoundPages(c, p, leftmostPage, rightmostPage);

	        cursor = new Lazy<Cursor>(() => c);
	        return p;
	    }

	    private void AddToRecentlyFoundPages(Cursor c, Page p, bool? leftmostPage, bool? rightmostPage)
	    {
	        var foundPage = new RecentlyFoundPages.FoundPage(c.Pages.Count)
	        {
	            Number = p.PageNumber,
	            FirstKey = leftmostPage == true ? Slice.BeforeAllKeys : p.GetNodeKey(0),
	            LastKey = rightmostPage == true ? Slice.AfterAllKeys : p.GetNodeKey(p.NumberOfEntries - 1),
	        };
	        var cur = c.Pages.First;
	        int pos = foundPage.CursorPath.Length - 1;
	        while (cur != null)
	        {
	            foundPage.CursorPath[pos--] = cur.Value.PageNumber;
	            cur = cur.Next;
	        }

			_tx.AddRecentlyFoundPage(this, foundPage);
	    }

	    private bool TryUseRecentTransactionPage(Slice key, out Lazy<Cursor> cursor, out Page page)
		{
			page = null;
			cursor = null;

			var recentPages = _tx.GetRecentlyFoundPages(this);

			if (recentPages == null)
				return false;

			var foundPage = recentPages.Find(key);

			if (foundPage == null)
				return false;

			var lastFoundPageNumber = foundPage.Number;
			page = _tx.GetReadOnlyPage(lastFoundPageNumber);

			if (page.IsLeaf == false)
				throw new DataException("Index points to a non leaf page");

			page.NodePositionFor(key, _cmp); // will set the LastSearchPosition

			var cursorPath = foundPage.CursorPath;
			var pageCopy = page;
			cursor = new Lazy<Cursor>(() =>
			{
				var c = new Cursor();
				foreach (var p in cursorPath)
				{
					if (p == lastFoundPageNumber)
						c.Push(pageCopy);
					else
					{
						var cursorPage = _tx.GetReadOnlyPage(p);
						if (key.Options == SliceOptions.BeforeAllKeys)
						{
							cursorPage.LastSearchPosition = 0;
						}
						else if (key.Options == SliceOptions.AfterAllKeys)
						{
							cursorPage.LastSearchPosition = (ushort)(cursorPage.NumberOfEntries - 1);
						}
						else if (cursorPage.Search(key, _cmp) != null)
						{
							if (cursorPage.LastMatch != 0)
							{
								cursorPage.LastSearchPosition--;
							}
						}

						c.Push(cursorPage);
					}
				}

				return c;
			});

			return true;
		}

		internal static Page NewPage(Transaction tx, PageFlags flags, int num)
		{
			var page = tx.AllocatePage(num);
			page.Flags = flags;

			return page;
		}

		public void Delete(Slice key, ushort? version = null)
		{
			if (_tx.Flags == (TransactionFlags.ReadWrite) == false)
				throw new ArgumentException("Cannot delete a value in a read only transaction");

			State.IsModified = true;
			Lazy<Cursor> lazy;
			var page = FindPageFor(key, out lazy);

			page.NodePositionFor(key, _cmp);
			if (page.LastMatch != 0)
				return; // not an exact match, can't delete

			page = _tx.ModifyPage(page.PageNumber, page);

			State.EntriesCount--;
			ushort nodeVersion;
			RemoveLeafNode(page, out nodeVersion);

			CheckConcurrency(key, version, nodeVersion, TreeActionType.Delete);

			var treeRebalancer = new TreeRebalancer(_tx, this);
			var changedPage = page;
			while (changedPage != null)
			{
				changedPage = treeRebalancer.Execute(lazy.Value, changedPage);
			}

			page.DebugValidate(_tx, _cmp, State.RootPageNumber);
		}

		public TreeIterator Iterate(WriteBatch writeBatch = null)
		{
			return new TreeIterator(this, _tx, _cmp);
		}

		public ReadResult Read(Slice key)
		{
			Lazy<Cursor> lazy;
			var p = FindPageFor(key, out lazy);

            if (p.LastMatch != 0)
		        return null;

		    var node = p.GetNode(p.LastSearchPosition);

			return new ReadResult(NodeHeader.Reader(_tx, node), node->Version);
		}

		public int GetDataSize(Slice key)
		{
			Lazy<Cursor> lazy;
			var p = FindPageFor(key, out lazy);
			var node = p.Search(key, _cmp);

			if (node == null || new Slice(node).Compare(key, _cmp) != 0)
				return -1;

			return node->DataSize;
		}

		public ushort ReadVersion(Slice key)
		{
			Lazy<Cursor> lazy;
			var p = FindPageFor(key, out lazy);
			var node = p.Search(key, _cmp);

			if (node == null || new Slice(node).Compare(key, _cmp) != 0)
				return 0;

			return node->Version;
		}

		internal byte* DirectRead(Slice key)
		{
			Lazy<Cursor> lazy;
			var p = FindPageFor(key, out lazy);
			var node = p.Search(key, _cmp);

			if (node == null)
				return null;

			var item1 = new Slice(node);

			if (item1.Compare(key, _cmp) != 0)
				return null;

			if (node->Flags == (NodeFlags.PageRef))
			{
				var overFlowPage = _tx.GetReadOnlyPage(node->PageNumber);
				return overFlowPage.Base + Constants.PageHeaderSize;
			}

			return (byte*) node + node->KeySize + Constants.NodeHeaderSize;
		}

		public List<long> AllPages()
		{
			var results = new List<long>();
			var stack = new Stack<Page>();
			var root = _tx.GetReadOnlyPage(State.RootPageNumber);
			stack.Push(root);
			while (stack.Count > 0)
			{
				var p = stack.Pop();
				results.Add(p.PageNumber);
				for (int i = 0; i < p.NumberOfEntries; i++)
				{
					var node = p.GetNode(i);
					var pageNumber = node->PageNumber;
					if (p.IsBranch)
					{
						stack.Push(_tx.GetReadOnlyPage(pageNumber));
					}
					else if (node->Flags == NodeFlags.PageRef)
					{
						// This is an overflow page
						var overflowPage = _tx.GetReadOnlyPage(pageNumber);
						var numberOfPages = _tx.DataPager.GetNumberOfOverflowPages(overflowPage.OverflowSize);
						for (long j = 0; j < numberOfPages; ++j)
							results.Add(overflowPage.PageNumber + j);
					}
					else if (node->Flags == NodeFlags.MultiValuePageRef)
					{
						var childTreeHeader = (TreeRootHeader*)((byte*)node + node->KeySize + Constants.NodeHeaderSize);

						results.Add(childTreeHeader->RootPageNumber);

						// this is a multi value
						var tree = OpenOrCreateMultiValueTree(_tx, new Slice(node), node);
						results.AddRange(tree.AllPages());
					}
				}
			}
			return results;
		}

		public override string ToString()
		{
			return Name + " " + State.EntriesCount;
		}

		private void CheckConcurrency(Slice key, ushort? expectedVersion, ushort nodeVersion, TreeActionType actionType)
		{
			if (expectedVersion.HasValue && nodeVersion != expectedVersion.Value)
				throw new ConcurrencyException(string.Format("Cannot {0} '{1}' to '{4}' tree. Version mismatch. Expected: {2}. Actual: {3}.", actionType.ToString().ToLowerInvariant(), key, expectedVersion.Value, nodeVersion, Name));
		}


		private void CheckConcurrency(Slice key, Slice value, ushort? expectedVersion, ushort nodeVersion, TreeActionType actionType)
		{
			if (expectedVersion.HasValue && nodeVersion != expectedVersion.Value)
				throw new ConcurrencyException(string.Format("Cannot {0} value '{5}' to key '{1}' to '{4}' tree. Version mismatch. Expected: {2}. Actual: {3}.", actionType.ToString().ToLowerInvariant(), key, expectedVersion.Value, nodeVersion, Name, value));
		}

		private enum TreeActionType
		{
			Add,
			Delete
		}

		internal Tree Clone(Transaction tx)
		{
			return new Tree(tx, _cmp, _state.Clone()) { Name = Name };
		}

		private bool TryOverwriteOverflowPages(TreeMutableState treeState, NodeHeader* updatedNode,
													  Slice key, int len, ushort? version, out byte* pos)
		{
			if (updatedNode->Flags == NodeFlags.PageRef &&
				_tx.Id <= _tx.Environment.OldestTransaction) // ensure MVCC - do not overwrite if there is some older active transaction that might read those overflows
			{
				var overflowPage = _tx.GetReadOnlyPage(updatedNode->PageNumber);

				if (len <= overflowPage.OverflowSize)
				{
					CheckConcurrency(key, version, updatedNode->Version, TreeActionType.Add);

					if (updatedNode->Version == ushort.MaxValue)
						updatedNode->Version = 0;
					updatedNode->Version++;

					var availableOverflows = _tx.DataPager.GetNumberOfOverflowPages(overflowPage.OverflowSize);

					var requestedOverflows = _tx.DataPager.GetNumberOfOverflowPages(len);

					var overflowsToFree = availableOverflows - requestedOverflows;

					for (int i = 0; i < overflowsToFree; i++)
					{
						_tx.FreePage(overflowPage.PageNumber + requestedOverflows + i);
					}

					treeState.OverflowPages -= overflowsToFree;
					treeState.PageCount -= overflowsToFree;

					overflowPage.OverflowSize = len;

					pos = overflowPage.Base + Constants.PageHeaderSize;
					return true;
				}
			}
			pos = null;
			return false;
		}
	}
}

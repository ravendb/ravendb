using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.IO;
using System.Linq;
using Voron.Debugging;
using Voron.Impl;
using Voron.Impl.FileHeaders;

namespace Voron.Trees
{
	using Voron.Exceptions;

	public unsafe class Tree
	{
		private TreeMutableState _state = new TreeMutableState();
		public string Name { get; set; }

		public TreeMutableState State
		{
			get { return _state; }
		}

		private readonly SliceComparer _cmp;

		private Tree(SliceComparer cmp, long root)
		{
			_cmp = cmp;
			_state.RootPageNumber = root;
		}

		private Tree(SliceComparer cmp, TreeMutableState state)
		{
			_cmp = cmp;
			_state = state;
		}


		public static Tree Open(Transaction tx, SliceComparer cmp, TreeRootHeader* header)
		{
			return new Tree(cmp, header->RootPageNumber)
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
			var tree = new Tree(cmp, newRootPage.PageNumber)
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

		public void Add(Transaction tx, Slice key, Stream value, ushort? version = null)
		{
             
            if (value == null) throw new ArgumentNullException("value");
			if (value.Length > int.MaxValue)
				throw new ArgumentException("Cannot add a value that is over 2GB in size", "value");

            State.IsModified = true;
            
            var pos = DirectAdd(tx, key, (int)value.Length, version: version);

			using (var ums = new UnmanagedMemoryStream(pos, value.Length, value.Length, FileAccess.ReadWrite))
			{
				value.CopyTo(ums);
			}
		    
		}

		public void MultiDelete(Transaction tx, Slice key, Slice value, ushort? version = null)
		{
            State.IsModified = true;

			using (var cursor = tx.NewCursor(this))
			{
				var page = FindPageFor(tx, key, cursor);
				if (page == null || page.LastMatch != 0)
				{
					return; //nothing to delete - key not found
				}

				page = tx.ModifyPage(page.PageNumber, cursor);

				var item = page.GetNode(page.LastSearchPosition);

				if (item->Flags == NodeFlags.MultiValuePageRef) //multi-value tree exists
				{
					var tree = OpenOrCreateMultiValueTree(tx, key, item);

					tree.Delete(tx, value, version);

					if (tree.State.EntriesCount > 1)
						return;
					// convert back to simple key/val
					var iterator = tree.Iterate(tx);
					if(!iterator.Seek(Slice.BeforeAllKeys))
						throw new InvalidDataException("MultiDelete() failed : sub-tree is empty where it should not be, this is probably a Voron bug.");

					var dataToSave = iterator.CurrentKey;

					var ptr = DirectAdd(tx, key, dataToSave.Size);
					dataToSave.CopyTo(ptr);

					tx.TryRemoveMultiValueTree(this, key);
                    tx.FreePage(tree.State.RootPageNumber);
				}
				else //the regular key->value pattern
				{
					Delete(tx, key, version);
				}
			}
		}

		public void MultiAdd(Transaction tx, Slice key, Slice value, ushort? version = null)
		{
            
            if (value == null) throw new ArgumentNullException("value");
			if (value.Size > tx.DataPager.MaxNodeSize)
				throw new ArgumentException("Cannot add a value to child tree that is over " + tx.DataPager.MaxNodeSize + " bytes in size", "value");
			if (value.Size == 0)
				throw new ArgumentException("Cannot add empty value to child tree");

            State.IsModified = true;

			using (var cursor = tx.NewCursor(this))
			{
				var page = FindPageFor(tx, key, cursor);

				if (page == null || page.LastMatch != 0)
				{
					var ptr = DirectAdd(tx, key, value.Size, version: version);
					value.CopyTo(ptr);
					return;
				}

				page = tx.ModifyPage(page.PageNumber, cursor);

				var item = page.GetNode(page.LastSearchPosition);

				CheckConcurrency(key, version, item->Version, TreeActionType.Add);
				var existingValue = new Slice(DirectRead(tx, key), (ushort)item->DataSize);
				if (existingValue.Compare(value, _cmp) == 0)
					return; //nothing to do, the exact value is already there				

				if (item->Flags == NodeFlags.MultiValuePageRef)
				{
					var tree = OpenOrCreateMultiValueTree(tx, key, item);
					tree.DirectAdd(tx, value, 0);
				}
				else // need to turn to tree
				{
					var tree = Create(tx, _cmp, TreeFlags.MultiValue);
					var current = NodeHeader.GetData(tx, item);
					tree.DirectAdd(tx, current, 0);
					tree.DirectAdd(tx, value, 0);
					tx.AddMultiValueTree(this, key, tree);

					DirectAdd(tx, key, sizeof(TreeRootHeader), NodeFlags.MultiValuePageRef);
				}
			}
		}

		internal byte* DirectAdd(Transaction tx, Slice key, int len, NodeFlags nodeType = NodeFlags.Data, ushort? version = null)
		{
			Debug.Assert(nodeType == NodeFlags.Data || nodeType == NodeFlags.MultiValuePageRef);

			if (tx.Flags == (TransactionFlags.ReadWrite) == false)
				throw new ArgumentException("Cannot add a value in a read only transaction");

			if (key.Size > tx.DataPager.MaxNodeSize)
				throw new ArgumentException("Key size is too big, must be at most " + tx.DataPager.MaxNodeSize + " bytes, but was " + key.Size, "key");

			using (var cursor = tx.NewCursor(this))
			{

				var foundPage = FindPageFor(tx, key, cursor);

			    var page = tx.ModifyPage(foundPage.PageNumber, cursor);
                
                cursor.Update(cursor.Pages.First, page);

				ushort nodeVersion = 0;
				if (page.LastMatch == 0) // this is an update operation
				{
					RemoveLeafNode(tx, cursor, page, out nodeVersion);
				}
				else // new item should be recorded
				{
					State.EntriesCount++;
				}

				CheckConcurrency(key, version, nodeVersion, TreeActionType.Add);

				var lastSearchPosition = page.LastSearchPosition; // searching for overflow pages might change this
				byte* overFlowPos = null;
				var pageNumber = -1L;
				if (tx.DataPager.ShouldGoToOverflowPage(len))
				{
					pageNumber = WriteToOverflowPages(tx, State, len, out overFlowPos);
					len = -1;
					nodeType = NodeFlags.PageRef;
				}

				byte* dataPos;
				if (page.HasSpaceFor(key, len) == false)
				{
					var pageSplitter = new PageSplitter(tx, _cmp, key, len, pageNumber, nodeType, nodeVersion, cursor, State);
					dataPos = pageSplitter.Execute();

					DebugValidateTree(tx, State.RootPageNumber);
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
					page.DebugValidate(tx, _cmp, State.RootPageNumber);
				}
				if (overFlowPos != null)
					return overFlowPos;
				return dataPos;
			}
		}

		public bool SetAsMultiValueTreeRef(Transaction tx, Slice key)
		{
			using (var cursor = tx.NewCursor(this))
			{
				var foundPage = FindPageFor(tx, key, cursor);
				var page = tx.ModifyPage(foundPage.PageNumber, cursor);

				if (page.LastMatch != 0)
					return false; // not there

				var nodeHeader = page.GetNode(page.LastSearchPosition);
				if (nodeHeader->Flags == NodeFlags.MultiValuePageRef)
					return false;
				if (nodeHeader->Flags != NodeFlags.Data)
					throw new InvalidOperationException("Only data nodes can be set to MultiValuePageRef");
				nodeHeader->Flags = NodeFlags.MultiValuePageRef;
				return true;
			}
		}

		private long WriteToOverflowPages(Transaction tx, TreeMutableState txInfo, int overflowSize, out byte* dataPos)
		{
			var numberOfPages = tx.DataPager.GetNumberOfOverflowPages(overflowSize);
			var overflowPageStart = tx.AllocatePage(numberOfPages);
			overflowPageStart.Flags = PageFlags.Overflow;
			overflowPageStart.OverflowSize = overflowSize;
			dataPos = overflowPageStart.Base + Constants.PageHeaderSize;
			txInfo.OverflowPages += numberOfPages;
			txInfo.PageCount += numberOfPages;
			return overflowPageStart.PageNumber;
		}

		private void RemoveLeafNode(Transaction tx, Cursor cursor, Page page, out ushort nodeVersion)
		{
			var node = page.GetNode(page.LastSearchPosition);
			nodeVersion = node->Version;
			if (node->Flags == (NodeFlags.PageRef)) // this is an overflow pointer
			{
				var overflowPage = tx.GetReadOnlyPage(node->PageNumber);
				var numberOfPages = tx.DataPager.GetNumberOfOverflowPages(overflowPage.OverflowSize);
				for (int i = 0; i < numberOfPages; i++)
				{
					tx.FreePage(overflowPage.PageNumber + i);
				}

				State.OverflowPages -= numberOfPages;
				State.PageCount -= numberOfPages;
			}
			page.RemoveNode(page.LastSearchPosition);
		}

		[Conditional("VALIDATE")]
		public void DebugValidateTree(Transaction tx, long rootPageNumber)
		{
			var pages = new HashSet<long>();
			var stack = new Stack<Page>();
			var root = tx.GetReadOnlyPage(rootPageNumber);
			stack.Push(root);
			pages.Add(rootPageNumber);
			while (stack.Count > 0)
			{
				var p = stack.Pop();
				if (p.NumberOfEntries == 0 && p != root)
				{
					DebugStuff.RenderAndShow(tx, rootPageNumber, 1);
					throw new InvalidOperationException("The page " + p.PageNumber + " is empty");

				}
				p.DebugValidate(tx, _cmp, rootPageNumber);
				if (p.IsBranch == false)
					continue;
				for (int i = 0; i < p.NumberOfEntries; i++)
				{
					var page = p.GetNode(i)->PageNumber;
					if (pages.Add(page) == false)
					{
						DebugStuff.RenderAndShow(tx, rootPageNumber, 1);
						throw new InvalidOperationException("The page " + page + " already appeared in the tree!");
					}
					stack.Push(tx.GetReadOnlyPage(page));
				}
			}
		}

		public Page FindPageFor(Transaction tx, Slice key, Cursor cursor)
		{
			var p = tx.GetReadOnlyPage(State.RootPageNumber);
			cursor.Push(p);
			while (p.Flags == (PageFlags.Branch))
			{
				int nodePos;
				if (key.Options == SliceOptions.BeforeAllKeys)
				{
					p.LastSearchPosition = nodePos = 0;
				}
				else if (key.Options == SliceOptions.AfterAllKeys)
				{
					p.LastSearchPosition = nodePos = (ushort)(p.NumberOfEntries - 1);
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
					}
					else
					{
						nodePos = (ushort)(p.LastSearchPosition - 1);
					}

				}

				var node = p.GetNode(nodePos);
				p = tx.GetReadOnlyPage(node->PageNumber);
				cursor.Push(p);
			}

			if (p.IsLeaf == false)
				throw new DataException("Index points to a non leaf page");

			p.NodePositionFor(key, _cmp); // will set the LastSearchPosition

			return p;
		}

		internal static Page NewPage(Transaction tx, PageFlags flags, int num)
		{
			var page = tx.AllocatePage(num);
			page.Flags = flags;

			return page;
		}

		public void Delete(Transaction tx, Slice key, ushort? version = null)
		{
			if (tx.Flags == (TransactionFlags.ReadWrite) == false) throw new ArgumentException("Cannot delete a value in a read only transaction");

            State.IsModified = true;

			using (var cursor = tx.NewCursor(this))
			{
				var page = FindPageFor(tx, key, cursor);

				page.NodePositionFor(key, _cmp);
				if (page.LastMatch != 0)
					return; // not an exact match, can't delete

				page = tx.ModifyPage(page.PageNumber, cursor);

				State.EntriesCount--;
				ushort nodeVersion;
				RemoveLeafNode(tx, cursor, page, out nodeVersion);

				CheckConcurrency(key, version, nodeVersion, TreeActionType.Delete);

				var treeRebalancer = new TreeRebalancer(tx, this, _cmp);
				var changedPage = page;
				while (changedPage != null)
				{
					changedPage = treeRebalancer.Execute(cursor, changedPage);
				}

				page.DebugValidate(tx, _cmp, State.RootPageNumber);
			}
		}

		public TreeIterator Iterate(Transaction tx, WriteBatch writeBatch = null)
		{
			return new TreeIterator(this, tx, _cmp);
		}

		public ReadResult Read(Transaction tx, Slice key)
		{
			using (var cursor = tx.NewCursor(this))
			{
				var p = FindPageFor(tx, key, cursor);
				var node = p.Search(key, _cmp);

				if (node == null)
					return null;

				var item = new Slice(node);

				return item.Compare(key, _cmp) == 0 ?
					new ReadResult(NodeHeader.Stream(tx, node), node->Version) : null;
			}
		}

		public int GetDataSize(Transaction tx, Slice key)
		{
			using (var cursor = tx.NewCursor(this))
			{
				var p = FindPageFor(tx, key, cursor);
				var node = p.Search(key, _cmp);

				if (node == null || new Slice(node).Compare(key, _cmp) != 0)
					return -1;

				return node->DataSize;
			}

		}

		public ushort ReadVersion(Transaction tx, Slice key)
		{
			using (var cursor = tx.NewCursor(this))
			{
				var p = FindPageFor(tx, key, cursor);
				var node = p.Search(key, _cmp);

				if (node == null || new Slice(node).Compare(key, _cmp) != 0)
					return 0;

				return node->Version;
			}
		}

		public IIterator MultiRead(Transaction tx, Slice key)
		{
			using (var cursor = tx.NewCursor(this))
			{
				var page = FindPageFor(tx, key, cursor);

				if (page == null || page.LastMatch != 0)
				{
					return new EmptyIterator();
				}

				var item = page.Search(key, _cmp);

				var fetchedNodeKey = new Slice(item);
				if (fetchedNodeKey.Compare(key, _cmp) != 0)
				{
					throw new InvalidDataException("Was unable to retrieve the correct node. Data corruption possible");
				}

				if (item->Flags == NodeFlags.MultiValuePageRef)
				{
					var tree = OpenOrCreateMultiValueTree(tx, key, item);

					return tree.Iterate(tx);
				}

				return new SingleEntryIterator(_cmp, item, tx);
			}
		}

		internal byte* DirectRead(Transaction tx, Slice key)
		{
			using (var cursor = tx.NewCursor(this))
			{
				var p = FindPageFor(tx, key, cursor);
				var node = p.Search(key, _cmp);

				if (node == null)
					return null;

				var item1 = new Slice(node);

				if (item1.Compare(key, _cmp) != 0)
					return null;

				if (node->Flags == (NodeFlags.PageRef))
				{
					var overFlowPage = tx.GetReadOnlyPage(node->PageNumber);
					return overFlowPage.Base + Constants.PageHeaderSize;
				}

				return (byte*)node + node->KeySize + Constants.NodeHeaderSize;
			}
		}

		internal void SetState(TreeMutableState state)
		{
			_state = state;
		}

		public List<long> AllPages(Transaction tx)
		{
			var results = new List<long>();
			var stack = new Stack<Page>();
			var root = tx.GetReadOnlyPage(State.RootPageNumber);
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
						stack.Push(tx.GetReadOnlyPage(pageNumber));
					}
					else if (node->Flags == NodeFlags.PageRef)
					{
						// This is an overflow page
						var overflowPage = tx.GetReadOnlyPage(pageNumber);
						var numberOfPages = tx.DataPager.GetNumberOfOverflowPages(overflowPage.OverflowSize);
						for (long j = 0; j < numberOfPages; ++j)
							results.Add(overflowPage.PageNumber + j);
					}
					else if (node->Flags == NodeFlags.MultiValuePageRef)
					{
						var childTreeHeader = (TreeRootHeader*)((byte*)node + node->KeySize + Constants.NodeHeaderSize);

						results.Add(childTreeHeader->RootPageNumber);

						// this is a multi value
						var tree = OpenOrCreateMultiValueTree(tx, new Slice(node), node);
						results.AddRange(tree.AllPages(tx));
					}
				}
			}
			return results;
		}

		public override string ToString()
		{
			return Name + " " + State.EntriesCount;
		}

		private Tree OpenOrCreateMultiValueTree(Transaction tx, Slice key, NodeHeader* item)
		{
			Tree tree;
			if (tx.TryGetMultiValueTree(this, key, out tree))
				return tree;

		    var childTreeHeader =
		        (TreeRootHeader*) ((byte*) item + item->KeySize + Constants.NodeHeaderSize);
            tree = childTreeHeader != null ?
				Open(tx, _cmp, childTreeHeader) :
				Create(tx, _cmp);

			tx.AddMultiValueTree(this, key, tree);
			return tree;
		}

		private static void CheckConcurrency(Slice key, ushort? expectedVersion, ushort nodeVersion, TreeActionType actionType)
		{
			if (expectedVersion.HasValue && nodeVersion != expectedVersion.Value)
				throw new ConcurrencyException(string.Format("Cannot {0} '{1}'. Version mismatch. Expected: {2}. Actual: {3}.", actionType.ToString().ToLowerInvariant(), key, expectedVersion.Value, nodeVersion));
		}

		public bool IsMultiValueTree { get; set; }

		private enum TreeActionType
		{
			Add,
			Delete
		}

		public Tree Clone()
		{
			return new Tree(_cmp, _state.Clone()){ Name = Name};
		}
	}
}
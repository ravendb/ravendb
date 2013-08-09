using System;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.IO;
using System.Linq;
using Nevar.Impl;
using Nevar.Impl.FileHeaders;

namespace Nevar.Trees
{
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
							Flags = header->Flags
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
							Flags = flags
						}
				};
			var txInfo = tx.GetTreeInformation(tree);
			txInfo.RecordNewPage(newRootPage, 1);
			return tree;
		}

		public void Add(Transaction tx, Slice key, Stream value)
		{
			if (value == null) throw new ArgumentNullException("value");
			if (value.Length > int.MaxValue)
				throw new ArgumentException("Cannot add a value that is over 2GB in size", "value");
			var pos = DirectAdd(tx, key, (int)value.Length);

			using (var ums = new UnmanagedMemoryStream(pos, value.Length, value.Length, FileAccess.ReadWrite))
			{
				value.CopyTo(ums);
			}
		}

		internal byte* DirectAdd(Transaction tx, Slice key, int len)
		{
			if (tx.Flags == (TransactionFlags.ReadWrite) == false)
				throw new ArgumentException("Cannot add a value in a read only transaction");

			if (key.Size > tx.Pager.MaxNodeSize)
				throw new ArgumentException("Key size is too big, must be at most " + tx.Pager.MaxNodeSize + " bytes, but was " + key.Size, "key");


			using (var cursor = tx.NewCursor(this))
			{
				var txInfo = tx.GetTreeInformation(this);

				FindPageFor(tx, key, cursor);

				var page = tx.ModifyCursor(this, cursor);

				if (page.LastMatch == 0) // this is an update operation
				{
					RemoveLeafNode(tx, cursor, page);
				}
				else // new item should be recorded
				{
					txInfo.State.EntriesCount++;
				}

				var lastSearchPosition = page.LastSearchPosition; // searching for overflow pages might change this
				byte* overFlowPos = null;
				var pageNumber = -1L;
				if (ShouldGoToOverflowPage(tx, len))
				{
					pageNumber = WriteToOverflowPages(tx, txInfo, len, out overFlowPos);
					len = -1;
				}

				byte* dataPos;
				if (page.HasSpaceFor(key, len) == false)
				{
					var pageSplitter = new PageSplitter(tx, _cmp, key, len, pageNumber, cursor, txInfo);
					dataPos = pageSplitter.Execute();
					DebugValidateTree(tx, txInfo.RootPageNumber);
				}
				else
				{
					dataPos = page.AddNode(lastSearchPosition, key, len, pageNumber);
					page.DebugValidate(tx, _cmp, txInfo.RootPageNumber);
				}
				if (overFlowPos != null)
					return overFlowPos;
				return dataPos;
			}
		}

		private long WriteToOverflowPages(Transaction tx, TreeDataInTransaction txInfo, int overflowSize, out byte* dataPos)
		{
			var numberOfPages = GetNumberOfOverflowPages(tx, overflowSize);
			var overflowPageStart = tx.AllocatePage(numberOfPages);
			overflowPageStart.OverflowSize = numberOfPages;
			overflowPageStart.Flags = PageFlags.Overlfow;
			overflowPageStart.OverflowSize = overflowSize;
			dataPos = overflowPageStart.Base + Constants.PageHeaderSize;
			txInfo.State.OverflowPages += numberOfPages;
			txInfo.State.PageCount += numberOfPages;
			return overflowPageStart.PageNumber;
		}

		private int GetNumberOfOverflowPages(Transaction tx, int overflowSize)
		{
			return (tx.Environment.PageSize - 1 + overflowSize) / (tx.Environment.PageSize) + 1;
		}

		private bool ShouldGoToOverflowPage(Transaction tx, int len)
		{
			return len + Constants.PageHeaderSize > tx.Pager.MaxNodeSize;
		}

		private void RemoveLeafNode(Transaction tx, Cursor cursor, Page page)
		{
			var node = page.GetNode(page.LastSearchPosition);
			if (node->Flags == (NodeFlags.PageRef)) // this is an overflow pointer
			{
				tx.ModifyCursor(this, cursor);
				var overflowPage = tx.GetReadOnlyPage(node->PageNumber);
				var numberOfPages = GetNumberOfOverflowPages(tx, overflowPage.OverflowSize);
				for (int i = 0; i < numberOfPages; i++)
				{
					tx.FreePage(overflowPage.PageNumber + i);
				}
				var txInfo = tx.GetTreeInformation(this);

				txInfo.State.OverflowPages -= numberOfPages;
				txInfo.State.PageCount -= numberOfPages;
			}
			page.RemoveNode(page.LastSearchPosition);
		}

		[Conditional("VALIDATE")]
		private void DebugValidateTree(Transaction tx, long rootPageNumber)
		{
			var stack = new Stack<Page>();
			var root = tx.GetReadOnlyPage(rootPageNumber);
			stack.Push(root);
			while (stack.Count > 0)
			{
				var p = stack.Pop();
				p.DebugValidate(tx, _cmp, rootPageNumber);
				if (p.IsBranch == false)
					continue;
				for (int i = 0; i < p.NumberOfEntries; i++)
				{
					stack.Push(tx.GetReadOnlyPage(p.GetNode(i)->PageNumber));
				}
			}
		}

		public Page FindPageFor(Transaction tx, Slice key, Cursor cursor)
		{
			var treeDataInTransaction = tx.GetTreeInformation(this);
			var p = tx.GetReadOnlyPage(treeDataInTransaction.RootPageNumber);
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

		public void Delete(Transaction tx, Slice key)
		{
			if (tx.Flags == (TransactionFlags.ReadWrite) == false) throw new ArgumentException("Cannot delete a value in a read only transaction");

			var txInfo = tx.GetTreeInformation(this);
			using (var cursor = tx.NewCursor(this))
			{
				var page = FindPageFor(tx, key, cursor);

				page.NodePositionFor(key, _cmp);
				if (page.LastMatch != 0)
					return; // not an exact match, can't delete

				page = tx.ModifyCursor(this, cursor);

				txInfo.State.EntriesCount--;
				RemoveLeafNode(tx, cursor, page);
				var treeRebalancer = new TreeRebalancer(tx, txInfo, _cmp);
				var changedPage = page;
				while (changedPage != null)
				{
					changedPage = treeRebalancer.Execute(cursor, changedPage);
				}

				page.DebugValidate(tx, _cmp, txInfo.RootPageNumber);
			}
		}

		public Iterator Iterate(Transaction tx)
		{
			return new Iterator(this, tx, _cmp);
		}

		public Stream Read(Transaction tx, Slice key)
		{
			using (var cursor = tx.NewCursor(this))
			{
				var p = FindPageFor(tx, key, cursor);
				var node = p.Search(key, _cmp);

				if (node == null)
					return null;

				var item = new Slice(node);

				if (item.Compare(key, _cmp) != 0)
					return null;
				return NodeHeader.Stream(tx, node);
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

		public override string ToString()
		{
			return Name + " " + State.EntriesCount;
		}

		internal void SetState(TreeMutableState state)
		{
			_state = state;
		}
	}
}
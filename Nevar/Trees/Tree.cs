using System;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.IO;
using Nevar.Debugging;
using Nevar.Impl;

namespace Nevar.Trees
{
	public unsafe class Tree
	{
		public int BranchPages;
		public int LeafPages;
		public int OverflowPages;
		public int Depth;
		public int PageCount;

		private readonly SliceComparer _cmp;

		public Page Root;

		public Tree(SliceComparer cmp, Page root)
		{
			_cmp = cmp;
			Root = root;
		}

		public static Tree Create(Transaction tx, SliceComparer cmp)
		{
			var newRootPage = NewPage(tx, PageFlags.Leaf, 1);
			var tree = new Tree(cmp, newRootPage) { Depth = 1 };
			var cursor = tx.GetCursor(tree);
			cursor.RecordNewPage(newRootPage, 1);
			return tree;
		}

		public void Add(Transaction tx, Slice key, Stream value)
		{
			if (value == null) throw new ArgumentNullException("value");
			if (value.Length > int.MaxValue) throw new ArgumentException("Cannot add a value that is over 2GB in size", "value");

			var cursor = tx.GetCursor(this);

			var page = FindPageFor(tx, key, cursor);

			if (page.LastMatch == 0) // this is an update operation
			{
				RemoveLeafNode(tx, cursor, page);
			}

			var pageNumber = -1;
			if (ShouldGoToOverflowPage(value))
			{
				pageNumber = WriteToOverflowPages(tx, cursor, value);
				value = null;
			}

			if (page.HasSpaceFor(key, value) == false)
			{
				new PageSplitter(tx, _cmp, key, value, pageNumber, cursor).Execute();
				DebugValidateTree(tx, cursor.Root);
				return;
			}

			page.AddNode(page.LastSearchPosition, key, value, pageNumber);

			page.DebugValidate(_cmp);
		}

		private static int WriteToOverflowPages(Transaction tx, Cursor cursor, Stream value)
		{
			var overflowSize = (int)value.Length;
			var numberOfPages = GetNumberOfOverflowPages(overflowSize);
			var overflowPageStart = tx.AllocatePage(numberOfPages);
			overflowPageStart.OverflowSize = numberOfPages;
			overflowPageStart.Flags = PageFlags.Overlfow;
			overflowPageStart.OverflowSize = overflowSize;
			using (var ums = new UnmanagedMemoryStream(overflowPageStart.Base + Constants.PageHeaderSize,
				value.Length, value.Length, FileAccess.ReadWrite))
			{
				value.CopyTo(ums);
			}
			cursor.OverflowPages += numberOfPages;
			cursor.PageCount += numberOfPages;
			return overflowPageStart.PageNumber;
		}

		private static int GetNumberOfOverflowPages(int overflowSize)
		{
			return (Constants.PageSize - 1 + overflowSize) / (Constants.PageSize) + 1;
		}

		private bool ShouldGoToOverflowPage(Stream value)
		{
			return value.Length + Constants.PageHeaderSize > Constants.MaxNodeSize;
		}

		private static void RemoveLeafNode(Transaction tx, Cursor cursor, Page page)
		{
			var node = page.GetNode(page.LastSearchPosition);
			if (node->Flags.HasFlag(NodeFlags.PageRef)) // this is an overflow pointer
			{
				var overflowPage = tx.GetPage(node->PageNumber);
				var numberOfPages = GetNumberOfOverflowPages(overflowPage.OverflowSize);
				for (int i = 0; i < numberOfPages; i++)
				{
					tx.FreePage(tx.GetPage(node->PageNumber + i));
				}
				cursor.OverflowPages -= numberOfPages;
				cursor.PageCount -= numberOfPages;
			}
			page.RemoveNode(page.LastSearchPosition);
		}

		[Conditional("DEBUG")]
		private void DebugValidateTree(Transaction tx, Page root)
		{
			var stack = new Stack<Page>();
			stack.Push(root);
			while (stack.Count > 0)
			{
				var p = stack.Pop();
				p.DebugValidate(_cmp);
				if (p.IsBranch == false)
					continue;
				for (int i = 0; i < p.NumberOfEntries; i++)
				{
					stack.Push(tx.GetPage(p.GetNode(i)->PageNumber));
				}
			}
		}



		public Page FindPageFor(Transaction tx, Slice key, Cursor cursor)
		{
			var p = cursor.Root;
			cursor.Push(p);
			while (p.Flags.HasFlag(PageFlags.Branch))
			{
				int nodePos;
				if (key.Options == SliceOptions.BeforeAllKeys)
				{
					p.LastSearchPosition = nodePos = 0;
				}
				else if (key.Options == SliceOptions.AfterAllKeys)
				{
					p.LastSearchPosition  = nodePos = (ushort)(p.NumberOfEntries - 1);
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
				p = tx.GetPage(node->PageNumber);
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
			var cursor = tx.GetCursor(this);

			var page = FindPageFor(tx, key, cursor);

			page.NodePositionFor(key, _cmp);
			if (page.LastMatch != 0)
				return; // not an exact match, can't delete
			RemoveLeafNode(tx, cursor, page);

			var treeRebalancer = new TreeRebalancer(tx);

			var changedPage = page;
			while (changedPage != null)
			{
				changedPage = treeRebalancer.Execute(cursor, changedPage);
			}

			page.DebugValidate(_cmp);
		}

		public List<Slice> KeysAsList(Transaction tx)
		{
			var l = new List<Slice>();
			AddKeysToListInOrder(tx, l, Root);
			return l;
		}

		private void AddKeysToListInOrder(Transaction tx, List<Slice> l, Page page)
		{
			for (int i = 0; i < page.NumberOfEntries; i++)
			{
				var node = page.GetNode(i);
				if (page.IsBranch)
				{
					var p = tx.GetPage(node->PageNumber);
					AddKeysToListInOrder(tx, l, p);
				}
				else
				{
					l.Add(new Slice(node));
				}
			}
		}

		public Iterator Iterage(Transaction tx)
		{
			return new Iterator(this, tx, _cmp);
		}

		public Stream Read(Transaction tx, Slice key)
		{
			var cursor = tx.GetCursor(this);
			var p = FindPageFor(tx, key, cursor);
			var node = p.Search(key, _cmp);

			if (node == null)
				return null;

			var item1 = new Slice(node);

			if (item1.Compare(key, _cmp) != 0)
				return null;
			if (node->Flags.HasFlag(NodeFlags.PageRef))
			{
				var overFlowPage = tx.GetPage(node->PageNumber);
				return new UnmanagedMemoryStream(overFlowPage.Base + Constants.PageHeaderSize, overFlowPage.OverflowSize, overFlowPage.OverflowSize, FileAccess.Read);
			}
			return new UnmanagedMemoryStream((byte*)node + node->KeySize + Constants.NodeHeaderSize, node->DataSize, node->DataSize, FileAccess.Read);
		}
	}

	public unsafe class Iterator
	{
		private readonly Tree _tree;
		private readonly Transaction _tx;
		private readonly SliceComparer _cmp;
		private readonly Cursor _cursor;
		private Page _currentPage;


		public Iterator(Tree tree, Transaction tx, SliceComparer cmp)
		{
			_tree = tree;
			_tx = tx;
			_cmp = cmp;
			_cursor = _tx.GetCursor(tree);
		}

		public bool Seek(Slice key)
		{
			_currentPage = _tree.FindPageFor(_tx, key, _cursor);
			_cursor.Pop();
			var node = _currentPage.Search(key, _cmp);
			return node != null;
		}

		public NodeHeader* Current
		{
			get
			{
				if (_currentPage == null)
					throw new InvalidOperationException("No current page was set");
				return _currentPage.GetNode(_currentPage.LastSearchPosition);
			}
		}

		public bool MoveNext()
		{
			while (true)
			{
				_currentPage.LastSearchPosition++;
				if (_currentPage.LastSearchPosition < _currentPage.NumberOfEntries)
				{
					// run out of entries, need to select the next page...
					if (_currentPage.IsBranch)
					{
						_cursor.Push(_currentPage);
						var node = _currentPage.GetNode(_currentPage.LastSearchPosition);
						_currentPage = _tx.GetPage(node->PageNumber);
						_currentPage.LastSearchPosition = 0;
					}
					return true;// there is another entry in this page
				}
				if (_cursor.Pages.Count == 0)
					break;
				_currentPage = _cursor.Pop();
			}
			_currentPage = null;
			return false;
		}
	}
}
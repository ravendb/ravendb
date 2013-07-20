using System;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.IO;

namespace Nevar
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

		private Tree(SliceComparer cmp, Page root)
		{
			_cmp = cmp;
			Root = root;
		}

		public static Tree CreateOrOpen(Transaction tx, int root, SliceComparer cmp)
		{
			if (root != -1)
			{
				return new Tree(cmp, tx.GetPage(root));
			}

			// need to create the root
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
				page.RemoveNode(page.LastSearchPosition);

			if (page.HasSpaceFor(key, value) == false)
			{
				new PageSplitter(tx, this, key, value, -1, cursor).Execute();
				DebugValidateTree(tx, cursor.Root);
				return;
			}

			page.AddNode(page.LastSearchPosition, key, value, 0);

			page.DebugValidate(_cmp);
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


		/// <summary>
		/// For leaf pages, check the split point based on what
		///	fits where, since otherwise adding the node can fail.
		///	
		///	This check is only needed when the data items are
		///	relatively large, such that being off by one will
		///	make the difference between success or failure.
		///	
		///	It's also relevant if a page happens to be laid out
		///	such that one half of its nodes are all "small" and
		///	the other half of its nodes are "large." If the new
		///	item is also "large" and falls on the half with
		///	"large" nodes, it also may not fit.
		/// </summary>
		private static int AdjustSplitPosition(Slice key, Stream value, Page page, int currentIndex, int splitIndex,
													  ref bool newPosition)
		{
			var nodeSize = SizeOf.NodeEntry(key, value) + Constants.NodeOffsetSize;
			if (page.NumberOfEntries >= 20 && nodeSize <= Constants.PageMaxSpace / 16)
			{
				return splitIndex;
			}

			int pageSize = nodeSize;
			if (currentIndex <= splitIndex)
			{
				newPosition = false;
				for (int i = 0; i < splitIndex; i++)
				{
					var node = page.GetNode(i);
					pageSize += node->GetNodeSize();
					pageSize += pageSize & 1;
					if (pageSize > Constants.PageMaxSpace)
					{
						if (i <= currentIndex)
						{
							if (i < currentIndex)
								newPosition = true;
							return currentIndex;
						}
						return (ushort)i;
					}
				}
			}
			else
			{
				for (int i = page.NumberOfEntries - 1; i >= splitIndex; i--)
				{
					var node = page.GetNode(i);
					pageSize += node->GetNodeSize();
					pageSize += pageSize & 1;
					if (pageSize > Constants.PageMaxSpace)
					{
						if (i >= currentIndex)
						{
							newPosition = false;
							return currentIndex;
						}
						return (ushort)(i + 1);
					}
				}
			}
			return splitIndex;
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
					nodePos = 0;
				}
				else if (key.Options == SliceOptions.AfterAllKeys)
				{
					nodePos = (ushort)(p.NumberOfEntries - 1);
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

		private static Page NewPage(Transaction tx, PageFlags flags, int num)
		{
			var page = tx.AllocatePage(num);

			page.Flags = flags;

			return page;
		}

		public void Delete(Transaction tx, Slice key)
		{
			var cursor = tx.GetCursor(this);

			var page = FindPageFor(tx, key, cursor);

			var pos = page.NodePositionFor(key, _cmp);
			if (page.LastMatch != 0)
				return; // not an exact match, can't delete
			page.RemoveNode(pos);

			var treeRebalancer = new TreeRebalancer(tx);

			var changedPage = page;
			while (changedPage != null)
			{
				changedPage = treeRebalancer.Execute(cursor, changedPage);
			}

			page.DebugValidate(_cmp);
		}

		public class TreeRebalancer
		{
			private readonly Transaction _tx;

			public TreeRebalancer(Transaction tx)
			{
				_tx = tx;
			}

			public Page Execute(Cursor cursor, Page page)
			{
				if (cursor.Pages.Count <= 1) // the root page
				{
					RebalanceRoot(cursor, page);
					return null;
				}

				var parentPage = cursor.ParentPage;
				if (page.NumberOfEntries == 0)
				{
					// empty page, just delete it and fixup parent
					parentPage.RemoveNode(parentPage.LastSearchPosition);
					cursor.Pop();
					return parentPage;
				}

				var minKeys = page.IsBranch ? 2 : 1;
				if (page.SizeUsed >= Constants.PageMinSpace &&
				    page.NumberOfEntries >= minKeys)
					return null; // above space/keys thresholds


				Debug.Assert(parentPage.NumberOfEntries >= 2); // if we have less than 2 entries in the parent, the tree is invalid

				Page sibling;
				if (parentPage.LastSearchPosition == 0) // we are the left most item
				{
					sibling = _tx.GetPage(parentPage.GetNode(1)->PageNumber);
					sibling.LastSearchPosition = 0;
					page.LastSearchPosition = page.NumberOfEntries;
					parentPage.LastSearchPosition = 1;
				}
				else // there is at least 1 page to our left
				{
					sibling = _tx.GetPage(parentPage.GetNode(parentPage.LastSearchPosition - 1)->PageNumber);
					sibling.LastSearchPosition = sibling.NumberOfEntries - 1;
					page.LastSearchPosition = 0;
					parentPage.LastSearchPosition--;

				}

				minKeys = sibling.IsBranch ? 2 : 1; // branch must have at least 2 keys
				if (sibling.SizeUsed > Constants.PageMinSpace && sibling.NumberOfEntries > minKeys)
				{
					// neighbor is over the min size and has enough key, can move just one key to  the current page
					MoveNode(parentPage, sibling, page);
					return parentPage;
				}
				return null;
			}

			private void MoveNode(Page parentPage, Page from, Page to)
			{
				var fromKey = GetCurrentKeyFrom(from);

				var fromNode = from.GetNode(from.LastSearchPosition);
				Stream val = null;
				int pageNum;
				if (fromNode->Flags.HasFlag(NodeFlags.Data))
				{
					val = new UnmanagedMemoryStream(from.Base + from.KeysOffsets[from.LastSearchPosition] + Constants.NodeHeaderSize, fromNode->DataSize);
					pageNum = -1;
				}
				else
				{
					pageNum = fromNode->PageNumber;
				}
				using (val)
				{
					to.AddNode(to.LastSearchPosition, fromKey, val, pageNum);
				}

				to.RemoveNode(to.LastSearchPosition);

				parentPage.RemoveNode(parentPage.LastSearchPosition);
				var toKey = GetCurrentKeyFrom(from); // get the next smallest key it has
				parentPage.AddNode(parentPage.LastSearchPosition, toKey, null, from.PageNumber);
			}

			private Slice GetCurrentKeyFrom(Page page)
			{
				var node = page.GetNode(page.LastSearchPosition);
				var key = new Slice(node);
				while (key.Size == 0)
				{
					Debug.Assert(page.LastSearchPosition == 0 && page.IsBranch);
					page = _tx.GetPage(node->PageNumber);
					node = page.GetNode(0);
					key.Set(node);
				}
				return key;
			}

			private void RebalanceRoot(Cursor cursor, Page page)
			{
				if (page.NumberOfEntries == 0)
					return; // nothing to do 
				if (!page.IsBranch || page.NumberOfEntries > 1)
				{
					return; // cannot do anything here
				}
				// in this case, we have a root pointer with just one pointer, we can just swap it out
				var node = page.GetNode(0);
				Debug.Assert(node->Flags.HasFlag(NodeFlags.PageRef));
				cursor.Root = _tx.GetPage(node->PageNumber);
				cursor.LeafPages = 1;
				cursor.BranchPages = 0;
				cursor.Depth = 1;
				cursor.PageCount = 1;
				cursor.Pop();
				cursor.Push(cursor.Root);

				_tx.FreePage(page);
			}
		}

		public class PageSplitter
		{
			private readonly Transaction _tx;
			private readonly Tree _parent;
			private readonly Slice _newKey;
			private readonly Stream _value;
			private readonly int _pageNumber;
			private readonly Cursor _cursor;
			private readonly Page _page;
			private Page _parentPage;

			public PageSplitter(Transaction tx, Tree parent, Slice newKey, Stream value, int pageNumber, Cursor cursor)
			{
				_tx = tx;
				_parent = parent;
				_newKey = newKey;
				_value = value;
				_pageNumber = pageNumber;
				_cursor = cursor;
				_page = _cursor.Pop();
			}

			public void Execute()
			{
				var rightPage = NewPage(_tx, _page.Flags, 1);
				_cursor.RecordNewPage(_page, 1);
				rightPage.Flags = _page.Flags;
				if (_cursor.Pages.Count == 0) // we need to do a root split
				{
					var newRootPage = NewPage(_tx, PageFlags.Branch, 1);
					_cursor.Push(newRootPage);
					_cursor.Root = newRootPage;
					_cursor.Depth++;
					_cursor.RecordNewPage(newRootPage, 1);

					// now add implicit left page
					newRootPage.AddNode(0, Slice.BeforeAllKeys, null, _page.PageNumber);
					_parentPage = newRootPage;
					_parentPage.LastSearchPosition++;
				}
				else
				{
					// we already popped the page, so the current one on the stack is what the parent of the page
					_parentPage = _cursor.CurrentPage;
				}

				if (_page.LastSearchPosition >= _page.NumberOfEntries)
				{
					// when we get a split at the end of the page, we take that as a hint that the user is doing 
					// sequential inserts, at that point, we are going to keep the current page as is and create a new 
					// page, this will allow us to do minimal amount of work to get the best density

					AddSeperatorToParentPage(rightPage, _newKey);
					rightPage.AddNode(0, _newKey, _value, _pageNumber);
					_cursor.Push(rightPage);
					return;
				}

				SplitPageInHalf(rightPage);
			}

			private void SplitPageInHalf(Page rightPage)
			{
				var currentIndex = _page.LastSearchPosition;
				var newPosition = true;
				var splitIndex = _page.NumberOfEntries / 2;
				if (currentIndex < splitIndex)
					newPosition = false;

				if (_page.IsLeaf)
				{
					splitIndex = AdjustSplitPosition(_newKey, _value, _page, currentIndex, splitIndex, ref newPosition);
				}

				// here we the current key is the separator key and can go either way, so 
				// use newPosition to decide if it stays on the left node or moves to the right
				Slice seperatorKey;
				if (currentIndex == splitIndex && newPosition)
				{
					seperatorKey = _newKey;
				}
				else
				{
					var node = _page.GetNode(splitIndex);
					seperatorKey = new Slice(node);
				}

				AddSeperatorToParentPage(rightPage, seperatorKey);

				// move the actual entries from page to right page
				var nKeys = _page.NumberOfEntries;
				for (int i = splitIndex; i < nKeys; i++)
				{
					var node = _page.GetNode(i);
					rightPage.CopyNodeDataToEndOfPage(node);
				}
				_page.Truncate(_tx, splitIndex);

				// actually insert the new key
				if (currentIndex > splitIndex ||
					newPosition && currentIndex == splitIndex)
				{
					var pos = rightPage.NodePositionFor(_newKey, _parent._cmp);
					rightPage.AddNode(pos, _newKey, _value, _pageNumber);
					_cursor.Push(rightPage);
				}
				else
				{
					_page.AddNode(_page.LastSearchPosition, _newKey, _value, _pageNumber);
					_cursor.Push(_page);
				}
			}

			private void AddSeperatorToParentPage(Page rightPage, Slice seperatorKey)
			{
				if (_parentPage.SizeLeft < SizeOf.BranchEntry(seperatorKey) + Constants.NodeOffsetSize)
				{
					new PageSplitter(_tx, _parent, seperatorKey, null, rightPage.PageNumber, _cursor).Execute();
				}
				else
				{
					_parentPage.AddNode(_parentPage.LastSearchPosition, seperatorKey, null, rightPage.PageNumber);
				}
			}
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
	}
}
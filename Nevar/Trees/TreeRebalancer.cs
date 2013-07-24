using System.IO;
using Nevar.Impl;

namespace Nevar.Trees
{
	public unsafe class TreeRebalancer
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

			System.Diagnostics.Debug.Assert(parentPage.NumberOfEntries >= 2); // if we have less than 2 entries in the parent, the tree is invalid

			var sibling = SetupMoveOrMerge(page, parentPage);

			minKeys = sibling.IsBranch ? 2 : 1; // branch must have at least 2 keys
			if (sibling.SizeUsed > Constants.PageMinSpace && 
				sibling.NumberOfEntries > minKeys)
			{
				// neighbor is over the min size and has enough key, can move just one key to  the current page
				MoveNode(parentPage, sibling, page);
				cursor.Pop();
				return parentPage;
			}

			if (page.LastSearchPosition == 0) // this is the right page, merge left
			{
				MergePages(parentPage, sibling, page);
			}
			else // this is the left page, merge right
			{
				parentPage.LastSearchPosition++; // move to the right page to be unlinked
				MergePages(parentPage, page, sibling);
			}
			cursor.Pop();
			return parentPage;
		}

		private void MergePages(Page parentPage, Page left, Page right)
		{
			for (int i = 0; i < right.NumberOfEntries; i++)
			{
				right.LastSearchPosition = i;
				var key = GetCurrentKeyFrom(right);
				var node = right.GetNode(i);
				left.CopyNodeDataToEndOfPage(node, key);
			}
			parentPage.RemoveNode(parentPage.LastSearchPosition); // unlink the right sibling
			_tx.FreePage(right);
		}

		private Page SetupMoveOrMerge(Page page, Page parentPage)
		{
			Page sibling;
			if (parentPage.LastSearchPosition == 0) // we are the left most item
			{
				sibling = _tx.GetPage(parentPage.GetNode(1)->PageNumber);
				sibling.LastSearchPosition = 0;
				page.LastSearchPosition = page.NumberOfEntries + 1;
				parentPage.LastSearchPosition = 1;
			}
			else // there is at least 1 page to our left
			{
				sibling = _tx.GetPage(parentPage.GetNode(parentPage.LastSearchPosition - 1)->PageNumber);
				sibling.LastSearchPosition = sibling.NumberOfEntries - 1;
				page.LastSearchPosition = 0;
			}
			return sibling;
		}

		private void MoveNode(Page parentPage, Page from, Page to)
		{
			var fromKey = GetCurrentKeyFrom(from);

			var fromNode = from.GetNode(from.LastSearchPosition);
			Stream val = null;
			int pageNum;
			if (fromNode->Flags.HasFlag(NodeFlags.Data))
			{
				val = new UnmanagedMemoryStream(from.Base + from.KeysOffsets[from.LastSearchPosition] + Constants.NodeHeaderSize + fromKey.Size, fromNode->DataSize);
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

			from.RemoveNode(from.LastSearchPosition);

			parentPage.RemoveNode(parentPage.LastSearchPosition);
			var toKey = GetCurrentKeyFrom(from); // get the next smallest key it has
			parentPage.AddNode(parentPage.LastSearchPosition, toKey, null, to.PageNumber);
		}

		private Slice GetCurrentKeyFrom(Page page)
		{
			var node = page.GetNode(page.LastSearchPosition);
			var key = new Slice(node);
			while (key.Size == 0)
			{
				System.Diagnostics.Debug.Assert(page.LastSearchPosition == 0 && page.IsBranch);
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
			System.Diagnostics.Debug.Assert(node->Flags.HasFlag(NodeFlags.PageRef));
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
}
using System;
using System.Diagnostics;
using Voron.Debugging;
using Voron.Impl;

namespace Voron.Trees
{
    public unsafe class TreeRebalancer
    {
        private readonly Transaction _tx;
		private readonly Tree _tree;

        public TreeRebalancer(Transaction tx, Tree tree)
        {
            _tx = tx;
			_tree = tree;
        }

        public Page Execute(Cursor cursor, Page page)
        {
            _tx.ClearRecentFoundPages(_tree);
            if (cursor.PageCount <= 1) // the root page
            {
                RebalanceRoot(cursor, page);
                return null;
            }

			var parentPage = _tx.ModifyPage(cursor.ParentPage.PageNumber, cursor.ParentPage);
			cursor.Update(cursor.Pages.First.Next, parentPage);

            if (page.NumberOfEntries == 0) // empty page, just delete it and fixup parent
            {
				// need to delete the implicit left page, shift right 
                if (parentPage.LastSearchPosition == 0 && parentPage.NumberOfEntries > 2)
                {
					var newImplicit = parentPage.GetNode(1)->PageNumber;
                    parentPage.RemoveNode(0);
                    parentPage.RemoveNode(0);
                    parentPage.AddPageRefNode(0, Slice.Empty, newImplicit);
                }
                else // will be set to rights by the next rebalance call
                {
                    parentPage.RemoveNode(parentPage.LastSearchPositionOrLastEntry);
                }
				
				_tx.FreePage(page.PageNumber);
                cursor.Pop();

                return parentPage;
            }

            var minKeys = page.IsBranch ? 2 : 1;
            if ((page.UseMoreSizeThan(_tx.DataPager.PageMinSpace)) &&
                page.NumberOfEntries >= minKeys)
                return null; // above space/keys thresholds

            Debug.Assert(parentPage.NumberOfEntries >= 2); // if we have less than 2 entries in the parent, the tree is invalid

            var sibling = SetupMoveOrMerge(cursor, page, parentPage);
            Debug.Assert(sibling.PageNumber != page.PageNumber);

            minKeys = sibling.IsBranch ? 2 : 1; // branch must have at least 2 keys
            if (sibling.UseMoreSizeThan(_tx.DataPager.PageMinSpace) &&
                sibling.NumberOfEntries > minKeys)
            {	         
                // neighbor is over the min size and has enough key, can move just one key to  the current page
	            if (page.IsBranch)
	            {
		            MoveBranchNode(parentPage, sibling, page);
	            }
	            else
		            MoveLeafNode(parentPage, sibling, page);
	            cursor.Pop();

                return parentPage;
            }

			if (page.LastSearchPosition == 0) // this is the right page, merge left
            {
				if (!HasEnoughSpaceToCopyNodes(sibling, page))
					return null;
				MergePages(parentPage, sibling, page);
            }
            else // this is the left page, merge right
            {
				if (!HasEnoughSpaceToCopyNodes(page, sibling))
					return null;
				MergePages(parentPage, page, sibling);
            }
            cursor.Pop();			

            return parentPage;
        }

	    private bool HasEnoughSpaceToCopyNodes(Page left, Page right)
	    {
		    var actualSpaceNeeded = 0;
		    var previousSearchPosition = right.LastSearchPosition;
		    for (int i = 0; i < right.NumberOfEntries; i++)
		    {
			    right.LastSearchPosition = i;
			    var key = GetActualKey(right, right.LastSearchPositionOrLastEntry);
			    var node = right.GetNode(i);
			    actualSpaceNeeded += (SizeOf.NodeEntryWithAnotherKey(node, key) + Constants.NodeOffsetSize);
		    }

		    right.LastSearchPosition = previousSearchPosition; //previous position --> prevent mutation of parameter
	        return left.HasSpaceFor(_tx, actualSpaceNeeded);
	    }

        private void MergePages(Page parentPage, Page left, Page right)
        {
            for (int i = 0; i < right.NumberOfEntries; i++)
            {
                right.LastSearchPosition = i;
                var key = GetActualKey(right, right.LastSearchPositionOrLastEntry);
                var node = right.GetNode(i);

                left.CopyNodeDataToEndOfPage(node, key);
            }

            parentPage.RemoveNode(parentPage.LastSearchPositionOrLastEntry); // unlink the right sibling
            _tx.FreePage(right.PageNumber);
        }

        private Page SetupMoveOrMerge(Cursor c, Page page, Page parentPage)
        {
            Page sibling;
            if (parentPage.LastSearchPosition == 0) // we are the left most item
            {
                parentPage.LastSearchPosition = 1;
                sibling = _tx.ModifyPage(parentPage.GetNode(1)->PageNumber, null);
                parentPage.LastSearchPosition = 0;
                sibling.LastSearchPosition = 0;
                page.LastSearchPosition = page.NumberOfEntries;
                parentPage.LastSearchPosition = 1;
            }
            else // there is at least 1 page to our left
            {
                var beyondLast = parentPage.LastSearchPosition == parentPage.NumberOfEntries;
                if (beyondLast)
                    parentPage.LastSearchPosition--;
                parentPage.LastSearchPosition--;
                sibling = _tx.ModifyPage(parentPage.GetNode(parentPage.LastSearchPosition)->PageNumber, null);
                parentPage.LastSearchPosition++;
                if (beyondLast)
                    parentPage.LastSearchPosition++;
                sibling.LastSearchPosition = sibling.NumberOfEntries - 1;
                page.LastSearchPosition = 0;
            }
            return sibling;
        }

        private void MoveLeafNode(Page parentPage, Page from, Page to)
        {
            Debug.Assert(from.IsBranch == false);
            var originalFromKeyStart = GetActualKey(from, from.LastSearchPositionOrLastEntry);

            var fromNode = from.GetNode(from.LastSearchPosition);
            byte* val = @from.Base + @from.KeysOffsets[@from.LastSearchPosition] + Constants.NodeHeaderSize + originalFromKeyStart.Size;

			var nodeVersion = fromNode->Version; // every time new node is allocated the version is increased, but in this case we do not want to increase it
			if (nodeVersion > 0)
				nodeVersion -= 1;

	        byte* dataPos;
	        switch (fromNode->Flags)
	        {
				case NodeFlags.PageRef:
	                to.EnsureHasSpaceFor(_tx, originalFromKeyStart, -1);
					dataPos = to.AddPageRefNode(to.LastSearchPosition, originalFromKeyStart, fromNode->PageNumber);
					break;
				case NodeFlags.Data:
	                to.EnsureHasSpaceFor(_tx, originalFromKeyStart, fromNode->DataSize);
			        dataPos = to.AddDataNode(to.LastSearchPosition, originalFromKeyStart, fromNode->DataSize, nodeVersion);
					break;
				case NodeFlags.MultiValuePageRef:
                    to.EnsureHasSpaceFor(_tx, originalFromKeyStart, fromNode->DataSize);
                    dataPos = to.AddMultiValueNode(to.LastSearchPosition, originalFromKeyStart, fromNode->DataSize, nodeVersion);
					break;
				default:
			        throw new NotSupportedException("Invalid node type to move: " + fromNode->Flags);
	        }
			
			if(dataPos != null)
				NativeMethods.memcpy(dataPos, val, fromNode->DataSize);
            
            from.RemoveNode(from.LastSearchPositionOrLastEntry);

            var pos = parentPage.LastSearchPositionOrLastEntry;
            parentPage.RemoveNode(pos);

            var newKey = GetActualKey(to, 0); // get the next smallest key it has now
            var pageNumber = to.PageNumber;
            if (parentPage.GetNode(0)->PageNumber == to.PageNumber)
            {
                pageNumber = from.PageNumber;
                newKey = GetActualKey(from, 0);
            }
            parentPage.EnsureHasSpaceFor(_tx, newKey, -1);
			parentPage.AddPageRefNode(pos, newKey, pageNumber);
        }

        private void MoveBranchNode(Page parentPage, Page from, Page to)
        {
            Debug.Assert(from.IsBranch);
            var originalFromKeyStart = GetActualKey(from, from.LastSearchPositionOrLastEntry);

            to.EnsureHasSpaceFor(_tx, originalFromKeyStart, -1);

            var fromNode = from.GetNode(from.LastSearchPosition);
            long pageNum = fromNode->PageNumber;

            if (to.LastSearchPosition == 0)
            {
                // cannot add to left implicit side, adjust by moving the left node
                // to the right by one, then adding the new one as the left

                var implicitLeftKey = GetActualKey(to, 0);
                var leftPageNumber = to.GetNode(0)->PageNumber;
                to.AddPageRefNode(1, implicitLeftKey, leftPageNumber);
                to.AddPageRefNode(0, Slice.BeforeAllKeys, pageNum);
                to.RemoveNode(1);
            }
            else
            {
				to.AddPageRefNode(to.LastSearchPosition, originalFromKeyStart, pageNum);
            }

            if (from.LastSearchPositionOrLastEntry == 0)
            {
                // cannot just remove the left node, need to adjust those
                var rightPageNumber = from.GetNode(1)->PageNumber;
                from.RemoveNode(0); // remove the original implicit node
                from.RemoveNode(0); // remove the next node that we now turned into implicit
                from.EnsureHasSpaceFor(_tx, Slice.BeforeAllKeys, -1);
                from.AddPageRefNode(0, Slice.BeforeAllKeys, rightPageNumber);
                Debug.Assert(from.NumberOfEntries >= 2);
            }
            else
            {
                from.RemoveNode(from.LastSearchPositionOrLastEntry);
            }

            var pos = parentPage.LastSearchPositionOrLastEntry;
            parentPage.RemoveNode(pos);
            var newKey = GetActualKey(to, 0); // get the next smallest key it has now
            var pageNumber = to.PageNumber;
            if (parentPage.GetNode(0)->PageNumber == to.PageNumber)
            {
                pageNumber = from.PageNumber;
                newKey = GetActualKey(from, 0);
            }
            parentPage.EnsureHasSpaceFor(_tx, newKey, -1);
			parentPage.AddPageRefNode(pos, newKey, pageNumber);
        }

        private Slice GetActualKey(Page page, int pos)
        {
            var node = page.GetNode(pos);
            var key = new Slice(node);
            while (key.Size == 0)
            {
                Debug.Assert(page.IsBranch);
                page = _tx.GetReadOnlyPage(node->PageNumber);
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
            Debug.Assert(node->Flags == (NodeFlags.PageRef));

            _tree.State.LeafPages = 1;
			_tree.State.BranchPages = 0;
			_tree.State.Depth = 1;
			_tree.State.PageCount = 1;

			var rootPage = _tx.ModifyPage(node->PageNumber, null);
			_tree.State.RootPageNumber = rootPage.PageNumber;

            Debug.Assert(rootPage.Dirty);

            cursor.Pop();
            cursor.Push(rootPage);

            _tx.FreePage(page.PageNumber);
        }
    }
}
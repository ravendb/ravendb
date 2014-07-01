using System;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using Voron.Debugging;
using Voron.Impl;
using Voron.Impl.Paging;

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
				// need to change the implicit left page
				if (parentPage.LastSearchPosition == 0 && parentPage.NumberOfEntries > 2)
				{
					var newImplicit = parentPage.GetNode(1)->PageNumber;
					parentPage.RemoveNode(0);
					parentPage.ChangeImplicitRefPageNode(newImplicit);
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
		            MoveBranchNode(parentPage, sibling, page);
	            else
		            MoveLeafNode(parentPage, sibling, page);

	            cursor.Pop();

                return parentPage;
            }

			if (page.LastSearchPosition == 0) // this is the right page, merge left
			{
				if (TryMergePages(parentPage, sibling, page) == false)
					return null;
            }
            else // this is the left page, merge right
            {
	            if (TryMergePages(parentPage, page, sibling) == false)
					return null;
            }

            cursor.Pop();			

            return parentPage;
        }

		private bool TryMergePages(Page parentPage, Page left, Page right)
		{
			TemporaryPage tmp;
			using (_tx.Environment.GetTemporaryPage(_tx, out tmp))
			{
				var mergedPage = tmp.GetTempPage(left.KeysPrefixed);
				NativeMethods.memcpy(mergedPage.Base, left.Base, left.PageSize);

				var previousSearchPosition = right.LastSearchPosition;

				for (int i = 0; i < right.NumberOfEntries; i++)
				{
					right.LastSearchPosition = i;
					var key = GetActualKey(right, right.LastSearchPositionOrLastEntry);
					var node = right.GetNode(i);

					var prefixedKey = mergedPage.PrepareKeyToInsert(key, mergedPage.NumberOfEntries);

					if (mergedPage.HasSpaceFor(_tx, SizeOf.NodeEntryWithAnotherKey(node, prefixedKey) + Constants.NodeOffsetSize + SizeOf.NewPrefix(prefixedKey)) == false)
					{
						right.LastSearchPosition = previousSearchPosition; //previous position --> prevent mutation of parameter
						return false;
					}

					mergedPage.CopyNodeDataToEndOfPage(node, prefixedKey);
				}

				NativeMethods.memcpy(left.Base, mergedPage.Base, left.PageSize);
			}

			parentPage.RemoveNode(parentPage.LastSearchPositionOrLastEntry); // unlink the right sibling
			_tx.FreePage(right.PageNumber);

			return true;
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
            byte* val = @from.Base + @from.KeysOffsets[@from.LastSearchPosition] + Constants.NodeHeaderSize + new PrefixedSlice(fromNode).Size;

			var nodeVersion = fromNode->Version; // every time new node is allocated the version is increased, but in this case we do not want to increase it
			if (nodeVersion > 0)
				nodeVersion -= 1;

	        var prefixedOriginalFromKey = to.PrepareKeyToInsert(originalFromKeyStart, to.LastSearchPosition);

	        byte* dataPos;
	        switch (fromNode->Flags)
	        {
				case NodeFlags.PageRef:
					to.EnsureHasSpaceFor(_tx, prefixedOriginalFromKey, -1);
					dataPos = to.AddPageRefNode(to.LastSearchPosition, prefixedOriginalFromKey, fromNode->PageNumber);
					break;
				case NodeFlags.Data:
					to.EnsureHasSpaceFor(_tx, prefixedOriginalFromKey, fromNode->DataSize);
					dataPos = to.AddDataNode(to.LastSearchPosition, prefixedOriginalFromKey, fromNode->DataSize, nodeVersion);
					break;
				case NodeFlags.MultiValuePageRef:
					to.EnsureHasSpaceFor(_tx, prefixedOriginalFromKey, fromNode->DataSize);
					dataPos = to.AddMultiValueNode(to.LastSearchPosition, prefixedOriginalFromKey, fromNode->DataSize, nodeVersion);
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

	        var prefixedNewKey = parentPage.PrepareKeyToInsert(newKey, pos);

			parentPage.EnsureHasSpaceFor(_tx, prefixedNewKey, -1);
			parentPage.AddPageRefNode(pos, prefixedNewKey, pageNumber);
        }

        private void MoveBranchNode(Page parentPage, Page from, Page to)
        {
            Debug.Assert(from.IsBranch);

	        var originalFromKey = to.PrepareKeyToInsert(GetActualKey(from, from.LastSearchPositionOrLastEntry), to.LastSearchPosition);

            to.EnsureHasSpaceFor(_tx, originalFromKey, -1);

            var fromNode = from.GetNode(from.LastSearchPosition);
            long pageNum = fromNode->PageNumber;

            if (to.LastSearchPosition == 0)
            {
                // cannot add to left implicit side, adjust by moving the left node
                // to the right by one, then adding the new one as the left

	            NodeHeader* actualKeyNode;
                var implicitLeftKey = GetActualKey(to, 0, out actualKeyNode);
	            var implicitLeftNode = to.GetNode(0);
				var leftPageNumber = implicitLeftNode->PageNumber;

	            MemorySlice implicitLeftKeyToInsert;

	            if (implicitLeftNode == actualKeyNode) // no need to create a prefix, just use the existing prefixed key from the node
					implicitLeftKeyToInsert = _tree.KeysPrefixing ? (MemorySlice) new PrefixedSlice(actualKeyNode) : new Slice(actualKeyNode);
				else
					implicitLeftKeyToInsert = to.PrepareKeyToInsert(implicitLeftKey, 1);
	            
				to.EnsureHasSpaceFor(_tx, implicitLeftKeyToInsert, -1);
				to.AddPageRefNode(1, implicitLeftKeyToInsert, leftPageNumber);

				to.ChangeImplicitRefPageNode(pageNum); // setup the new implicit node
            }
            else
            {
				to.AddPageRefNode(to.LastSearchPosition, originalFromKey, pageNum);
            }

            if (from.LastSearchPositionOrLastEntry == 0)
            {
                var rightPageNumber = from.GetNode(1)->PageNumber;
                from.RemoveNode(0); // remove the original implicit node
                from.ChangeImplicitRefPageNode(rightPageNumber); // setup the new implicit node
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

	        var prefixedNewKey = parentPage.PrepareKeyToInsert(newKey, pos);

			parentPage.EnsureHasSpaceFor(_tx, prefixedNewKey, -1);
			parentPage.AddPageRefNode(pos, prefixedNewKey, pageNumber);
        }

		[MethodImpl(MethodImplOptions.AggressiveInlining)]
	    private MemorySlice GetActualKey(Page page, int pos)
	    {
		    NodeHeader* _;
		    return GetActualKey(page, pos, out _);
	    }

		[MethodImpl(MethodImplOptions.AggressiveInlining)]
        private MemorySlice GetActualKey(Page page, int pos, out NodeHeader* node)
        {
            node = page.GetNode(pos);
			var key = page.GetNodeKey(node);
			while (key.Size == 0)
            {
                Debug.Assert(page.IsBranch);
                page = _tx.GetReadOnlyPage(node->PageNumber);
                node = page.GetNode(0);
				page.SetNodeKey(node, ref key);
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
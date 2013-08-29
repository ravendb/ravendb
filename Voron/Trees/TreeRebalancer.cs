using System.Diagnostics;
using Voron.Impl;

namespace Voron.Trees
{
    public unsafe class TreeRebalancer
    {
        private readonly Transaction _tx;
        private readonly TreeDataInTransaction _txInfo;
        private readonly SliceComparer _cmp;

        public TreeRebalancer(Transaction tx, TreeDataInTransaction txInfo, SliceComparer cmp)
        {
            _tx = tx;
            _txInfo = txInfo;
            _cmp = cmp;
        }

        public Page Execute(Cursor cursor, Page page)
        {
            if (cursor.PageCount <= 1) // the root page
            {
                RebalanceRoot(cursor, _txInfo, page);
                return null;
            }

            var parentPage = cursor.ParentPage;
            if (page.NumberOfEntries == 0) // empty page, just delete it and fixup parent
            {
                // need to delete the implicit left page, shift right 
                if (parentPage.LastSearchPosition == 0 && parentPage.NumberOfEntries > 2)
                {
					var newImplicit = parentPage.GetNode(1)->PageNumber;
                    parentPage.AddNode(0, Slice.Empty, -1, newImplicit, 0);
                    parentPage.RemoveNode(1);
                    parentPage.RemoveNode(1);
                }
                else // will be set to rights by the next rebalance call
                {
                    parentPage.RemoveNode(parentPage.LastSearchPosition);
                }
                cursor.Pop();
                return parentPage;
            }

            var minKeys = page.IsBranch ? 2 : 1;
            if (page.SizeUsed >= _tx.Pager.PageMinSpace &&
                page.NumberOfEntries >= minKeys)
                return null; // above space/keys thresholds

            Debug.Assert(parentPage.NumberOfEntries >= 2); // if we have less than 2 entries in the parent, the tree is invalid

            var sibling = SetupMoveOrMerge(cursor, page, parentPage);

            Debug.Assert(sibling.PageNumber != page.PageNumber);

            minKeys = sibling.IsBranch ? 2 : 1; // branch must have at least 2 keys
            if (sibling.SizeUsed > _tx.Pager.PageMinSpace &&
                sibling.NumberOfEntries > minKeys)
            {
                // neighbor is over the min size and has enough key, can move just one key to  the current page
                if(page.IsBranch)
                    MoveBranchNode(parentPage, sibling, page);
                else
                    MoveLeafNode(parentPage, sibling, page);
                cursor.Pop();
                return parentPage;
            }

            if (page.LastSearchPosition == 0) // this is the right page, merge left
            {
                MergePages(parentPage, sibling, page);
            }
            else // this is the left page, merge right
            {
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
                var key = GetActualKey(right, right.LastSearchPositionOrLastEntry);
                var node = right.GetNode(i);
                left.CopyNodeDataToEndOfPage(node, key);
            }
            left.ItemCount += right.ItemCount;
            parentPage.RemoveNode(parentPage.LastSearchPositionOrLastEntry); // unlink the right sibling
            _tx.FreePage(right.PageNumber);
        }

        private Page SetupMoveOrMerge(Cursor c, Page page, Page parentPage)
        {
            Page sibling;
            if (parentPage.LastSearchPosition == 0) // we are the left most item
            {
                _tx.ModifyCursor(_txInfo, c);
                parentPage.LastSearchPosition = 1;
                sibling = _tx.ModifyPage(_txInfo.Tree, parentPage, parentPage.GetNode(1)->PageNumber, c);
                parentPage.LastSearchPosition = 0;
                sibling.LastSearchPosition = 0;
                page.LastSearchPosition = page.NumberOfEntries;
                parentPage.LastSearchPosition = 1;
            }
            else // there is at least 1 page to our left
            {
                _tx.ModifyCursor(_txInfo, c);
                var beyondLast = parentPage.LastSearchPosition == parentPage.NumberOfEntries;
                if (beyondLast)
                    parentPage.LastSearchPosition--;
                parentPage.LastSearchPosition--;
                sibling = _tx.ModifyPage(_txInfo.Tree, parentPage, parentPage.GetNode(parentPage.LastSearchPosition)->PageNumber, c);
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
            var dataPos = to.AddNode(to.LastSearchPosition, originalFromKeyStart, fromNode->DataSize, -1, fromNode->Version);
            NativeMethods.memcpy(dataPos, val, fromNode->DataSize);
            --@from.ItemCount;
            ++to.ItemCount;

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

            parentPage.AddNode(pos, newKey, -1, pageNumber, 0);
        }

        private void MoveBranchNode(Page parentPage, Page from, Page to)
        {
            Debug.Assert(from.IsBranch);
            var originalFromKeyStart = GetActualKey(from, from.LastSearchPositionOrLastEntry);

            var fromNode = from.GetNode(from.LastSearchPosition);
            long pageNum = fromNode->PageNumber;
            var itemsMoved = _tx.Pager.Get(_tx, pageNum).ItemCount;
            from.ItemCount -= itemsMoved;
            to.ItemCount += itemsMoved;

            if (to.LastSearchPosition == 0)
            {
                // cannot add to left implicit side, adjust by moving the left node
                // to the right by one, then adding the new one as the left

                var implicitLeftKey = GetActualKey(to, 0);
                var leftPageNumber = to.GetNode(0)->PageNumber;

				to.AddNode(1, implicitLeftKey, -1, leftPageNumber, 0);
				to.AddNode(0, Slice.BeforeAllKeys, -1, pageNum, 0);
				to.RemoveNode(1);
			}
            else
            {
                to.AddNode(to.LastSearchPosition, originalFromKeyStart, -1, pageNum, 0);
            }

            if (from.LastSearchPositionOrLastEntry == 0)
            {
                // cannot just remove the left node, need to adjust those
                var rightPageNumber = from.GetNode(1)->PageNumber;
                from.RemoveNode(0); // remove the original node
                from.RemoveNode(0); // remove the next node
                from.AddNode(0, Slice.BeforeAllKeys, -1, rightPageNumber, 0);
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

            parentPage.AddNode(pos, newKey, -1, pageNumber, 0);
        }

        private Slice GetActualKey(Page page, int pos)
        {
            var node = page.GetNode(pos);
            var key = new Slice(node);
            while (key.Size == 0)
            {
                Debug.Assert(page.LastSearchPosition == 0 && page.IsBranch);
                page = _tx.GetReadOnlyPage(node->PageNumber);
                node = page.GetNode(0);
                key.Set(node);
            }
            return key;
        }

        private void RebalanceRoot(Cursor cursor, TreeDataInTransaction txInfo, Page page)
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

            _tx.ModifyCursor(txInfo, cursor);
            txInfo.State.LeafPages = 1;
            txInfo.State.BranchPages = 0;
            txInfo.State.Depth = 1;
            txInfo.State.PageCount = 1;

            var rootPage = _tx.ModifyPage(_txInfo.Tree, null, node->PageNumber, cursor);
            txInfo.RootPageNumber = rootPage.PageNumber;

            Debug.Assert(rootPage.Dirty);

            cursor.Pop();
            cursor.Push(rootPage);

            _tx.FreePage(page.PageNumber);
        }
    }
}
using System.Diagnostics;
using System.IO;
using Nevar.Impl;

namespace Nevar.Trees
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
            if (cursor.Pages.Count <= 1) // the root page
            {
                RebalanceRoot(cursor, _txInfo, page);
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
            _tx.FreePage(right.PageNumber);
        }

        private Page SetupMoveOrMerge(Cursor c, Page page, Page parentPage)
        {
            Page sibling;
            if (parentPage.LastSearchPosition == 0) // we are the left most item
            {
                _tx.ModifyCursor(_txInfo, c);
                parentPage.LastSearchPosition = 1;
                sibling = _tx.GetModifiedPage(parentPage, parentPage.GetNode(1)->PageNumber);
                parentPage.LastSearchPosition = 0;
                sibling.LastSearchPosition = 0;
                page.LastSearchPosition = page.NumberOfEntries;
                parentPage.LastSearchPosition = 1;
            }
            else // there is at least 1 page to our left
            {
                _tx.ModifyCursor(_txInfo, c);
                parentPage.LastSearchPosition--;
                sibling = _tx.GetModifiedPage(parentPage, parentPage.GetNode(parentPage.LastSearchPosition)->PageNumber);
                parentPage.LastSearchPosition++;
                sibling.LastSearchPosition = sibling.NumberOfEntries - 1;
                page.LastSearchPosition = 0;
            }
            return sibling;
        }

        private void MoveNode(Page parentPage, Page from, Page to)
        {
            var originalFromKeyStart = GetCurrentKeyFrom(from);

            var fromNode = from.GetNode(from.LastSearchPosition);
            if (fromNode->Flags.HasFlag(NodeFlags.Data))
            {
                byte* val = @from.Base + @from.KeysOffsets[@from.LastSearchPosition] + Constants.NodeHeaderSize + originalFromKeyStart.Size;
                var dataPos = to.AddNode(to.LastSearchPosition, originalFromKeyStart, fromNode->DataSize, -1);
                NativeMethods.memcpy(dataPos, val, fromNode->DataSize);
            }
            else
            {
                long pageNum = fromNode->PageNumber;
                to.AddNode(to.LastSearchPosition, originalFromKeyStart, -1, pageNum);
            }

            from.RemoveNode(from.LastSearchPosition);

            parentPage.RemoveNode(parentPage.LastSearchPosition);
            var newFromKey = GetCurrentKeyFrom(from); // get the next smallest key it has now

            var pageNumber = to.PageNumber;
            // the current page is implicit left, so need to update it the _next_ entry
            if (parentPage.LastSearchPosition == 1) 
            {
                pageNumber = from.PageNumber;
            }

            parentPage.AddNode(parentPage.LastSearchPosition, newFromKey, -1, pageNumber);
        }

        private Slice GetCurrentKeyFrom(Page page)
        {
            var node = page.GetNode(page.LastSearchPositionOrLastEntry);
            var key = new Slice(node);
            while (key.Size == 0)
            {
                System.Diagnostics.Debug.Assert(page.LastSearchPosition == 0 && page.IsBranch);
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
            System.Diagnostics.Debug.Assert(node->Flags.HasFlag(NodeFlags.PageRef));
            _tx.ModifyCursor(txInfo, cursor);
            txInfo.State.LeafPages = 1;
            txInfo.State.BranchPages = 0;
            txInfo.State.Depth = 1;
            txInfo.State.PageCount = 1;
            txInfo.Root = _tx.GetReadOnlyPage(node->PageNumber);

            Debug.Assert(txInfo.Root.Dirty);

            cursor.Pop();
            cursor.Push(txInfo.Root);

            _tx.FreePage(page.PageNumber);
        }
    }
}
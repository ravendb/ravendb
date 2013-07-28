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
            if (page.SizeUsed >= Constants.PageMinSpace &&
                page.NumberOfEntries >= minKeys)
                return null; // above space/keys thresholds

            Debug.Assert(parentPage.NumberOfEntries >= 2); // if we have less than 2 entries in the parent, the tree is invalid

            var sibling = SetupMoveOrMerge(cursor, page, parentPage);

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
                page.LastSearchPosition = page.NumberOfEntries + 1;
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
            var fromKey = GetCurrentKeyFrom(from);

            var fromNode = from.GetNode(from.LastSearchPosition);
            byte* val = null;
            long pageNum;
            if (fromNode->Flags.HasFlag(NodeFlags.Data))
            {
                val = from.Base + from.KeysOffsets[from.LastSearchPosition] + Constants.NodeHeaderSize + fromKey.Size;
                var dataPos = to.AddNode(to.LastSearchPosition, fromKey, fromNode->DataSize, -1);
                NativeMethods.memcpy(dataPos, val, fromNode->DataSize);
            }
            else
            {
                pageNum = fromNode->PageNumber;
                to.AddNode(to.LastSearchPosition, fromKey, -1, pageNum);
            }

            from.RemoveNode(from.LastSearchPosition);

            parentPage.RemoveNode(parentPage.LastSearchPosition);
            var toKey = GetCurrentKeyFrom(from); // get the next smallest key it has

            var pageNumber = to.PageNumber;
            if (fromKey.Compare(GetCurrentKeyFrom(to), _cmp) > 0)
            {
                pageNumber = from.PageNumber;
            }

            parentPage.AddNode(parentPage.LastSearchPosition, toKey, -1, pageNumber);
        }

        private Slice GetCurrentKeyFrom(Page page)
        {
            var node = page.GetNode(page.LastSearchPosition);
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
            txInfo.LeafPages = 1;
            txInfo.BranchPages = 0;
            txInfo.Depth = 1;
            txInfo.PageCount = 1;
            txInfo.Root = _tx.GetReadOnlyPage(node->PageNumber);

            Debug.Assert(txInfo.Root.Dirty);

            cursor.Pop();
            cursor.Push(txInfo.Root);

            _tx.FreePage(page.PageNumber);
        }
    }
}
using System;
using System.Diagnostics;
using Voron.Impl;
using Voron.Impl.Paging;

namespace Voron.Trees
{
    public unsafe class PageSplitter
    {
        private readonly SliceComparer _cmp;
        private readonly Cursor _cursor;
        private readonly int _len;
        private readonly Slice _newKey;
        private readonly NodeFlags _nodeType;
        private readonly ushort _nodeVersion;
        private readonly Page _page;
        private readonly long _pageNumber;
        private readonly TreeMutableState _treeState;
        private readonly Transaction _tx;
        private readonly Tree _tree;
        private Page _parentPage;

        public PageSplitter(Transaction tx,
            Tree tree,
            SliceComparer cmp,
            Slice newKey,
            int len,
            long pageNumber,
            NodeFlags nodeType,
            ushort nodeVersion,
            Cursor cursor,
            TreeMutableState treeState)
        {
            _tx = tx;
            _tree = tree;
            _cmp = cmp;
            _newKey = newKey;
            _len = len;
            _pageNumber = pageNumber;
            _nodeType = nodeType;
            _nodeVersion = nodeVersion;
            _cursor = cursor;
            _treeState = treeState;
            Page page = _cursor.Pages.First.Value;
            _page = tx.ModifyPage(page.PageNumber, page);
            _cursor.Pop();
        }

        public byte* Execute()
        {
            Page rightPage = Tree.NewPage(_tx, _page.Flags, 1);
            _treeState.RecordNewPage(_page, 1);
            rightPage.Flags = _page.Flags;
            if (_cursor.PageCount == 0) // we need to do a root split
            {
                Page newRootPage = Tree.NewPage(_tx, PageFlags.Branch, 1);
                _cursor.Push(newRootPage);
                _treeState.RootPageNumber = newRootPage.PageNumber;
                _treeState.Depth++;
                _treeState.RecordNewPage(newRootPage, 1);

                // now add implicit left page
                newRootPage.AddPageRefNode(0, Slice.BeforeAllKeys, _page.PageNumber);
                _parentPage = newRootPage;
                _parentPage.LastSearchPosition++;
            }
            else
            {
                // we already popped the page, so the current one on the stack is what the parent of the page
                _parentPage = _tx.ModifyPage(_cursor.CurrentPage.PageNumber, _cursor.CurrentPage);
                _cursor.Update(_cursor.Pages.First, _parentPage);
            }

            if (_page.IsLeaf)
            {
                _tx.ClearRecentFoundPages(_tree);
            }

            if (_page.LastSearchPosition >= _page.NumberOfEntries)
            {
                // when we get a split at the end of the page, we take that as a hint that the user is doing 
                // sequential inserts, at that point, we are going to keep the current page as is and create a new 
                // page, this will allow us to do minimal amount of work to get the best density

                byte* pos;
                if (_page.IsBranch)
                {
                    // here we steal the last entry from the current page so we maintain the implicit null left entry
                    NodeHeader* node = _page.GetNode(_page.NumberOfEntries - 1);
                    Debug.Assert(node->Flags == NodeFlags.PageRef);
                    rightPage.AddPageRefNode(0, Slice.Empty, node->PageNumber);
                    pos = AddNodeToPage(rightPage, 1);

                    AddSeparatorToParentPage(rightPage, new Slice(node));

                    _page.RemoveNode(_page.NumberOfEntries - 1);
                }
                else
                {
                    AddSeparatorToParentPage(rightPage, _newKey);
                    pos = AddNodeToPage(rightPage, 0);
                }
                _cursor.Push(rightPage);
                return pos;
            }

            return SplitPageInHalf(rightPage);
        }

        private byte* AddNodeToPage(Page page, int index)
        {
            switch (_nodeType)
            {
                case NodeFlags.PageRef:
                    return page.AddPageRefNode(index, _newKey, _pageNumber);
                case NodeFlags.Data:
                    return page.AddDataNode(index, _newKey, _len, _nodeVersion);
                case NodeFlags.MultiValuePageRef:
                    return page.AddMultiValueNode(index, _newKey, _len, _nodeVersion);
                default:
                    throw new NotSupportedException("Unknown node type");
            }
        }

        private byte* SplitPageInHalf(Page rightPage)
        {
            int currentIndex = _page.LastSearchPosition;
            bool newPosition = true;
            int splitIndex = _page.NumberOfEntries/2;
            if (currentIndex < splitIndex)
                newPosition = false;

            if (_page.IsLeaf)
            {
                splitIndex = AdjustSplitPosition(_newKey, _len, _page, currentIndex, splitIndex, ref newPosition);
            }

            NodeHeader* currentNode = _page.GetNode(splitIndex);
            var currentKey = new Slice(currentNode);

            // here we the current key is the separator key and can go either way, so 
            // use newPosition to decide if it stays on the left node or moves to the right
            Slice seperatorKey;
            if (currentIndex == splitIndex && newPosition)
            {
                seperatorKey = currentKey.Compare(_newKey, NativeMethods.memcmp) < 0 ? currentKey : _newKey;
            }
            else
            {
                seperatorKey = currentKey;
            }

            AddSeparatorToParentPage(rightPage, seperatorKey);

            // move the actual entries from page to right page
            ushort nKeys = _page.NumberOfEntries;
            for (int i = splitIndex; i < nKeys; i++)
            {
                NodeHeader* node = _page.GetNode(i);
                if (_page.IsBranch && rightPage.NumberOfEntries == 0)
                {
                    rightPage.CopyNodeDataToEndOfPage(node, Slice.Empty);
                }
                else
                {
                    rightPage.CopyNodeDataToEndOfPage(node);
                }
            }
            _page.Truncate(_tx, splitIndex);

            // actually insert the new key
            return (currentIndex > splitIndex || newPosition && currentIndex == splitIndex)
                ? InsertNewKey(rightPage)
                : InsertNewKey(_page);
        }

        private byte* InsertNewKey(Page p)
        {
            int pos = p.NodePositionFor(_newKey, _cmp);

            byte* dataPos = AddNodeToPage(p, pos);
            _cursor.Push(p);
            return dataPos;
        }

        private void AddSeparatorToParentPage(Page rightPage, Slice seperatorKey)
        {
            if (_parentPage.SizeLeft < SizeOf.BranchEntry(seperatorKey) + Constants.NodeOffsetSize)
            {
                var pageSplitter = new PageSplitter(_tx, _tree, _cmp, seperatorKey, -1, rightPage.PageNumber, NodeFlags.PageRef,
                    0, _cursor, _treeState);
                pageSplitter.Execute();
            }
            else
            {
                _parentPage.NodePositionFor(seperatorKey, _cmp); // select the appropriate place for this
                _parentPage.AddPageRefNode(_parentPage.LastSearchPosition, seperatorKey, rightPage.PageNumber);
            }
        }


        /// <summary>
        ///     For leaf pages, check the split point based on what
        ///     fits where, since otherwise adding the node can fail.
        ///     This check is only needed when the data items are
        ///     relatively large, such that being off by one will
        ///     make the difference between success or failure.
        ///     It's also relevant if a page happens to be laid out
        ///     such that one half of its nodes are all "small" and
        ///     the other half of its nodes are "large." If the new
        ///     item is also "large" and falls on the half with
        ///     "large" nodes, it also may not fit.
        /// </summary>
        private int AdjustSplitPosition(Slice key, int len, Page page, int currentIndex, int splitIndex,
            ref bool newPosition)
        {
            int nodeSize = SizeOf.NodeEntry(AbstractPager.PageMaxSpace, key, len) + Constants.NodeOffsetSize;
            if (page.NumberOfEntries >= 20 && nodeSize <= AbstractPager.PageMaxSpace/16)
            {
                return splitIndex;
            }

            int pageSize = nodeSize;
            if (currentIndex <= splitIndex)
            {
                newPosition = false;
                for (int i = 0; i < splitIndex; i++)
                {
                    NodeHeader* node = page.GetNode(i);
                    pageSize += node->GetNodeSize();
                    pageSize += pageSize & 1;
                    if (pageSize > AbstractPager.PageMaxSpace)
                    {
                        if (i <= currentIndex)
                        {
                            if (i < currentIndex)
                                newPosition = true;
                            return currentIndex;
                        }
                        return (ushort) i;
                    }
                }
            }
            else
            {
                for (int i = page.NumberOfEntries - 1; i >= splitIndex; i--)
                {
                    NodeHeader* node = page.GetNode(i);
                    pageSize += node->GetNodeSize();
                    pageSize += pageSize & 1;
                    if (pageSize > AbstractPager.PageMaxSpace)
                    {
                        if (i >= currentIndex)
                        {
                            newPosition = false;
                            return currentIndex;
                        }
                        return (ushort) (i + 1);
                    }
                }
            }
            return splitIndex;
        }
    }
}
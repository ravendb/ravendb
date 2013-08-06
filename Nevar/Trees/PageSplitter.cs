using Nevar.Impl;

namespace Nevar.Trees
{
    public unsafe class PageSplitter
    {
        private readonly Transaction _tx;
        private readonly SliceComparer _cmp;
        private readonly Slice _newKey;
        private readonly int _len;
        private readonly long _pageNumber;
        private readonly Cursor _cursor;
        private readonly TreeDataInTransaction _txInfo;
        private readonly Page _page;
        private Page _parentPage;

        public PageSplitter(Transaction tx, SliceComparer cmp, Slice newKey, int len, long pageNumber, Cursor cursor, TreeDataInTransaction txInfo)
        {
            _tx = tx;
            _cmp = cmp;
            _newKey = newKey;
            _len = len;
            _pageNumber = pageNumber;
            _cursor = cursor;
            _txInfo = txInfo;
            _page = _cursor.Pop();
        }

        public byte* Execute()
        {
            var rightPage = Tree.NewPage(_tx, _page.Flags, 1);
            _txInfo.RecordNewPage(_page, 1);
            rightPage.Flags = _page.Flags;
            if (_cursor.Pages.Count == 0) // we need to do a root split
            {
                var newRootPage = Tree.NewPage(_tx, PageFlags.Branch, 1);
                _cursor.Push(newRootPage);
                _txInfo.Root = newRootPage;
                _txInfo.State.Depth++;
                _txInfo.RecordNewPage(newRootPage, 1);

                // now add implicit left page
                newRootPage.AddNode(0, Slice.BeforeAllKeys, -1, _page.PageNumber);
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
                var pos = rightPage.AddNode(0, _newKey, _len, _pageNumber);
                _cursor.Push(rightPage);
                return pos;
            }

            return SplitPageInHalf(rightPage);
        }

        private byte* SplitPageInHalf(Page rightPage)
        {
            var currentIndex = _page.LastSearchPosition;
            var newPosition = true;
            var splitIndex = _page.NumberOfEntries / 2;
            if (currentIndex < splitIndex)
                newPosition = false;

            if (_page.IsLeaf)
            {
                splitIndex = AdjustSplitPosition(_newKey, _len, _page, currentIndex, splitIndex, ref newPosition);
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

            byte* dataPos;
            // actually insert the new key
            if (currentIndex > splitIndex ||
                newPosition && currentIndex == splitIndex)
            {
                var pos = rightPage.NodePositionFor(_newKey, _cmp);
                dataPos = rightPage.AddNode(pos, _newKey, _len, _pageNumber);
                _cursor.Push(rightPage);
            }
            else
            {
                var pos = _page.NodePositionFor(_newKey, _cmp);
                dataPos = _page.AddNode(pos, _newKey, _len, _pageNumber);
                _cursor.Push(_page);
            }
            return dataPos;
        }

        private void AddSeperatorToParentPage(Page rightPage, Slice seperatorKey)
        {
            if (_parentPage.SizeLeft < SizeOf.BranchEntry(seperatorKey) + Constants.NodeOffsetSize)
            {
                new PageSplitter(_tx, _cmp, seperatorKey, -1, rightPage.PageNumber, _cursor, _txInfo).Execute();
            }
            else
            {
                _parentPage.NodePositionFor(seperatorKey, _cmp); // select the appropriate place for this
                _parentPage.AddNode(_parentPage.LastSearchPosition, seperatorKey, -1, rightPage.PageNumber);
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
        private int AdjustSplitPosition(Slice key, int len, Page page, int currentIndex, int splitIndex,
                                                      ref bool newPosition)
        {
            var nodeSize = SizeOf.NodeEntry(_tx.Pager.PageMaxSpace, key, len) + Constants.NodeOffsetSize;
            if (page.NumberOfEntries >= 20 && nodeSize <= _tx.Pager.PageMaxSpace / 16)
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
                    if (pageSize > _tx.Pager.PageMaxSpace)
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
                    if (pageSize > _tx.Pager.PageMaxSpace)
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
    }
}
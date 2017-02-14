using System;
using System.Diagnostics;
using System.Text;
using Voron.Impl;
using Voron.Impl.FreeSpace;
using Voron.Impl.Paging;

namespace Voron.Trees
{
    public unsafe class PageSplitter
    {
        private readonly Cursor _cursor;
        private readonly int _len;
        private readonly MemorySlice _newKey;
        private readonly NodeFlags _nodeType;
        private readonly ushort _nodeVersion;
        private readonly Page _page;
        private readonly long _pageNumber;
        private readonly Transaction _tx;
        private readonly Tree _tree;
        private Page _parentPage;

        public PageSplitter(Transaction tx,
            Tree tree,
            MemorySlice newKey,
            int len,
            long pageNumber,
            NodeFlags nodeType,
            ushort nodeVersion,
            Cursor cursor)
        {
            _tx = tx;
            _tree = tree;
            _newKey = newKey;
            _len = len;
            _pageNumber = pageNumber;
            _nodeType = nodeType;
            _nodeVersion = nodeVersion;
            _cursor = cursor;
            Page page = _cursor.Pages.First.Value;
            _page = tx.ModifyPage(page.PageNumber, _tree, page);
            _cursor.Pop();
        }

        public byte* Execute()
        {
            using (_tree.IsFreeSpaceTree ? _tx.Environment.FreeSpaceHandling.Disable() : null)
            {
                Page rightPage = _tree.NewPage(_page.Flags, 1);

                if (_cursor.PageCount == 0) // we need to do a root split
                {
                    Page newRootPage = _tree.NewPage(_tree.KeysPrefixing ? PageFlags.Branch | PageFlags.KeysPrefixed : PageFlags.Branch, 1);
                    _cursor.Push(newRootPage);
                    _tree.State.RootPageNumber = newRootPage.PageNumber;
                    _tree.State.Depth++;

                    // now add implicit left page
                    newRootPage.AddPageRefNode(0, _tree.KeysPrefixing ? (MemorySlice) PrefixedSlice.BeforeAllKeys : Slice.BeforeAllKeys, _page.PageNumber);
                    _parentPage = newRootPage;
                    _parentPage.LastSearchPosition++;
                }
                else
                {
                    // we already popped the page, so the current one on the stack is the parent of the page

                    _parentPage = _tx.ModifyPage(_cursor.CurrentPage.PageNumber, _tree, _cursor.CurrentPage);

                    _cursor.Update(_cursor.Pages.First, _parentPage);
                }

                if (_page.IsLeaf)
                {
                    _tree.ClearRecentFoundPages();
                }

                if (_page.LastSearchPosition >= _page.NumberOfEntries)
                {
                    // when we get a split at the end of the page, we take that as a hint that the user is doing 
                    // sequential inserts, at that point, we are going to keep the current page as is and create a new 
                    // page, this will allow us to do minimal amount of work to get the best density

                    Page branchOfSeparator;

                    byte* pos;
                    if (_page.IsBranch)
                    {
                        if (_page.NumberOfEntries > 2)
                        {
                            // here we steal the last entry from the current page so we maintain the implicit null left entry

                            NodeHeader* node = _page.GetNode(_page.NumberOfEntries - 1);
                            Debug.Assert(node->Flags == NodeFlags.PageRef);
                            rightPage.AddPageRefNode(0, _tree.KeysPrefixing ? (MemorySlice) PrefixedSlice.BeforeAllKeys : Slice.BeforeAllKeys, node->PageNumber);
                            pos = AddNodeToPage(rightPage, 1);

                            var separatorKey = _page.GetNodeKey(node);

                            AddSeparatorToParentPage(rightPage.PageNumber, separatorKey, out branchOfSeparator);

                            _page.RemoveNode(_page.NumberOfEntries - 1);
                        }
                        else
                        {
                            _tree.FreePage(rightPage); // return the unnecessary right page
                            pos = AddSeparatorToParentPage(_pageNumber, _newKey, out branchOfSeparator);

                            if (_cursor.CurrentPage.PageNumber != branchOfSeparator.PageNumber)
                                _cursor.Push(branchOfSeparator);

                            return pos;
                        }
                    }
                    else
                    {
                        AddSeparatorToParentPage(rightPage.PageNumber, _newKey, out branchOfSeparator);
                        pos = AddNodeToPage(rightPage, 0);
                    }
                    _cursor.Push(rightPage);
                    return pos;
                }

                return SplitPageInHalf(rightPage);
            }
        }

        private byte* AddNodeToPage(Page page, int index, MemorySlice alreadyPreparedNewKey = null)
        {
            var newKeyToInsert = alreadyPreparedNewKey ?? page.PrepareKeyToInsert(_newKey, index);

            switch (_nodeType)
            {
                case NodeFlags.PageRef:
                    return page.AddPageRefNode(index, newKeyToInsert, _pageNumber);
                case NodeFlags.Data:
                    return page.AddDataNode(index, newKeyToInsert, _len, _nodeVersion);
                case NodeFlags.MultiValuePageRef:
                    return page.AddMultiValueNode(index, newKeyToInsert, _len, _nodeVersion);
                default:
                    throw new NotSupportedException("Unknown node type");
            }
        }

        private byte* SplitPageInHalf(Page rightPage)
        {
            bool toRight;

            var currentIndex = _page.LastSearchPosition;
            var splitIndex = _page.NumberOfEntries / 2;

            if (currentIndex <= splitIndex)
            {
                toRight = false;
            }
            else
            {
                toRight = true;

                var leftPageEntryCount = splitIndex;
                var rightPageEntryCount = _page.NumberOfEntries - leftPageEntryCount + 1;

                if (rightPageEntryCount > leftPageEntryCount)
                {
                    splitIndex++;

                    Debug.Assert(splitIndex < _page.NumberOfEntries);
                }
            }

            PrefixNode[] prefixes = null;

            if (_tree.KeysPrefixing && _page.HasPrefixes)
            {
                prefixes = _page.GetPrefixes();
            }

            if (_page.IsLeaf || prefixes != null)
            {
                splitIndex = AdjustSplitPosition(currentIndex, splitIndex, prefixes, ref toRight);
            }

            var currentKey = _page.GetNodeKey(splitIndex);
            MemorySlice seperatorKey;

            if (toRight && splitIndex == currentIndex)
            {
                seperatorKey = currentKey.Compare(_newKey) < 0 ? currentKey : _newKey;
            }
            else
            {
                seperatorKey = currentKey;
            }

            Page parentOfRight;
            AddSeparatorToParentPage(rightPage.PageNumber, seperatorKey, out parentOfRight);

            var parentOfPage = _cursor.CurrentPage;

            MemorySlice instance = _page.CreateNewEmptyKey();

            if (prefixes != null)
            {
                for (int i = 0; i < prefixes.Length; i++)
                {
                    var prefix = prefixes[i];

                    rightPage.WritePrefix(new Slice(prefix.ValuePtr, prefix.PrefixLength), i);
                }
            }

            bool addedAsImplicitRef = false;

            if (_page.IsBranch && toRight && seperatorKey == _newKey)
            {
                // _newKey needs to be inserted as first key (BeforeAllKeys) to the right page, so we need to add it before we move entries from the current page
                AddNodeToPage(rightPage, 0, _tree.KeysPrefixing ? (MemorySlice)PrefixedSlice.BeforeAllKeys : Slice.BeforeAllKeys);
                addedAsImplicitRef = true;
            }

            // move the actual entries from page to right page
            ushort nKeys = _page.NumberOfEntries;
            for (int i = splitIndex; i < nKeys; i++)
            {
                NodeHeader* node = _page.GetNode(i);
                if (_page.IsBranch && rightPage.NumberOfEntries == 0)
                {
                    rightPage.CopyNodeDataToEndOfPage(node, _tree.KeysPrefixing ? (MemorySlice) PrefixedSlice.BeforeAllKeys : Slice.BeforeAllKeys);
                }
                else
                {
                    _page.SetNodeKey(node, ref instance);
                    var key = rightPage.PrepareKeyToInsert(instance, rightPage.NumberOfEntries);

                    rightPage.CopyNodeDataToEndOfPage(node, key);
                }
            }

            _page.Truncate(_tx, splitIndex);

            byte* pos;

            if (addedAsImplicitRef == false)
            {
                try
                {
                    if (toRight && _cursor.CurrentPage.PageNumber != parentOfRight.PageNumber)
                    {
                        // modify the cursor if we are going to insert to the right page
                        _cursor.Pop();
                        _cursor.Push(parentOfRight);
                    }

                    // actually insert the new key
                    pos = toRight ? InsertNewKey(rightPage) : InsertNewKey(_page);
                }
                catch (InvalidOperationException e)
                {
                    if (e.Message.StartsWith("The page is full and cannot add an entry") == false)
                        throw;

                    throw new InvalidOperationException(GatherDetailedDebugInfo(rightPage, currentKey, seperatorKey, currentIndex, splitIndex, toRight), e);
                }
            }
            else
            {
                pos = null;
                _cursor.Push(rightPage);
            }

            if (_page.IsBranch) // remove a branch that has only one entry, the page ref needs to be added to the parent of the current page
            {
                Debug.Assert(_page.NumberOfEntries > 0);
                Debug.Assert(rightPage.NumberOfEntries > 0);

                if (_page.NumberOfEntries == 1)
                    RemoveBranchWithOneEntry(_page, parentOfPage);

                if (rightPage.NumberOfEntries == 1)
                    RemoveBranchWithOneEntry(rightPage, parentOfRight);
            }

            return pos;
        }

        private void RemoveBranchWithOneEntry(Page page, Page parentPage)
        {
            Debug.Assert(page.NumberOfEntries == 1);

            var pageRefNumber = page.GetNode(0)->PageNumber;

            NodeHeader* nodeHeader = null;

            for (int i = 0; i < parentPage.NumberOfEntries; i++)
            {
                nodeHeader = parentPage.GetNode(i);

                if (nodeHeader->PageNumber == page.PageNumber)
                    break;
            }

            Debug.Assert(nodeHeader->PageNumber == page.PageNumber, string.Format("Node page number: {0}, page number: {1}", nodeHeader->PageNumber, page.PageNumber));

            nodeHeader->PageNumber = pageRefNumber;

            if (_cursor.CurrentPage.PageNumber == page.PageNumber)
            {
                _cursor.Pop();
                _cursor.Push(_tx.GetReadOnlyPage(pageRefNumber));
            }

            _tree.FreePage(page);
        }

        private byte* InsertNewKey(Page p)
        {
            int pos = p.NodePositionFor(_newKey);

            var newKeyToInsert = p.PrepareKeyToInsert(_newKey, pos);

            if (p.HasSpaceFor(_tx, p.GetRequiredSpace(newKeyToInsert, _len)) == false)
            {
                _cursor.Push(p);

                var pageSplitter = new PageSplitter(_tx, _tree, _newKey, _len, _pageNumber, _nodeType, _nodeVersion, _cursor);

                return pageSplitter.Execute();
            }

            byte* dataPos = AddNodeToPage(p, pos, newKeyToInsert);
            _cursor.Push(p);
            return dataPos;
        }

        private byte* AddSeparatorToParentPage(long pageRefNumber, MemorySlice separatorKey, out Page parentOfPageRef)
        {
            var parent = new ParentPageAction(_parentPage, _page, _tree, _cursor, _tx);

            var pos = parent.AddSeparator(separatorKey, pageRefNumber);

            parentOfPageRef = parent.ParentOfAddedPageRef;

            return pos;
        }

        private int AdjustSplitPosition(int currentIndex, int splitIndex, PrefixNode[] prefixes, ref bool toRight)
        {
            MemorySlice keyToInsert;

            int pageSize = 0;

            if (_tree.KeysPrefixing)
            {
                keyToInsert = new PrefixedSlice(_newKey); // let's assume that _newkey won't match any of the existing prefixes

                pageSize += Constants.PrefixInfoSectionSize;
                pageSize += Constants.PrefixNodeHeaderSize + 1; // possible new prefix,  + 1 because of possible 2-byte alignment
            }
            else
                keyToInsert = _newKey;

            pageSize += SizeOf.NodeEntry(AbstractPager.PageMaxSpace, keyToInsert , _len) + Constants.NodeOffsetSize;

            if (prefixes != null)
            {
                // we are going to copy all existing prefixes so we need to take into account their sizes
                for (var i = 0; i < prefixes.Length; i++)
                {
                    var prefixNodeSize = Constants.PrefixNodeHeaderSize + prefixes[i].Header.PrefixLength;
                    pageSize += prefixNodeSize + (prefixNodeSize & 1); // & 1 because we need 2-byte alignment
                }
            }

            if (toRight == false)
            {
                for (int i = 0; i < splitIndex; i++)
                {
                    NodeHeader* node = _page.GetNode(i);
                    pageSize += node->GetNodeSize();
                    pageSize += pageSize & 1;
                    if (pageSize > AbstractPager.PageMaxSpace)
                    {
                        if (i <= currentIndex)
                        {
                            if (i < currentIndex)
                                toRight = true;
                            return currentIndex;
                        }
                        return i;
                    }
                }
            }
            else
            {
                for (int i = _page.NumberOfEntries - 1; i >= splitIndex; i--)
                {
                    NodeHeader* node = _page.GetNode(i);
                    pageSize += node->GetNodeSize();
                    pageSize += pageSize & 1;
                    if (pageSize > AbstractPager.PageMaxSpace)
                    {
                        if (i >= currentIndex)
                        {
                            toRight = false;
                            return currentIndex;
                        }
                        return i + 1;
                    }
                }
            }

            return splitIndex;
        }

        private string GatherDetailedDebugInfo(Page rightPage, MemorySlice currentKey, MemorySlice seperatorKey, int currentIndex, int splitIndex, bool toRight)
        {
            var debugInfo = new StringBuilder();

            debugInfo.AppendFormat("\r\n_tree.Name: {0}\r\n", _tree.Name);
            debugInfo.AppendFormat("_newKey: {0}, _len: {1}, needed space: {2}\r\n", _newKey, _len, _page.GetRequiredSpace(_newKey, _len));
            debugInfo.AppendFormat("key at LastSearchPosition: {0}, current key: {1}, seperatorKey: {2}\r\n", _page.GetNodeKey(_page.LastSearchPosition), currentKey, seperatorKey);
            debugInfo.AppendFormat("currentIndex: {0}\r\n", currentIndex);
            debugInfo.AppendFormat("splitIndex: {0}\r\n", splitIndex);
            debugInfo.AppendFormat("toRight: {0}\r\n", toRight);

            debugInfo.AppendFormat("_page info: flags - {0}, # of entries {1}, size left: {2}, calculated size left: {3}\r\n", _page.Flags, _page.NumberOfEntries, _page.SizeLeft, _page.CalcSizeLeft());

            for (int i = 0; i < _page.NumberOfEntries; i++)
            {
                var node = _page.GetNode(i);
                var key = _page.GetNodeKey(node);
                debugInfo.AppendFormat("{0} - {2} {1}\r\n", key,
                    node->DataSize, node->Flags == NodeFlags.Data ? "Size" : "Page");
            }

            debugInfo.AppendFormat("rightPage info: flags - {0}, # of entries {1}, size left: {2}, calculated size left: {3}\r\n", rightPage.Flags, rightPage.NumberOfEntries, rightPage.SizeLeft, rightPage.CalcSizeLeft());

            for (int i = 0; i < rightPage.NumberOfEntries; i++)
            {
                var node = rightPage.GetNode(i);
                var key = rightPage.GetNodeKey(node);
                debugInfo.AppendFormat("{0} - {2} {1}\r\n", key,
                    node->DataSize, node->Flags == NodeFlags.Data ? "Size" : "Page");
            }
            return debugInfo.ToString();
        }
    }
}

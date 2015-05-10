using System;
using System.Diagnostics;
using System.IO;
using System.Text;
using Voron.Debugging;
using Voron.Impl;
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
        private readonly TreeMutableState _treeState;
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
            Cursor cursor,
            TreeMutableState treeState)
        {
            _tx = tx;
            _tree = tree;
            _newKey = newKey;
            _len = len;
            _pageNumber = pageNumber;
            _nodeType = nodeType;
            _nodeVersion = nodeVersion;
            _cursor = cursor;
            _treeState = treeState;
            Page page = _cursor.Pages.First.Value;
            _page = tx.ModifyPage(page.PageNumber, _tree, page);
            _cursor.Pop();
        }

        public byte* Execute()
        {
            Page rightPage = Tree.NewPage(_tx, _page.Flags, 1);
            _treeState.RecordNewPage(_page, 1);

            if (_cursor.PageCount == 0) // we need to do a root split
            {
				Page newRootPage = Tree.NewPage(_tx, _tree.KeysPrefixing ? PageFlags.Branch | PageFlags.KeysPrefixed : PageFlags.Branch, 1);
                _cursor.Push(newRootPage);
                _treeState.RootPageNumber = newRootPage.PageNumber;
                _treeState.Depth++;
                _treeState.RecordNewPage(newRootPage, 1);

                // now add implicit left page
                newRootPage.AddPageRefNode(0, _tree.KeysPrefixing ? (MemorySlice) PrefixedSlice.BeforeAllKeys : Slice.BeforeAllKeys, _page.PageNumber);
                _parentPage = newRootPage;
                _parentPage.LastSearchPosition++;
            }
            else
            {
                // we already popped the page, so the current one on the stack is the parent of the page

	            if (_tree.Name == Constants.FreeSpaceTreeName)
	            {
					// a special case for FreeSpaceTree because the allocation of a new page called above
					// can cause a delete of a free space section resulting in a run of the tree rebalancer
					// and here the parent page that exists in cursor can be outdated

					_parentPage = _tx.ModifyPage(_cursor.CurrentPage.PageNumber, _tree, null); // pass _null_ to make sure we'll get the most updated parent page
					_parentPage.LastSearchPosition = _cursor.CurrentPage.LastSearchPosition;
					_parentPage.LastMatch = _cursor.CurrentPage.LastMatch;
	            }
	            else
	            {
					_parentPage = _tx.ModifyPage(_cursor.CurrentPage.PageNumber, _tree, _cursor.CurrentPage);
	            }

                _cursor.Update(_cursor.Pages.First, _parentPage);
            }

            if (_page.IsLeaf)
            {
                _tree.ClearRecentFoundPages();
            }

	        if (_tree.Name == Constants.FreeSpaceTreeName)
	        {
				// we need to refresh the LastSearchPosition of the split page which is used by the free space handling
				// because the allocation of a new page called above could remove some sections
				// from the page that is being split

		        _page.NodePositionFor(_newKey);
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
					rightPage.AddPageRefNode(0, _tree.KeysPrefixing ? (MemorySlice) PrefixedSlice.BeforeAllKeys : Slice.BeforeAllKeys, node->PageNumber);
                    pos = AddNodeToPage(rightPage, 1);

	                var separatorKey = _page.GetNodeKey(node);

                    AddSeparatorToParentPage(rightPage.PageNumber, separatorKey);

                    _page.RemoveNode(_page.NumberOfEntries - 1);
                }
                else
                {
                    AddSeparatorToParentPage(rightPage.PageNumber, _newKey);
                    pos = AddNodeToPage(rightPage, 0);
                }
                _cursor.Push(rightPage);
                return pos;
            }

            return SplitPageInHalf(rightPage);
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
            int currentIndex = _page.LastSearchPosition;
            bool newPosition = true;
            int splitIndex = _page.NumberOfEntries/2;
            if (currentIndex < splitIndex)
                newPosition = false;

            if (_page.IsLeaf)
            {
                splitIndex = AdjustSplitPosition(currentIndex, splitIndex, ref newPosition);
            }

	        var currentKey = _page.GetNodeKey(splitIndex);

            // here the current key is the separator key and can go either way, so 
            // use newPosition to decide if it stays on the left node or moves to the right
            MemorySlice seperatorKey;
            if (currentIndex == splitIndex && newPosition)
            {
                seperatorKey = currentKey.Compare(_newKey) < 0 ? currentKey : _newKey;
            }
            else
            {
                seperatorKey = currentKey;
            }

            AddSeparatorToParentPage(rightPage.PageNumber, seperatorKey);

	        MemorySlice instance = _page.CreateNewEmptyKey();
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

            // actually insert the new key
			try
			{
				return (currentIndex > splitIndex || newPosition && currentIndex == splitIndex)
					? InsertNewKey(rightPage)
					: InsertNewKey(_page);
			}
			catch (InvalidOperationException e)
			{
				if (e.Message.StartsWith("The page is full and cannot add an entry"))
				{
					var debugInfo = new StringBuilder();

					debugInfo.AppendFormat("\r\n_tree.Name: {0}\r\n", _tree.Name);
					debugInfo.AppendFormat("_newKey: {0}, _len: {1}, needed space: {2}\r\n", _newKey, _len, _page.GetRequiredSpace(_newKey, _len));
					debugInfo.AppendFormat("currentKey: {0}, seperatorKey: {1}\r\n", currentKey, seperatorKey);
					debugInfo.AppendFormat("currentIndex: {0}\r\n", currentIndex);
					debugInfo.AppendFormat("splitIndex: {0}\r\n", splitIndex);
					debugInfo.AppendFormat("newPosition: {0}\r\n", newPosition);

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

					throw new InvalidOperationException(debugInfo.ToString(), e);
				}

				throw;
			}

        }

        private byte* InsertNewKey(Page p)
        {
            int pos = p.NodePositionFor(_newKey);

			var newKeyToInsert = p.PrepareKeyToInsert(_newKey, pos);

			if (p.HasSpaceFor(_tx, p.GetRequiredSpace(newKeyToInsert, _len)) == false)
			{
				_cursor.Push(p);

				var pageSplitter = new PageSplitter(_tx, _tree, _newKey, _len, p.PageNumber, _nodeType, _nodeVersion, _cursor, _treeState);

				return pageSplitter.Execute();
			}

            byte* dataPos = AddNodeToPage(p, pos, newKeyToInsert);
            _cursor.Push(p);
            return dataPos;
        }

        private void AddSeparatorToParentPage(long pageNumber, MemorySlice seperatorKey)
        {
	        var separatorKeyToInsert = _parentPage.PrepareKeyToInsert(seperatorKey, _parentPage.LastSearchPosition);

			if (_parentPage.HasSpaceFor(_tx, SizeOf.BranchEntry(separatorKeyToInsert) + Constants.NodeOffsetSize + SizeOf.NewPrefix(separatorKeyToInsert)) == false)
            {
                var pageSplitter = new PageSplitter(_tx, _tree, seperatorKey, -1, pageNumber, NodeFlags.PageRef,
                    0, _cursor, _treeState);
                pageSplitter.Execute();
            }
            else
            {
                _parentPage.NodePositionFor(seperatorKey); // select the appropriate place for this
				_parentPage.AddPageRefNode(_parentPage.LastSearchPosition, separatorKeyToInsert, pageNumber);
            }
        }

        private int AdjustSplitPosition(int currentIndex, int splitIndex,
            ref bool newPosition)
        {
	        MemorySlice keyToInsert;

	        if (_tree.KeysPrefixing)
		        keyToInsert = new PrefixedSlice(_newKey); // let's assume that _newkey won't be prefixed to ensure the destination page will have enough space
	        else
		        keyToInsert = _newKey;

	        var pageSize = SizeOf.NodeEntry(AbstractPager.PageMaxSpace, keyToInsert , _len) + Constants.NodeOffsetSize;

			if(_tree.KeysPrefixing)
				pageSize += (Constants.PrefixNodeHeaderSize + 1); // let's assume that prefix will be created to ensure the destination page will have enough space, + 1 because prefix node might require 2-byte alignment

            if (currentIndex <= splitIndex)
            {
                newPosition = false;
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
                                newPosition = true;
                            return currentIndex;
                        }
                        return (ushort) i;
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
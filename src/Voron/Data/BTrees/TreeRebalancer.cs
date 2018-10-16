using System;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using Sparrow;
using Voron.Data.Compression;
using Voron.Global;
using Voron.Impl;
using Voron.Impl.FreeSpace;
using Voron.Impl.Paging;

namespace Voron.Data.BTrees
{
    public unsafe class TreeRebalancer
    {
        private readonly LowLevelTransaction _tx;
        private readonly Tree _tree;
        private readonly TreeCursor _cursor;

        public TreeRebalancer(LowLevelTransaction tx, Tree tree, TreeCursor cursor)
        {
            _tx = tx;
            _tree = tree;
            _cursor = cursor;
        }

        private FreeSpaceHandlingDisabler DisableFreeSpaceUsageIfSplittingRootTree()
        {
            if (_tree == _tx.RootObjects)
            {
                return _tx.Environment.FreeSpaceHandling.Disable();
            }
            return new FreeSpaceHandlingDisabler();
        }

        public TreePage Execute(TreePage page)
        {
            using (DisableFreeSpaceUsageIfSplittingRootTree())
            {
                _tree.ClearPagesCache();
                if (_cursor.PageCount <= 1) // the root page
                {
                    RebalanceRoot(page);
                    return null;
                }

                _cursor.Pop();

                var parentPage = _tree.ModifyPage(_cursor.CurrentPage);
                _cursor.Update(_cursor.Pages, parentPage);

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

                    _tree.FreePage(page);

                    return parentPage;
                }

                if (page.IsBranch && page.NumberOfEntries == 1)
                {
                    RemoveBranchWithOneEntry(page, parentPage);

                    return parentPage;
                }

                var minKeys = page.IsBranch ? 2 : 1;
                if ((page.UseMoreSizeThan(_tx.DataPager.PageMinSpace)) && page.NumberOfEntries >= minKeys)
                    return null; // above space/keys thresholds

                Debug.Assert(parentPage.NumberOfEntries >= 2); // if we have less than 2 entries in the parent, the tree is invalid

                var sibling = SetupMoveOrMerge(page, parentPage);
                Debug.Assert(sibling.PageNumber != page.PageNumber);

                if (page.TreeFlags != sibling.TreeFlags)
                    return null;

                if (sibling.IsCompressed)
                    return null;

                if (sibling.PageSize != page.PageSize)
                    // if the current page is compressed (but already opened), we need to 
                    // avoid merging it with the right (uncompressed) page
                    return null;

                Debug.Assert(page.IsCompressed == false);

                minKeys = sibling.IsBranch ? 2 : 1; // branch must have at least 2 keys
                if (sibling.UseMoreSizeThan(_tx.DataPager.PageMinSpace) &&
                    sibling.NumberOfEntries > minKeys)
                {
                    // neighbor is over the min size and has enough key, can move just one key to  the current page
                    if (page.IsBranch)
                        MoveBranchNode(parentPage, sibling, page);
                    else
                        MoveLeafNode(parentPage, sibling, page);

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

                return parentPage;
            }
        }

        private void RemoveBranchWithOneEntry(TreePage page, TreePage parentPage)
        {
            var pageRefNumber = page.GetNode(0)->PageNumber;

            TreeNodeHeader* nodeHeader = null;

            for (int i = 0; i < parentPage.NumberOfEntries; i++)
            {
                nodeHeader = parentPage.GetNode(i);

                if (nodeHeader->PageNumber == page.PageNumber)
                    break;
            }

            Debug.Assert(nodeHeader->PageNumber == page.PageNumber);

            nodeHeader->PageNumber = pageRefNumber;

            _tree.FreePage(page);
        }

        private bool TryMergePages(TreePage parentPage, TreePage left, TreePage right)
        {
            TemporaryPage tmp;
            using (_tx.Environment.GetTemporaryPage(_tx, out tmp))
            {
                var mergedPage = tmp.GetTempPage();
                Memory.Copy(mergedPage.Base, left.Base, left.PageSize);

                var previousSearchPosition = right.LastSearchPosition;

                for (int i = 0; i < right.NumberOfEntries; i++)
                {
                    right.LastSearchPosition = i;

                    Slice key;
                    using (GetActualKey(right, right.LastSearchPositionOrLastEntry, out key))
                    {

                        var node = right.GetNode(i);

                        if (mergedPage.HasSpaceFor(_tx, TreeSizeOf.NodeEntryWithAnotherKey(node, key) + Constants.Tree.NodeOffsetSize) == false)
                        {
                            right.LastSearchPosition = previousSearchPosition;
                            //previous position --> prevent mutation of parameter
                            return false;
                        }

                        mergedPage.CopyNodeDataToEndOfPage(node, key);
                    }
                }

                Memory.Copy(left.Base, mergedPage.Base, left.PageSize);
            }

            parentPage.RemoveNode(parentPage.LastSearchPositionOrLastEntry); // unlink the right sibling
            _tree.FreePage(right);

            return true;
        }

        private TreePage SetupMoveOrMerge(TreePage page, TreePage parentPage)
        {
            TreePage sibling;
            if (parentPage.LastSearchPosition == 0) // we are the left most item
            {
                sibling = _tree.ModifyPage(parentPage.GetNode(1)->PageNumber);

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
                sibling = _tree.ModifyPage(parentPage.GetNode(parentPage.LastSearchPosition)->PageNumber);
                parentPage.LastSearchPosition++;
                if (beyondLast)
                    parentPage.LastSearchPosition++;
                sibling.LastSearchPosition = sibling.NumberOfEntries - 1;
                page.LastSearchPosition = 0;
            }
            return sibling;
        }

        private void MoveLeafNode(TreePage parentPage, TreePage from, TreePage to)
        {
            Debug.Assert(from.IsBranch == false);
            Slice originalFromKeyStart;
            using (GetActualKey(from, from.LastSearchPositionOrLastEntry, out originalFromKeyStart))
            {
                var fromNode = from.GetNode(from.LastSearchPosition);
                byte* val = @from.Base + @from.KeysOffsets[@from.LastSearchPosition] + Constants.Tree.NodeHeaderSize +
                            originalFromKeyStart.Size;

                byte* dataPos;
                var fromDataSize = fromNode->DataSize;
                switch (fromNode->Flags)
                {
                    case TreeNodeFlags.PageRef:
                        to.EnsureHasSpaceFor(_tx, originalFromKeyStart, -1);
                        dataPos = to.AddPageRefNode(to.LastSearchPosition, originalFromKeyStart, fromNode->PageNumber);
                        break;
                    case TreeNodeFlags.Data:
                        to.EnsureHasSpaceFor(_tx, originalFromKeyStart, fromDataSize);
                        dataPos = to.AddDataNode(to.LastSearchPosition, originalFromKeyStart, fromDataSize);
                        break;
                    case TreeNodeFlags.MultiValuePageRef:
                        to.EnsureHasSpaceFor(_tx, originalFromKeyStart, fromDataSize);
                        dataPos = to.AddMultiValueNode(to.LastSearchPosition, originalFromKeyStart, fromDataSize);
                        break;
                    default:
                        throw new NotSupportedException("Invalid node type to move: " + fromNode->Flags);
                }

                if (dataPos != null && fromDataSize > 0)
                    Memory.Copy(dataPos, val, fromDataSize);

                from.RemoveNode(from.LastSearchPositionOrLastEntry);

                var pos = parentPage.LastSearchPositionOrLastEntry;
                parentPage.RemoveNode(pos);

                Slice newSeparatorKey;
                var scope = GetActualKey(to, 0, out newSeparatorKey); // get the next smallest key it has now
                try
                {
                    var pageNumber = to.PageNumber;
                    if (parentPage.GetNode(0)->PageNumber == to.PageNumber)
                    {
                        pageNumber = from.PageNumber;
                        scope.Dispose();
                        scope = GetActualKey(from, 0, out newSeparatorKey);
                    }

                    AddSeparatorToParentPage(to, parentPage, pageNumber, newSeparatorKey, pos);
                }
                finally
                {
                    scope.Dispose();
                }
            }
        }

        private void AddSeparatorToParentPage(TreePage childPage, TreePage parentPage, long pageNumber, Slice seperatorKey, int separatorKeyPosition)
        {
            var parent = new ParentPageAction(parentPage, childPage, _tree, _cursor, _tx);

            parent.AddSeparator(seperatorKey, pageNumber, separatorKeyPosition);
        }

        private void MoveBranchNode(TreePage parentPage, TreePage from, TreePage to)
        {
            Debug.Assert(from.IsBranch);

            Slice originalFromKey;
            using (GetActualKey(from, from.LastSearchPositionOrLastEntry, out originalFromKey))
            {
                to.EnsureHasSpaceFor(_tx, originalFromKey, -1);

                var fromNode = from.GetNode(from.LastSearchPosition);
                long pageNum = fromNode->PageNumber;

                if (to.LastSearchPosition == 0)
                {
                    // cannot add to left implicit side, adjust by moving the left node
                    // to the right by one, then adding the new one as the left

                    TreeNodeHeader* actualKeyNode;
                    Slice implicitLeftKey;
                    using (GetActualKey(to, 0, out actualKeyNode, out implicitLeftKey))
                    {
                        var implicitLeftNode = to.GetNode(0);
                        var leftPageNumber = implicitLeftNode->PageNumber;

                        Slice implicitLeftKeyToInsert;
                        ByteStringContext.ExternalScope? externalScope;

                        if (implicitLeftNode == actualKeyNode)
                        {
                            externalScope = TreeNodeHeader.ToSlicePtr(_tx.Allocator, actualKeyNode, out implicitLeftKeyToInsert);
                        }
                        else
                        {
                            implicitLeftKeyToInsert = implicitLeftKey;
                            externalScope = null;
                        }

                        to.EnsureHasSpaceFor(_tx, implicitLeftKeyToInsert, -1);
                        to.AddPageRefNode(1, implicitLeftKeyToInsert, leftPageNumber);
                        externalScope?.Dispose();

                        to.ChangeImplicitRefPageNode(pageNum); // setup the new implicit node
                    }
                }
                else
                {
                    to.AddPageRefNode(to.LastSearchPosition, originalFromKey, pageNum);
                }
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
            Slice newSeparatorKey;
            var scope = GetActualKey(to, 0, out newSeparatorKey); // get the next smallest key it has now
            try
            {
                var pageNumber = to.PageNumber;
                if (parentPage.GetNode(0)->PageNumber == to.PageNumber)
                {
                    pageNumber = from.PageNumber;
                    scope.Dispose();
                    scope = GetActualKey(from, 0, out newSeparatorKey);
                }

                AddSeparatorToParentPage(to, parentPage, pageNumber, newSeparatorKey, pos);
            }
            finally
            {
                scope.Dispose();
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private ActualKeyScope GetActualKey(TreePage page, int pos, out Slice slice)
        {
            TreeNodeHeader* _;
            return GetActualKey(page, pos, out _, out slice);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private ActualKeyScope GetActualKey(TreePage page, int pos, out TreeNodeHeader* node, out Slice key)
        {
            DecompressedLeafPage decompressedLeafPage = null;
            node = page.GetNode(pos);
            var scope = TreeNodeHeader.ToSlicePtr(_tx.Allocator, node, out key);
            while (key.Size == 0)
            {
                Debug.Assert(page.IsBranch);
                page = _tree.GetReadOnlyTreePage(node->PageNumber);
                if (page.IsCompressed == false)
                    node = page.GetNode(0);
                else
                {
                    decompressedLeafPage?.Dispose();
                    decompressedLeafPage = _tree.DecompressPage(page, skipCache: true);

                    if (decompressedLeafPage.NumberOfEntries > 0)
                    {
                        if (page.NumberOfEntries == 0)
                            node = decompressedLeafPage.GetNode(0);
                        else
                        {
                            // we want to find the smallest key in compressed page
                            // it can be inside compressed part or not compressed one
                            // in particular, it can be the key of compression tombstone node that we don't see after decompression
                            // so we need to take first keys from decompressed and compressed page and compare them

                            var decompressedNode = decompressedLeafPage.GetNode(0);
                            var compressedNode = page.GetNode(0);

                            using (TreeNodeHeader.ToSlicePtr(_tx.Allocator, decompressedNode, out var firstDecompressedKey))
                            using (TreeNodeHeader.ToSlicePtr(_tx.Allocator, compressedNode, out var firstCompressedKey))
                            {
                                node = SliceComparer.CompareInline(firstDecompressedKey, firstCompressedKey) > 0 ? compressedNode : decompressedNode;
                            }
                        }
                    }
                    else
                    {
                        // we have empty page after decompression (each compressed entry has a corresponding CompressionTombstone)
                        // we can safely use the node key of first tombstone (they have proper order)

                        node = page.GetNode(0);
                    }
                }

                scope.Dispose();
                scope = TreeNodeHeader.ToSlicePtr(_tx.Allocator, node, out key);
            }

            return new ActualKeyScope
            {
                DecompressedLeafPage = decompressedLeafPage,
                ExternalScope = scope
            };
        }

        private void RebalanceRoot(TreePage page)
        {
            if (page.NumberOfEntries == 0)
                return; // nothing to do 
            if (!page.IsBranch || page.NumberOfEntries > 1)
            {
                return; // cannot do anything here
            }
            // in this case, we have a root pointer with just one pointer, we can just swap it out

            var node = page.GetNode(0);
            Debug.Assert(node->Flags == (TreeNodeFlags.PageRef));

            var rootPage = _tree.ModifyPage(node->PageNumber);
            _tree.State.RootPageNumber = rootPage.PageNumber;
            _tree.State.Depth--;

            Debug.Assert(rootPage.Dirty);

            _cursor.Pop();
            _cursor.Push(rootPage);

            _tree.FreePage(page);
        }

        public struct ActualKeyScope : IDisposable
        {
            public ByteStringContext.ExternalScope ExternalScope;

            public DecompressedLeafPage DecompressedLeafPage;

            public void Dispose()
            {
                ExternalScope.Dispose();
                DecompressedLeafPage?.Dispose();
            }
        }
    }
}

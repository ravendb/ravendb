using System;
using System.Diagnostics;
using Sparrow;
using Sparrow.Binary;
using Sparrow.Server;
using Voron.Exceptions;
using Voron.Util;
using Constants = Voron.Global.Constants;

namespace Voron.Data.BTrees
{
    /* Multi tree behavior
     * -------------------
     * A multi tree is a tree that is used only with MultiRead, MultiAdd, MultiDelete
     * The common use case is a secondary index that allows duplicates. 
     * 
     * The API exposed goes like this:
     * 
     * MultiAdd("key", "val1"), MultiAdd("key", "val2"), MultiAdd("key", "val3") 
     * 
     * And then you can read it back with MultiRead("key") : IIterator
     * 
     * When deleting, you delete one value at a time: MultiDelete("key", "val1")
     * 
     * The actual values are stored as keys in a separate tree per key. In order to optimize
     * space usage, multi trees work in the following fashion.
     * 
     * If the total size of the values per key is less than NodeMaxSize, we store them as an embedded
     * page inside the owning tree. If then are more than the node max size, we create a separate tree
     * for them and then only store the tree root information.
     */
    public unsafe partial class Tree
    {
        private static void ValidateValuesForMultiBulkAdd(int maxNodeSize, ReadOnlySpan<Slice> values)
        {
            for (int i = 0; i < values.Length; i++)
            {
                if (values[i].HasValue == false)
                    throw new ArgumentNullException($"{nameof(values)}[{i}]");

                if (values[i].Size > maxNodeSize)
                    throw new ArgumentException("Cannot add a value to child tree that is over " + maxNodeSize + " bytes in size", $"{nameof(values)}[{i}]");
                if (values[i].Size == 0)
                    throw new ArgumentException("Cannot add empty value to child tree", $"{nameof(values)}[{i}]");
            }
        }

        [Conditional("DEBUG")]
        private static void EnsureValuesAreSorted(ReadOnlySpan<Slice> values)
        {
            for (int i = 1; i < values.Length; i++)
            {
                var cmp = SliceComparer.Compare(values[i - 1], values[i]);
                switch (cmp)
                {
                    case > 0:
                        throw new ArgumentException("Values must be sorted", nameof(values));
                    case 0:
                        throw new ArgumentException("Values must be unique", nameof(values));
                }
            }
        }
        
        public bool IsMultiValueTree { get; set; }

        /// <summary>
        /// Adds multiple values to the multi-tree.
        /// </summary>
        /// <param name="key">The key associated with the values.</param>
        /// <param name="values">The values to insert. Requirements: must be sorted and deduplicated.</param>
        public void MultiBulkAdd(Slice key, ReadOnlySpan<Slice> values)
        {
            switch (values.Length)
            {
                case 0: 
                    return;
                case 1:
                    MultiAdd(key, values[0]);
                    return;
            }
            
            int maxNodeSize = Llt.DataPager.NodeMaxSize;
            EnsureValuesAreSorted(values);
            ValidateValuesForMultiBulkAdd(maxNodeSize, values);
            
            // This is a new tree, we've to put a flag in the header.
            if ((State.Header.Flags & TreeFlags.MultiValueTrees) != TreeFlags.MultiValueTrees)
            {
                ref var state = ref State.Modify();
                state.Flags |= TreeFlags.MultiValueTrees;
            }

            var page = FindPageFor(key, out _);
            if (page is not { LastMatch: 0 }) // Key is not in the tree. Use an optimized path for this scenario.
            {
                MultiBulkAddOnNewValue(key, values, maxNodeSize);
                return;
            }

            ContextBoundNativeList<int> newItems;
            {
                var readonlyKeyItem = page.GetNode(page.LastSearchPosition);
                if (readonlyKeyItem->Flags == TreeNodeFlags.MultiValuePageRef)
                {

                    // We have an inner tree for the key. Since our values are sorted,
                    // we can iterate over the elements and add them one by one.
                    // As the page is cached, it is already loaded into memory. 
                    // Therefore, this path could be further optimized by batching them into a single page.
                    var existingTree = OpenMultiValueTree(key, readonlyKeyItem);
                    for (int i = 0; i < values.Length; i++)
                    {
                        existingTree.DataDirectAdd(values[i], 0).Dispose();
                    }
                    return;
                }
                
                if (readonlyKeyItem->Flags == TreeNodeFlags.PageRef)
                    throw new InvalidOperationException("Multi trees don't use overflows");
                
                // We have a nested page here, which means our values are inlined into the page.
                // Since we want to avoid modifying the page unless it is necessary,
                // we will prepare a list of new terms based on the loaded data.
                // This way, we can avoid modifying the page if we don't have to.
                newItems = GetListOfNewItems(ref readonlyKeyItem, values);
            }

            if (newItems.Count == 0)
            {
                // Everything is already in the tree, nothing to do here.
                newItems.Dispose();
                return;
            }

            // We know there are new insertions. Let's try to insert them.
            // Since we scoped the readonly keys in the previous scope,
            // we are guaranteed not to use invalid structures, but we need to recreate them.
            {
                page = ModifyPage(page);
                var keyItem = page.GetNode(page.LastSearchPosition);
                
                for (int i = 0; i < newItems.Count; i++)
                {
                    GetNestedPagePointer(page, out var nestedPage, out byte* nestedPagePtr);
                    var currentProcessingTerm = values[newItems[i]];
                    nestedPage.Search(_llt, currentProcessingTerm);

                    if (nestedPage.HasSpaceFor(_llt, currentProcessingTerm, 0))
                    {
                        nestedPage.AddDataNode(nestedPage.LastSearchPosition, currentProcessingTerm, 0);
                        continue;
                    }

                    if (page.HasSpaceFor(_llt, currentProcessingTerm, 0))
                    {
                        // page has space for an additional node in nested page ...
                        var requiredSpace = nestedPage.PageSize + // existing page
                                            nestedPage.GetRequiredSpace(currentProcessingTerm, 0); // new node

                        if (requiredSpace + Constants.Tree.NodeHeaderSize <= maxNodeSize)
                        {
                            // ... and it won't require to create an overflow, so we can just expand the current value, no need to create a nested tree yet

                            EnsureNestedPagePointer(page, keyItem, ref nestedPage, ref nestedPagePtr);

                            var newPageSize = (ushort)Math.Min(Bits.PowerOf2(requiredSpace), maxNodeSize - Constants.Tree.NodeHeaderSize);

                            ExpandMultiTreeNestedPageSize(key, currentProcessingTerm, nestedPagePtr, newPageSize, nestedPage.PageSize);
                            
                            // We may change the page. Ensure all pointers are valid as well.
                            page = ModifyPage(SearchForPage(key, out _)); 
                            continue;
                        }
                    }
                    
                    // There is no space for the new value, so we need to create a inner tree.
                    EnsureNestedPagePointer(page, keyItem, ref nestedPage, ref nestedPagePtr);
                    var tree = Create(_llt, _tx, key, TreeFlags.MultiValue);
                    for (int nestedPageItemIdx = 0; nestedPageItemIdx < nestedPage.NumberOfEntries; nestedPageItemIdx++)
                    {
                        using (nestedPage.GetNodeKey(_llt, nestedPageItemIdx, out Slice existingValue))
                        {
                            tree.DataDirectAdd(existingValue, 0).Dispose();
                        }
                    }

                    for (; i < newItems.Count; i++)
                        tree.DataDirectAdd(values[newItems[i]], 0).Dispose();
                    
                    // Register the new tree.
                    _tx.AddMultiValueTree(this, key, tree);
                    DirectAdd(key, sizeof(TreeRootHeader), TreeNodeFlags.MultiValuePageRef,out byte* _).Dispose();
                }
                newItems.Dispose();
            }
        }

        private ContextBoundNativeList<int> GetListOfNewItems(ref TreeNodeHeader* item, ReadOnlySpan<Slice> values)
        {
            var newItemsIndexes = new ContextBoundNativeList<int>(_tx.Allocator);
            var nestedPagePtr = DirectAccessFromHeader(item);
            var nestedPage = new TreePage(nestedPagePtr, (ushort)GetDataSize(item));
            for (int i = 0; i < values.Length; i++)
            {
                var itemInTree = nestedPage.Search(_tx.LowLevelTransaction, values[i]);
                if (itemInTree != null && nestedPage.LastMatch == 0)
                {
                    using var _ = TreeNodeHeader.ToSlicePtr(_tx.Allocator, itemInTree, out Slice fetchedNodeKey);
                    if (SliceComparer.Equals(values[i], fetchedNodeKey))
                        continue;
                }
                
                newItemsIndexes.Add(i);
            }
            
            return newItemsIndexes;
        }
        
        public void MultiAdd(Slice key, Slice value)
        {
            if (!value.HasValue)
                throw new ArgumentNullException(nameof(value));

            int maxNodeSize = Llt.DataPager.NodeMaxSize;
            if (value.Size > maxNodeSize)
                throw new ArgumentException("Cannot add a value to child tree that is over " + maxNodeSize + " bytes in size", nameof(value));
            if (value.Size == 0)
                throw new ArgumentException("Cannot add empty value to child tree");

            if ((State.Header.Flags & TreeFlags.MultiValueTrees) != TreeFlags.MultiValueTrees)
            {
                ref var state = ref State.Modify();
                state.Flags |= TreeFlags.MultiValueTrees;
            }

            var page = FindPageFor(key, out _);
            if (page == null || page.LastMatch != 0)
            {
                MultiAddOnNewValue(key, value, maxNodeSize);
                return;
            }
            
            var item = page.GetNode(page.LastSearchPosition);

            // already was turned into a multi tree, not much to do here
            if (item->Flags == TreeNodeFlags.MultiValuePageRef)
            {
                var existingTree = OpenMultiValueTree(key, item);
                existingTree.DataDirectAdd(value, 0).Dispose();
                return;
            }
            
            if (item->Flags == TreeNodeFlags.PageRef)
                throw new InvalidOperationException("Multi trees don't use overflows");

            var nestedPagePtr = DirectAccessFromHeader(item);

            var nestedPage = new TreePage(nestedPagePtr, (ushort)GetDataSize(item));
            var existingItem = nestedPage.Search(_llt, value);
            if (nestedPage.LastMatch == 0)
            {
                using (TreeNodeHeader.ToSlicePtr(_llt.Allocator, existingItem, out Slice valueFromNestedPage))
                {
                    if (SliceComparer.Equals(value, valueFromNestedPage))
                        return; // already there, turning into a no-op
                }
            }

            // refresh data on readable page
            page = ModifyPage(page);
            item = page.GetNode(page.LastSearchPosition);
            nestedPagePtr = DirectAccessFromHeader(item);
            nestedPage = new TreePage(nestedPagePtr, (ushort)GetDataSize(item))
            {
                LastMatch = nestedPage.LastMatch,
                LastSearchPosition = nestedPage.LastSearchPosition
            };
            
            if (nestedPage.HasSpaceFor(_llt, value, 0))
            {
                // we are now working on top of the modified root page, we can just modify the memory directly
                nestedPage.AddDataNode(nestedPage.LastSearchPosition, value, 0);
                return;
            }

            if (page.HasSpaceFor(_llt, value, 0))
            {
                // page has space for an additional node in nested page ...

                var requiredSpace = nestedPage.PageSize + // existing page
                                    nestedPage.GetRequiredSpace(value, 0); // new node

                if (requiredSpace + Constants.Tree.NodeHeaderSize <= maxNodeSize)
                {
                    // ... and it won't require to create an overflow, so we can just expand the current value, no need to create a nested tree yet

                    EnsureNestedPagePointer(page, item, ref nestedPage, ref nestedPagePtr);

                    var newPageSize = (ushort)Math.Min(Bits.PowerOf2(requiredSpace), maxNodeSize - Constants.Tree.NodeHeaderSize);

                    ExpandMultiTreeNestedPageSize(key, value, nestedPagePtr, newPageSize, nestedPage.PageSize);

                    return;
                }
            }

            EnsureNestedPagePointer(page, item, ref nestedPage, ref nestedPagePtr);

            // we now have to convert this into a tree instance, instead of just a nested page
            var tree = Create(_llt, _tx, key, TreeFlags.MultiValue);
            for (int i = 0; i < nestedPage.NumberOfEntries; i++)
            {
                using (nestedPage.GetNodeKey(_llt, i, out Slice existingValue))
                {
                    tree.DataDirectAdd(existingValue, 0).Dispose();
                }
            }
            
            tree.DataDirectAdd(value, 0).Dispose();
            _tx.AddMultiValueTree(this, key, tree);
            // we need to record that we switched to tree mode here, so the next call wouldn't also try to create the tree again
            DirectAdd(key, sizeof(TreeRootHeader), TreeNodeFlags.MultiValuePageRef,out byte* _).Dispose();
        }

        private void ExpandMultiTreeNestedPageSize(Slice key, Slice value, byte* nestedPagePtr, ushort newSize, int currentSize)
        {
            Debug.Assert(newSize > currentSize);

            using (_llt.GetTempPage(currentSize, out var tmp))
            {
                var tempPagePointer = tmp.Base;
                Memory.Copy(tempPagePointer, nestedPagePtr, currentSize);
                Delete(key); // release our current page
                TreePage nestedPage = new TreePage(tempPagePointer, (ushort)currentSize);

                using (DirectAdd(key, newSize,out var ptr))
                {
                    var newNestedPage = new TreePage(ptr, newSize)
                    {
                        Lower = (ushort) Constants.Tree.PageHeaderSize,
                        Upper = newSize,
                        TreeFlags = TreePageFlags.Leaf,
                        PageNumber = -1L, // mark as invalid page number
                        Flags = 0
                    };

                    ByteStringContext allocator = _llt.Allocator;
                    for (int i = 0; i < nestedPage.NumberOfEntries; i++)
                    {
                        var nodeHeader = nestedPage.GetNode(i);

                        using (TreeNodeHeader.ToSlicePtr(allocator, nodeHeader, out Slice nodeKey))
                            newNestedPage.AddDataNode(i, nodeKey, 0);
                    }

                    newNestedPage.Search(_llt, value);
                    newNestedPage.AddDataNode(newNestedPage.LastSearchPosition, value, 0);
                }
            }
        }

        private void MultiBulkAddOnNewValue(Slice key, ReadOnlySpan<Slice> values, int maxNodeSize)
        {
            var requiredPageSize = Constants.Tree.PageHeaderSize + Constants.Tree.NodeHeaderSize;
            for (int i = 0; i < values.Length; i++)
            {
                requiredPageSize += TreeSizeOf.NodeEntry(-1, values[i], 0) + Constants.Tree.NodeHeaderSize;
            }
            
            if (requiredPageSize > maxNodeSize)
            {
                var tree = Create(_llt, _tx, key, TreeFlags.MultiValue);
                _tx.AddMultiValueTree(this, key, tree);
                DirectAdd(key, sizeof(TreeRootHeader), TreeNodeFlags.MultiValuePageRef,out _).Dispose();
                
                foreach (var value in values)
                    tree.DataDirectAdd(value, 0).Dispose();

                return;
            }
            
            // Now we know we can fit all values in a single nestedPage node.
            var actualPageSize = (ushort)Math.Min(Bits.PowerOf2(requiredPageSize), maxNodeSize - Constants.Tree.NodeHeaderSize);

            using (DirectAdd(key, actualPageSize, out byte* ptr))
            {
                var nestedPage = new TreePage(ptr, actualPageSize)
                {
                    PageNumber = -1L, // hint that this is an inner page
                    Lower = (ushort)Constants.Tree.PageHeaderSize,
                    Upper = actualPageSize,
                    TreeFlags = TreePageFlags.Leaf,
                    Flags = 0
                };
                
                // since values are sorted, we know the order that they will be added to the tree
                for (int itemIdx = 0; itemIdx < values.Length; itemIdx++)
                {
                    var value = values[itemIdx];
                    nestedPage.AddDataNode(itemIdx, value, 0);
                }
            }
        }

        private void MultiAddOnNewValue(Slice key, Slice value, int maxNodeSize)
        {
            var requiredPageSize = Constants.Tree.PageHeaderSize + // header of a nested page
                                   Constants.Tree.NodeOffsetSize +   // one node in a nested page
                                   TreeSizeOf.LeafEntry(-1, value, 0); // node header and its value

            if (requiredPageSize + Constants.Tree.NodeHeaderSize > maxNodeSize)
            {
                // no choice, very big value, we might as well just put it in its own tree from the get go...
                // otherwise, we would have to put this in overflow page, and that won't save us any space anyway

                var tree = Create(_llt, _tx, key, TreeFlags.MultiValue);

                tree.DataDirectAdd(value, 0).Dispose();
                _tx.AddMultiValueTree(this, key, tree);

                DirectAdd(key, sizeof(TreeRootHeader), TreeNodeFlags.MultiValuePageRef, out byte* _).Dispose();
                return;
            }

            var actualPageSize = (ushort)Math.Min(Bits.PowerOf2(requiredPageSize), maxNodeSize - Constants.Tree.NodeHeaderSize);

            using (DirectAdd(key, actualPageSize, out byte* ptr))
            {
                var nestedPage = new TreePage(ptr, actualPageSize)
                {
                    PageNumber = -1L, // hint that this is an inner page
                    Lower = (ushort) Constants.Tree.PageHeaderSize,
                    Upper = actualPageSize,
                    TreeFlags = TreePageFlags.Leaf,
                    Flags = 0
                };

                nestedPage.AddDataNode(0, value, 0);
            }
        }

        public void MultiDelete(Slice key, Slice value)
        {
            var page = FindPageFor(key, out TreeNodeHeader* _);
            if (page == null || page.LastMatch != 0)
            {
                return; //nothing to delete - key not found
            }

            page = ModifyPage(page);

            var item = page.GetNode(page.LastSearchPosition);

            if (item->Flags == TreeNodeFlags.MultiValuePageRef) //multi-value tree exists
            {
                var tree = OpenMultiValueTree(key, item);

                tree.Delete(value);

                // previously, we would convert back to a simple model if we dropped to a single entry
                // however, it doesn't really make sense, once you got enough values to go to an actual nested 
                // tree, you are probably going to remain that way, or be removed completely.
                if (tree.State.Header.NumberOfEntries != 0)
                    return;
                _tx.TryRemoveMultiValueTree(this, key);
                if (_newPageAllocator != null)
                {
                    if (IsIndexTree == false)
                        ThrowAttemptToFreePageToNewPageAllocator(Name, tree.State.Header.RootPageNumber);

                    _newPageAllocator.FreePage(tree.State.Header.RootPageNumber);
                }
                else
                {
                    if (IsIndexTree)
                        ThrowAttemptToFreeIndexPageToFreeSpaceHandling(Name, tree.State.Header.RootPageNumber);

                    _llt.FreePage(tree.State.Header.RootPageNumber);
                }

                Delete(key);
            }
            else // we use a nested page here
            {
                var nestedPage = new TreePage(DirectAccessFromHeader(item), (ushort)GetDataSize(item));

                nestedPage.Search(_llt, value);// need to search the value in the nested page

                if (nestedPage.LastMatch != 0) // value not found
                    return;

                if (item->Flags == TreeNodeFlags.PageRef)
                    throw new InvalidOperationException("Multi trees don't use overflows");

                var nestedPagePtr = DirectAccessFromHeader(item);

                nestedPage = new TreePage(nestedPagePtr, (ushort)GetDataSize(item))
                {
                    LastSearchPosition = nestedPage.LastSearchPosition
                };
                
                nestedPage.RemoveNode(nestedPage.LastSearchPosition);
                if (nestedPage.NumberOfEntries == 0)
                    Delete(key);
            }
        }

        public long MultiCount(Slice key)
        {
            var page = FindPageFor(key, out TreeNodeHeader* node);
            if (page == null || page.LastMatch != 0)
                return 0;

            Debug.Assert(node != null);

            using (TreeNodeHeader.ToSlicePtr(_llt.Allocator, node, out Slice fetchedNodeKey))
            {
                if (SliceComparer.Equals(fetchedNodeKey, key) == false)
                {
                    VoronUnrecoverableErrorException.Raise(_llt, "Was unable to retrieve the correct node. Data corruption possible");
                }
            }

            if (node->Flags == TreeNodeFlags.MultiValuePageRef)
            {
                var tree = OpenMultiValueTree(key, node);

                return tree.State.Header.NumberOfEntries;
            }

            var nestedPage = new TreePage(DirectAccessFromHeader(node), (ushort)GetDataSize(node));

            return nestedPage.NumberOfEntries;
        }

        public IIterator MultiRead(Slice key)
        {
            var page = FindPageFor(key, out TreeNodeHeader* node);
            if (page == null || page.LastMatch != 0)
                return new EmptyIterator();

            Debug.Assert(node != null);

            using (TreeNodeHeader.ToSlicePtr(_llt.Allocator, node, out Slice fetchedNodeKey))
            {
                if (SliceComparer.Equals(fetchedNodeKey, key) == false)
                {
                    VoronUnrecoverableErrorException.Raise(_llt, "Was unable to retrieve the correct node. Data corruption possible");
                }
            }

            if (node->Flags == TreeNodeFlags.MultiValuePageRef)
            {
                var tree = OpenMultiValueTree(key, node);

                return tree.Iterate(true);
            }

            var ptr = DirectAccessFromHeader(node);
            var nestedPage = new TreePage(ptr, (ushort)GetDataSize(node));

            return new TreePageIterator(_llt, key, this, nestedPage);
        }

        private Tree OpenMultiValueTree(Slice key, TreeNodeHeader* item)
        {
            if (_tx.TryGetMultiValueTree(this, key, out Tree tree))
                return tree;

            var childTreeHeader = (TreeRootHeader*)((byte*)item + item->KeySize + Constants.Tree.NodeHeaderSize);

            Debug.Assert(childTreeHeader->RootPageNumber < _llt.State.NextPageNumber);
            Debug.Assert(childTreeHeader->Flags == TreeFlags.MultiValue);

            tree = Open(_llt, _tx, key, *childTreeHeader);
            _tx.AddMultiValueTree(this, key, tree);
            return tree;
        }

        private bool TryOverwriteDataOrMultiValuePageRefNode(TreeNodeHeader* updatedNode, int len,
                                                             TreeNodeFlags requestedNodeType, out byte* pos)
        {
            switch (requestedNodeType)
            {
                case TreeNodeFlags.Data:
                case TreeNodeFlags.MultiValuePageRef:
                    {
                        if (updatedNode->DataSize == len &&
                            (updatedNode->Flags == TreeNodeFlags.Data || updatedNode->Flags == TreeNodeFlags.MultiValuePageRef))
                        {
                            updatedNode->Flags = requestedNodeType;

                            pos = (byte*)updatedNode + Constants.Tree.NodeHeaderSize + updatedNode->KeySize;
                            return true;
                        }
                        break;
                    }
                case TreeNodeFlags.PageRef:
                    throw new InvalidOperationException("We never add PageRef explicitly");
                default:
                    throw new ArgumentOutOfRangeException();
            }
            pos = null;
            return false;
        }

        private void GetNestedPagePointer(TreePage page, out TreePage nestedPage, out byte* nestedPagePtr)
        {
            var movedItem = page.GetNode(page.LastSearchPosition);
            nestedPagePtr = DirectAccessFromHeader(movedItem);
            nestedPage = new TreePage(nestedPagePtr, (ushort)GetDataSize(movedItem));
        }
        
        private void EnsureNestedPagePointer(TreePage page, TreeNodeHeader* currentItem, ref TreePage nestedPage, ref byte* nestedPagePtr)
        {
            var movedItem = page.GetNode(page.LastSearchPosition);

            if (movedItem == currentItem)
                return;

            // HasSpaceFor could called Defrag internally and read item has moved
            // need to ensure the nested page has a valid pointer

            nestedPagePtr = DirectAccessFromHeader(movedItem);
            nestedPage = new TreePage(nestedPagePtr, (ushort)GetDataSize(movedItem));
        }
    }
}

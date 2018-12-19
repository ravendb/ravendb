using System;
using System.Diagnostics;
using System.IO;
using Sparrow;
using Voron.Global;
using Voron.Impl;
using Voron.Impl.Paging;
using Sparrow.Binary;
using Voron.Exceptions;

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
        public bool IsMultiValueTree { get; set; }

        public void MultiAdd(Slice key, Slice value)
        {
            if (!value.HasValue)
                throw new ArgumentNullException(nameof(value));

            int maxNodeSize = Llt.DataPager.NodeMaxSize;
            if (value.Size > maxNodeSize)
                throw new ArgumentException("Cannot add a value to child tree that is over " + maxNodeSize + " bytes in size", nameof(value));
            if (value.Size == 0)
                throw new ArgumentException("Cannot add empty value to child tree");

            State.IsModified = true;
            State.Flags |= TreeFlags.MultiValueTrees;

            TreeNodeHeader* node;
            var page = FindPageFor(key, out node);
            if (page == null || page.LastMatch != 0)
            {
                MultiAddOnNewValue(key, value, maxNodeSize);
                return;
            }

            page = ModifyPage(page);

            var item = page.GetNode(page.LastSearchPosition);
            byte* _;

            // already was turned into a multi tree, not much to do here
            if (item->Flags == TreeNodeFlags.MultiValuePageRef)
            {
                var existingTree = OpenMultiValueTree(key, item);
                existingTree.DirectAdd(value, 0,out _).Dispose();
                return;
            }

            if (item->Flags == TreeNodeFlags.PageRef)
                throw new InvalidOperationException("Multi trees don't use overflows");

            var nestedPagePtr = DirectAccessFromHeader(item);

            var nestedPage = new TreePage(nestedPagePtr, (ushort)GetDataSize(item));

            var existingItem = nestedPage.Search(_llt, value);
            if (nestedPage.LastMatch != 0)
                existingItem = null;// not an actual match, just greater than

            if (existingItem != null)
            {
                // maybe same value added twice?
                Slice tmpKey;
                using (TreeNodeHeader.ToSlicePtr(_llt.Allocator, item, out tmpKey))
                {
                    if (SliceComparer.Equals(tmpKey, value))
                        return; // already there, turning into a no-op
                }

                nestedPage.RemoveNode(nestedPage.LastSearchPosition);
            }

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

                    var newPageSize = (ushort)Math.Min(Bits.NextPowerOf2(requiredSpace), maxNodeSize - Constants.Tree.NodeHeaderSize);

                    ExpandMultiTreeNestedPageSize(key, value, nestedPagePtr, newPageSize, nestedPage.PageSize);

                    return;
                }
            }

            EnsureNestedPagePointer(page, item, ref nestedPage, ref nestedPagePtr);

            // we now have to convert this into a tree instance, instead of just a nested page
            var tree = Create(_llt, _tx, key, TreeFlags.MultiValue);
            for (int i = 0; i < nestedPage.NumberOfEntries; i++)
            {
                Slice existingValue;
                using (nestedPage.GetNodeKey(_llt, i, out existingValue))
                {
                    tree.DirectAdd(existingValue, 0,out _).Dispose();
                }
            }
            tree.DirectAdd(value, 0,out _).Dispose();
            _tx.AddMultiValueTree(this, key, tree);
            // we need to record that we switched to tree mode here, so the next call wouldn't also try to create the tree again
            DirectAdd(key, sizeof(TreeRootHeader), TreeNodeFlags.MultiValuePageRef,out _).Dispose();
        }

        private void ExpandMultiTreeNestedPageSize(Slice key, Slice value, byte* nestedPagePtr, ushort newSize, int currentSize)
        {
            Debug.Assert(newSize > currentSize);

            TemporaryPage tmp;
            using (_llt.Environment.GetTemporaryPage(_llt, out tmp))
            {
                var tempPagePointer = tmp.TempPagePointer;
                Memory.Copy(tempPagePointer, nestedPagePtr, currentSize);
                Delete(key); // release our current page
                TreePage nestedPage = new TreePage(tempPagePointer, (ushort)currentSize);

                byte* ptr;
                using (DirectAdd(key, newSize,out ptr))
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

                        Slice nodeKey;
                        using (TreeNodeHeader.ToSlicePtr(allocator, nodeHeader, out nodeKey))
                            newNestedPage.AddDataNode(i, nodeKey, 0);
                    }

                    newNestedPage.Search(_llt, value);
                    newNestedPage.AddDataNode(newNestedPage.LastSearchPosition, value, 0);
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
                byte* _;
                tree.DirectAdd(value, 0,out _).Dispose();
                _tx.AddMultiValueTree(this, key, tree);

                DirectAdd(key, sizeof(TreeRootHeader), TreeNodeFlags.MultiValuePageRef,out _).Dispose();
                return;
            }

            var actualPageSize = (ushort)Math.Min(Bits.NextPowerOf2(requiredPageSize), maxNodeSize - Constants.Tree.NodeHeaderSize);

            byte* ptr;
            using (DirectAdd(key, actualPageSize, out ptr))
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
            State.IsModified = true;
            TreeNodeHeader* node;
            var page = FindPageFor(key, out node);
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
                if (tree.State.NumberOfEntries != 0)
                    return;
                _tx.TryRemoveMultiValueTree(this, key);
                if (_newPageAllocator != null)
                {
                    if (IsIndexTree == false)
                        ThrowAttemptToFreePageToNewPageAllocator(Name, tree.State.RootPageNumber);

                    _newPageAllocator.FreePage(tree.State.RootPageNumber);
                }
                else
                {
                    if (IsIndexTree)
                        ThrowAttemptToFreeIndexPageToFreeSpaceHandling(Name, tree.State.RootPageNumber);

                    _llt.FreePage(tree.State.RootPageNumber);
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
            TreeNodeHeader* node;
            var page = FindPageFor(key, out node);
            if (page == null || page.LastMatch != 0)
                return 0;

            Debug.Assert(node != null);

            Slice fetchedNodeKey;
            using (TreeNodeHeader.ToSlicePtr(_llt.Allocator, node, out fetchedNodeKey))
            {
                if (SliceComparer.Equals(fetchedNodeKey, key) == false)
                {
                    VoronUnrecoverableErrorException.Raise(_llt, "Was unable to retrieve the correct node. Data corruption possible");
                }
            }

            if (node->Flags == TreeNodeFlags.MultiValuePageRef)
            {
                var tree = OpenMultiValueTree(key, node);

                return tree.State.NumberOfEntries;
            }

            var nestedPage = new TreePage(DirectAccessFromHeader(node), (ushort)GetDataSize(node));

            return nestedPage.NumberOfEntries;
        }

        public IIterator MultiRead(Slice key)
        {
            TreeNodeHeader* node;
            var page = FindPageFor(key, out node);
            if (page == null || page.LastMatch != 0)
                return new EmptyIterator();

            Debug.Assert(node != null);

            Slice fetchedNodeKey;
            using (TreeNodeHeader.ToSlicePtr(_llt.Allocator, node, out fetchedNodeKey))
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
            Tree tree;
            if (_tx.TryGetMultiValueTree(this, key, out tree))
                return tree;

            var childTreeHeader =
                (TreeRootHeader*)((byte*)item + item->KeySize + Constants.Tree.NodeHeaderSize);

            Debug.Assert(childTreeHeader->RootPageNumber < _llt.State.NextPageNumber);
            Debug.Assert(childTreeHeader->Flags == TreeFlags.MultiValue);

            tree = Open(_llt, _tx, key, childTreeHeader);
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

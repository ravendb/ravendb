using Sparrow;
// -----------------------------------------------------------------------
//  <copyright file="Tree.MultiTree.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Diagnostics;
using System.IO;
using Voron.Impl;
using Voron.Impl.FileHeaders;
using Voron.Impl.Paging;
using Voron.Util;

namespace Voron.Trees
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
     * If the totale size of the values per key is less than NodeMaxSize, we store them as an embedded
     * page inside the owning tree. If then are more than the node max size, we create a separate tree
     * for them and then only store the tree root infromation.
     */
    public unsafe partial class Tree
    {
        public bool IsMultiValueTree { get; set; }

        public void MultiAdd(Slice key, Slice value, ushort? version = null)
        {
            if (value == null) throw new ArgumentNullException("value");
            int maxNodeSize = AbstractPager.NodeMaxSize;
            if (value.Size > maxNodeSize)
                throw new ArgumentException(
                    "Cannot add a value to child tree that is over " + maxNodeSize + " bytes in size", "value");
            if (value.Size == 0)
                throw new ArgumentException("Cannot add empty value to child tree");

            State.IsModified = true;
            State.Flags |= TreeFlags.MultiValueTrees;

            Lazy<Cursor> lazy;
            NodeHeader* node;
            var page = FindPageFor(key, out node, out lazy);
            if (page == null || page.LastMatch != 0)
            {
                MultiAddOnNewValue(_tx, key, value, version, maxNodeSize);
                return;
            }

            page = _tx.ModifyPage(page.PageNumber, this, page);
            var item = page.GetNode(page.LastSearchPosition);

            // already was turned into a multi tree, not much to do here
            if (item->Flags == NodeFlags.MultiValuePageRef)
            {
                var existingTree = OpenMultiValueTree(_tx, key, item);
                existingTree.DirectAdd(value, 0, version: version);
                return;
            }

            if (item->Flags == NodeFlags.PageRef)
                throw new InvalidOperationException($"Multi trees don't use overflows. Tree name: {Name}");

            var nestedPagePtr = NodeHeader.DirectAccess(_tx, item);

            var nestedPage = new Page(nestedPagePtr, "multi tree", (ushort)NodeHeader.GetDataSize(_tx, item));

            var existingItem = nestedPage.Search(value);
            if (nestedPage.LastMatch != 0)
                existingItem = null;// not an actual match, just greater than

            ushort previousNodeRevision = existingItem != null ?  existingItem->Version : (ushort)0;
            CheckConcurrency(key, value, version, previousNodeRevision, TreeActionType.Add);
            
            if (existingItem != null)
            {
                // maybe same value added twice?
                var tmpKey = page.GetNodeKey(item);
                if (tmpKey.Compare(value) == 0)
                    return; // already there, turning into a no-op
                nestedPage.RemoveNode(nestedPage.LastSearchPosition);
            }

            var valueToInsert = nestedPage.PrepareKeyToInsert(value, nestedPage.LastSearchPosition);

            if (nestedPage.HasSpaceFor(_tx, valueToInsert, 0))
            {
                // we are now working on top of the modified root page, we can just modify the memory directly
                nestedPage.AddDataNode(nestedPage.LastSearchPosition, valueToInsert, 0, previousNodeRevision);
                return;
            }

            if (page.HasSpaceFor(_tx, valueToInsert, 0))
            {
                // page has space for an additional node in nested page ...

                var requiredSpace = nestedPage.PageSize + // existing page
                                    nestedPage.GetRequiredSpace(value, 0); // new node

                if (requiredSpace + Constants.NodeHeaderSize <= maxNodeSize)
                {
                    // ... and it won't require to create an overflow, so we can just expand the current value, no need to create a nested tree yet

                    EnsureNestedPagePointer(page, item, ref nestedPage, ref nestedPagePtr);

                    var newPageSize = (ushort)Math.Min(Utils.NearestPowerOfTwo(requiredSpace), maxNodeSize - Constants.NodeHeaderSize);

                    ExpandMultiTreeNestedPageSize(_tx, key, valueToInsert, nestedPagePtr, newPageSize, nestedPage.PageSize);

                    return;
                }
            }

            EnsureNestedPagePointer(page, item, ref nestedPage, ref nestedPagePtr);

            // we now have to convert this into a tree instance, instead of just a nested page
            var tree = Create(_tx, KeysPrefixing, TreeFlags.MultiValue);
            for (int i = 0; i < nestedPage.NumberOfEntries; i++)
            {
                var existingValue = nestedPage.GetNodeKey(i);
                tree.DirectAdd(existingValue, 0);
            }
            tree.DirectAdd(value, 0, version: version);
            _tx.AddMultiValueTree(this, key, tree);
            // we need to record that we switched to tree mode here, so the next call wouldn't also try to create the tree again
            DirectAdd(key, sizeof (TreeRootHeader), NodeFlags.MultiValuePageRef);
        }

        private void ExpandMultiTreeNestedPageSize(Transaction tx, Slice key, MemorySlice value, byte* nestedPagePtr, ushort newSize, int currentSize)
        {
            Debug.Assert(newSize > currentSize);
            TemporaryPage tmp;
            using (tx.Environment.GetTemporaryPage(tx, out tmp))
            {
                var tempPagePointer = tmp.TempPagePointer;
                Memory.Copy(tempPagePointer, nestedPagePtr, currentSize);
                Delete(key); // release our current page
                Page nestedPage = new Page(tempPagePointer, "multi tree", (ushort)currentSize);

                var ptr = DirectAdd(key, newSize);

                var newNestedPage = new Page(ptr, "multi tree", newSize)
                {
                    Lower = (ushort)Constants.PageHeaderSize,
                    Upper = KeysPrefixing ? (ushort) (newSize - Constants.PrefixInfoSectionSize) : newSize,
                    Flags = KeysPrefixing ? PageFlags.Leaf | PageFlags.KeysPrefixed : PageFlags.Leaf,
                    PageNumber = -1L // mark as invalid page number
                };

                newNestedPage.ClearPrefixInfo();

                MemorySlice nodeKey = nestedPage.CreateNewEmptyKey();
                for (int i = 0; i < nestedPage.NumberOfEntries; i++)
                {
                    var nodeHeader = nestedPage.GetNode(i);
                    nestedPage.SetNodeKey(nodeHeader, ref nodeKey);
                    nodeKey = newNestedPage.PrepareKeyToInsert(nodeKey, i);
                    newNestedPage.AddDataNode(i, nodeKey, 0,
                        (ushort)(nodeHeader->Version - 1)); // we dec by one because AdddataNode will inc by one, and we don't want to change those values
                }

                newNestedPage.Search(value);
                newNestedPage.AddDataNode(newNestedPage.LastSearchPosition, newNestedPage.PrepareKeyToInsert(value, newNestedPage.LastSearchPosition), 0, 0);
            }
        }

        private void MultiAddOnNewValue(Transaction tx, Slice key, Slice value, ushort? version, int maxNodeSize)
        {
            MemorySlice valueToInsert;
            
            if(KeysPrefixing)
                valueToInsert = new PrefixedSlice(value); // first item is never prefixed
            else
                valueToInsert = value;

            var requiredPageSize = Constants.PageHeaderSize + // header of a nested page
                                   Constants.NodeOffsetSize +   // one node in a nested page
                                   SizeOf.LeafEntry(-1, value, 0); // node header and its value

            if (requiredPageSize + Constants.NodeHeaderSize > maxNodeSize)
            {
                // no choice, very big value, we might as well just put it in its own tree from the get go...
                // otherwise, we would have to put this in overflow page, and that won't save us any space anyway

                var tree = Create(tx, KeysPrefixing, TreeFlags.MultiValue);
                tree.DirectAdd(value, 0);
                tx.AddMultiValueTree(this, key, tree);

                DirectAdd(key, sizeof (TreeRootHeader), NodeFlags.MultiValuePageRef);
                return;
            }

            var actualPageSize = (ushort) Math.Min(Utils.NearestPowerOfTwo(requiredPageSize), maxNodeSize - Constants.NodeHeaderSize);

            var ptr = DirectAdd(key, actualPageSize);

            var nestedPage = new Page(ptr, "multi tree", actualPageSize)
            {
                PageNumber = -1L,// hint that this is an inner page
                Lower = (ushort) Constants.PageHeaderSize,
                Upper = KeysPrefixing ? (ushort)(actualPageSize - Constants.PrefixInfoSectionSize) : actualPageSize,
                Flags = KeysPrefixing ? PageFlags.Leaf | PageFlags.KeysPrefixed : PageFlags.Leaf,
            };

            nestedPage.ClearPrefixInfo();

            CheckConcurrency(key, value, version, 0, TreeActionType.Add);

            nestedPage.AddDataNode(0, valueToInsert, 0, 0);
        }

        public void MultiDelete(Slice key, Slice value, ushort? version = null)
        {
            State.IsModified = true;
            Lazy<Cursor> lazy;
            NodeHeader* node;
            var page = FindPageFor(key, out node, out lazy);
            if (page == null || page.LastMatch != 0)
            {
                return; //nothing to delete - key not found
            }

            page = _tx.ModifyPage(page.PageNumber, this, page);

            var item = page.GetNode(page.LastSearchPosition);

            if (item->Flags == NodeFlags.MultiValuePageRef) //multi-value tree exists
            {
                var tree = OpenMultiValueTree(_tx, key, item);

                tree.Delete(value, version);

                // previously, we would convert back to a simple model if we dropped to a single entry
                // however, it doesn't really make sense, once you got enough values to go to an actual nested 
                // tree, you are probably going to remain that way, or be removed completely.
                if (tree.State.EntriesCount != 0) 
                    return;
                _tx.TryRemoveMultiValueTree(this, key);
                _tx.FreePage(tree.State.RootPageNumber);

                Delete(key);
            }
            else // we use a nested page here
            {
                var nestedPage = new Page(NodeHeader.DirectAccess(_tx, item), "multi tree", (ushort)NodeHeader.GetDataSize(_tx, item));
                var nestedItem = nestedPage.Search(value);
                if (nestedPage.LastMatch != 0) // value not found
                    return;

                if (item->Flags == NodeFlags.PageRef)
                    throw new InvalidOperationException($"Multi trees don't use overflows. Tree name: {Name}" );

                var nestedPagePtr = NodeHeader.DirectAccess(_tx, item);

                nestedPage = new Page(nestedPagePtr, "multi tree", (ushort)NodeHeader.GetDataSize(_tx, item))
                {
                    LastSearchPosition = nestedPage.LastSearchPosition
                };

                CheckConcurrency(key, value, version, nestedItem->Version, TreeActionType.Delete);
                nestedPage.RemoveNode(nestedPage.LastSearchPosition);
                if (nestedPage.NumberOfEntries == 0)
                    Delete(key);
            }
        }

        //TODO: write a test for this
        public long MultiCount(Slice key)
        {
            Lazy<Cursor> lazy;
            NodeHeader* node;
            var page = FindPageFor(key, out node, out lazy);
            if (page == null || page.LastMatch != 0)
                return 0;

            Debug.Assert(node != null);

            var fetchedNodeKey = page.GetNodeKey(node);
            if (fetchedNodeKey.Compare(key) != 0)
            {
                throw new InvalidDataException("Was unable to retrieve the correct node. Data corruption possible");
            }

            if (node->Flags == NodeFlags.MultiValuePageRef)
            {
                var tree = OpenMultiValueTree(_tx, key, node);

                return tree.State.EntriesCount;
            }

            var nestedPage = new Page(NodeHeader.DirectAccess(_tx, node), "multi tree", (ushort)NodeHeader.GetDataSize(_tx, node));

            return nestedPage.NumberOfEntries;
        }

        public IIterator MultiRead(Slice key)
        {
            Lazy<Cursor> lazy;
            NodeHeader* node;
            var page = FindPageFor(key, out node, out lazy);
            if (page == null || page.LastMatch != 0)
                return new EmptyIterator();

            Debug.Assert(node != null);

            var fetchedNodeKey = page.GetNodeKey(node);
            if (fetchedNodeKey.Compare(key) != 0)
            {
                throw new InvalidDataException("Was unable to retrieve the correct node. Data corruption possible");
            }

            if (node->Flags == NodeFlags.MultiValuePageRef)
            {
                var tree = OpenMultiValueTree(_tx, key, node);

                return tree.Iterate();
            }

            var nestedPage = new Page(NodeHeader.DirectAccess(_tx, node), "multi tree", (ushort)NodeHeader.GetDataSize(_tx, node));
                
            return new PageIterator(nestedPage);
        }

        private Tree OpenMultiValueTree(Transaction tx, Slice key, NodeHeader* item)
        {
            Tree tree;
            if (tx.TryGetMultiValueTree(this, key, out tree))
                return tree;

            var childTreeHeader =
                (TreeRootHeader*)((byte*)item + item->KeySize + Constants.NodeHeaderSize);

            Debug.Assert(childTreeHeader->RootPageNumber < tx.State.NextPageNumber);
            Debug.Assert(childTreeHeader->Flags == TreeFlags.MultiValue);
            
            tree = Open(tx, childTreeHeader);

            tx.AddMultiValueTree(this, key, tree);
            return tree;
        }

        private bool TryOverwriteDataOrMultiValuePageRefNode(NodeHeader* updatedNode, MemorySlice key, int len,
                                                        NodeFlags requestedNodeType, ushort? version,
                                                        out byte* pos)
        {
            switch (requestedNodeType)
            {
                case NodeFlags.Data:
                case NodeFlags.MultiValuePageRef:
                    {
                        if (updatedNode->DataSize == len &&
                            (updatedNode->Flags == NodeFlags.Data || updatedNode->Flags == NodeFlags.MultiValuePageRef))
                        {
                            CheckConcurrency(key, version, updatedNode->Version, TreeActionType.Add);

                            if (updatedNode->Version == ushort.MaxValue)
                                updatedNode->Version = 0;
                            updatedNode->Version++;

                            updatedNode->Flags = requestedNodeType;

                            {
                                pos = (byte*)updatedNode + Constants.NodeHeaderSize + updatedNode->KeySize;
                                return true;
                            }
                        }
                        break;
                    }
                case NodeFlags.PageRef:
                    throw new InvalidOperationException("We never add PageRef explicitly");
                default:
                    throw new ArgumentOutOfRangeException();
            }
            pos = null;
            return false;
        }

        private void EnsureNestedPagePointer(Page page, NodeHeader* currentItem, ref Page nestedPage, ref byte* nestedPagePtr)
        {
            var movedItem = page.GetNode(page.LastSearchPosition);

            if (movedItem == currentItem)
                return;

            // HasSpaceFor could called Defrag internally and read item has moved
            // need to ensure the nested page has a valid pointer

            nestedPagePtr = NodeHeader.DirectAccess(_tx, movedItem);
            nestedPage = new Page(nestedPagePtr, "multi tree", (ushort)NodeHeader.GetDataSize(_tx, movedItem));
        }
    }
}

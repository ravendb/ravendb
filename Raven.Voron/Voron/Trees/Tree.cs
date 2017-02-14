using Sparrow;
using System;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.IO;
using System.Linq.Expressions;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Runtime.Remoting.Messaging;
using Voron.Debugging;
using Voron.Exceptions;
using Voron.Impl;
using Voron.Impl.FileHeaders;
using Voron.Impl.Paging;
using Voron.Trees.Fixed;
using Voron.Util;

namespace Voron.Trees
{
    public unsafe partial class Tree
    {
        private Dictionary<string, FixedSizeTree> _fixedSizeTrees;
        private readonly TreeMutableState _state = new TreeMutableState();

        private RecentlyFoundPages _recentlyFoundPages;
        public RecentlyFoundPages RecentlyFoundPages
        {
            get { return _recentlyFoundPages ?? (_recentlyFoundPages = new RecentlyFoundPages(_tx.Flags == TransactionFlags.Read ? 8 : 2)); }
        }

        public string Name { get; set; }

        public bool IsFreeSpaceTree { get; set; }

        public TreeMutableState State
        {
            get { return _state; }
        }

        private readonly Transaction _tx;
        public Transaction Tx
        {
            get { return _tx; }
        }

        private Tree(Transaction tx, long root)
        {
            _tx = tx;
            _state.RootPageNumber = root;
        }

        public Tree(Transaction tx, TreeMutableState state)
        {
            _tx = tx;
            _state = state;
        }

        public bool KeysPrefixing { get { return _state.KeysPrefixing; } }

        public static Tree Open(Transaction tx, TreeRootHeader* header)
        {
            return new Tree(tx, header->RootPageNumber)
            {
                _state =
                {
                    PageCount = header->PageCount,
                    BranchPages = header->BranchPages,
                    Depth = header->Depth,
                    OverflowPages = header->OverflowPages,
                    LeafPages = header->LeafPages,
                    EntriesCount = header->EntriesCount,
                    Flags = header->Flags,
                    InWriteTransaction = tx.Flags.HasFlag(TransactionFlags.ReadWrite),
                    KeysPrefixing = header->KeysPrefixing
                }
            };
        }

        public static Tree Create(Transaction tx, bool keysPrefixing, TreeFlags flags = TreeFlags.None)
        {
            var globalKeysPrefixingSetting = (CallContext.GetData("Voron/Trees/KeysPrefixing") as bool?);
            if (globalKeysPrefixingSetting != null)
                keysPrefixing = globalKeysPrefixingSetting.Value;

            var newRootPage = tx.AllocatePage(1, keysPrefixing ? PageFlags.Leaf | PageFlags.KeysPrefixed : PageFlags.Leaf);
            var tree = new Tree(tx, newRootPage.PageNumber)
            {
                _state =
                {
                    Depth = 1,
                    Flags = flags,
                    InWriteTransaction = true,
                    KeysPrefixing = keysPrefixing
                }
            };

            tree.State.RecordNewPage(newRootPage, 1);
            return tree;
        }

        public void Add(Slice key, Stream value, ushort? version = null)
        {
            if (value == null) throw new ArgumentNullException("value");
            if (value.Length > int.MaxValue)
                throw new ArgumentException("Cannot add a value that is over 2GB in size", "value");

            State.IsModified = true;
            var pos = DirectAdd(key, (int)value.Length, version: version);

            CopyStreamToPointer(_tx, value, pos);
        }

        public StructReadResult<T> ReadStruct<T>(Slice key, StructureSchema<T> schema)
        {
            var readResult = Read(key);

            if (readResult == null)
                return null;

            return new StructReadResult<T>(new StructureReader<T>(readResult.Reader.Base, schema), readResult.Version);
        }

        public void WriteStruct(Slice key, IStructure structure, ushort? version = null)
        {
            structure.AssertValidStructure();

            State.IsModified = true;
            var pos = DirectAdd(key, structure.GetSize(), version: version);

            structure.Write(pos);
        }

        /// <summary>
        /// This is using little endian
        /// </summary>
        public long Increment(Slice key, long delta, ushort? version = null)
        {
            State.IsModified = true;

            long currentValue = 0;

            var read = Read(key);
            if (read != null)
                currentValue = *(long*)read.Reader.Base;

            var value = currentValue + delta;
            var result = (long*)DirectAdd(key, sizeof(long), version: version);
            *result = value;

            return value;
        }

        public void Add(Slice key, byte[] value, ushort? version = null)
        {
            if (value == null) throw new ArgumentNullException("value");

            State.IsModified = true;
            var pos = DirectAdd(key, value.Length, version: version);

            fixed (byte* src = value)
            {
                Memory.Copy(pos, src, value.Length);
            }
        }

        public void Add(Slice key, Slice value, ushort? version = null)
        {
            if (value == null) throw new ArgumentNullException("value");

            State.IsModified = true;
            var pos = DirectAdd(key, value.Size, version: version);

            value.CopyTo(pos);
        }

        private static void CopyStreamToPointer(Transaction tx, Stream value, byte* pos)
        {
            TemporaryPage tmp;
            using (tx.Environment.GetTemporaryPage(tx, out tmp))
            {
                var tempPageBuffer = tmp.TempPageBuffer;
                var tempPagePointer = tmp.TempPagePointer;
                while (true)
                {
                    var read = value.Read(tempPageBuffer, 0, AbstractPager.PageSize);
                    if (read == 0)
                        break;

                    Memory.CopyInline(pos, tempPagePointer, read);
                    pos += read;

                    if (read != tempPageBuffer.Length)
                        break;
                }
            }
        }

        internal byte* DirectAdd(MemorySlice key, int len, NodeFlags nodeType = NodeFlags.Data, ushort? version = null)
        {
            Debug.Assert(nodeType == NodeFlags.Data || nodeType == NodeFlags.MultiValuePageRef);

            if (State.InWriteTransaction)
                State.IsModified = true;

            if (_tx.Flags == (TransactionFlags.ReadWrite) == false)
                throw new ArgumentException("Cannot add a value in a read only transaction");

            if (AbstractPager.IsKeySizeValid(key.Size, KeysPrefixing) == false)
                throw new ArgumentException("Key size is too big, must be at most " + AbstractPager.GetMaxKeySize(KeysPrefixing) + " bytes, but was " + key.Size, "key");

            Lazy<Cursor> lazy;
            NodeHeader* node;
            var foundPage = FindPageFor(key, out node, out lazy);

            var page = _tx.ModifyPage(foundPage.PageNumber, this, foundPage);

            ushort nodeVersion = 0;
            bool? shouldGoToOverflowPage = null;
            if (page.LastMatch == 0) // this is an update operation
            {
                node = page.GetNode(page.LastSearchPosition);

                Debug.Assert(page.GetNodeKey(node).Equals(key));

                shouldGoToOverflowPage = _tx.DataPager.ShouldGoToOverflowPage(len);

                byte* pos;
                if (shouldGoToOverflowPage == false)
                {
                    // optimization for Data and MultiValuePageRef - try to overwrite existing node space
                    if (TryOverwriteDataOrMultiValuePageRefNode(node, key, len, nodeType, version, out pos))
                        return pos;
                }
                else
                {
                    // optimization for PageRef - try to overwrite existing overflows
                    if (TryOverwriteOverflowPages(node, key, len, version, out pos))
                        return pos;
                }

                RemoveLeafNode(page, out nodeVersion);
            }
            else // new item should be recorded
            {
                State.EntriesCount++;
            }

            CheckConcurrency(key, version, nodeVersion, TreeActionType.Add);

            var lastSearchPosition = page.LastSearchPosition; // searching for overflow pages might change this
            byte* overFlowPos = null;
            var pageNumber = -1L;
            if (shouldGoToOverflowPage ?? _tx.DataPager.ShouldGoToOverflowPage(len))
            {
                pageNumber = WriteToOverflowPages(len, out overFlowPos);
                len = -1;
                nodeType = NodeFlags.PageRef;
            }

            var keyToInsert = page.PrepareKeyToInsert(key, lastSearchPosition);

            byte* dataPos;
            if (page.HasSpaceFor(_tx, keyToInsert, len) == false)
            {
                using ( var cursor = lazy.Value )
                {
                    cursor.Update(cursor.Pages.First, page);

                var pageSplitter = new PageSplitter(_tx, this, key, len, pageNumber, nodeType, nodeVersion, cursor);
                    dataPos = pageSplitter.Execute();
                }

                DebugValidateTree(State.RootPageNumber);
            }
            else
            {
                switch (nodeType)
                {
                    case NodeFlags.PageRef:
                        dataPos = page.AddPageRefNode(lastSearchPosition, keyToInsert, pageNumber);
                        break;
                    case NodeFlags.Data:
                        dataPos = page.AddDataNode(lastSearchPosition, keyToInsert, len, nodeVersion);
                        break;
                    case NodeFlags.MultiValuePageRef:
                        dataPos = page.AddMultiValueNode(lastSearchPosition, keyToInsert, len, nodeVersion);
                        break;
                    default:
                        throw new NotSupportedException("Unknown node type for direct add operation: " + nodeType);
                }
                page.DebugValidate(_tx, State.RootPageNumber);
            }
            if (overFlowPos != null)
                return overFlowPos;
            return dataPos;
        }

        private long WriteToOverflowPages(int overflowSize, out byte* dataPos)
        {
            var numberOfPages = _tx.DataPager.GetNumberOfOverflowPages(overflowSize);
            var overflowPageStart = _tx.AllocatePage(numberOfPages, PageFlags.Overflow);
            overflowPageStart.OverflowSize = overflowSize;
            dataPos = overflowPageStart.Base + Constants.PageHeaderSize;

            State.RecordNewPage(overflowPageStart, numberOfPages);

            return overflowPageStart.PageNumber;
        }

        private void RemoveLeafNode(Page page, out ushort nodeVersion)
        {
            var node = page.GetNode(page.LastSearchPosition);
            nodeVersion = node->Version;
            if (node->Flags == (NodeFlags.PageRef)) // this is an overflow pointer
            {
                var overflowPage = _tx.GetReadOnlyPage(node->PageNumber);
                FreePage(overflowPage);
                }

            page.RemoveNode(page.LastSearchPosition);
        }

        [Conditional("VALIDATE")]
        public void DebugValidateTree(long rootPageNumber)
        {
            var pages = new HashSet<long>();
            var stack = new Stack<Page>();
            var root = _tx.GetReadOnlyPage(rootPageNumber);
            stack.Push(root);
            pages.Add(rootPageNumber);
            while (stack.Count > 0)
            {
                var p = stack.Pop();
                if (p.NumberOfEntries == 0 && p != root)
                {
                    DebugStuff.RenderAndShow(_tx, rootPageNumber);
                    throw new InvalidOperationException("The page " + p.PageNumber + " is empty");

                }
                p.DebugValidate(_tx, rootPageNumber);
                if (p.IsBranch == false)
                    continue;

                if (p.NumberOfEntries < 2)
                {
                    throw new InvalidOperationException("The branch page " + p.PageNumber + " has " + p.NumberOfEntries + " entry");
                }

                for (int i = 0; i < p.NumberOfEntries; i++)
                {
                    var page = p.GetNode(i)->PageNumber;
                    if (pages.Add(page) == false)
                    {
                        DebugStuff.RenderAndShow(_tx, rootPageNumber);
                        throw new InvalidOperationException("The page " + page + " already appeared in the tree!");
                    }
                    stack.Push(_tx.GetReadOnlyPage(page));
                }
            }
        }

        internal Page FindPageFor(MemorySlice key, out NodeHeader* node, out Lazy<Cursor> cursor)
        {
            Page p;

            if (TryUseRecentTransactionPage(key, out cursor, out p, out node))
            {
                return p;
            }

            return SearchForPage(key, out cursor, out node);
        }

        private Page SearchForPage(MemorySlice key, out Lazy<Cursor> cursor, out NodeHeader* node)
        {
            var p = _tx.GetReadOnlyPage(State.RootPageNumber);
            var c = new Cursor();
            c.Push(p);

            bool rightmostPage = true;
            bool leftmostPage = true;

            while ((p.Flags & PageFlags.Branch) == PageFlags.Branch)
            {
                int nodePos;
                if (key.Options == SliceOptions.BeforeAllKeys)
                {
                    p.LastSearchPosition = nodePos = 0;
                    rightmostPage = false;
                }
                else if (key.Options == SliceOptions.AfterAllKeys)
                {
                    p.LastSearchPosition = nodePos = (ushort)(p.NumberOfEntries - 1);
                    leftmostPage = false;
                }
                else
                {
                    if (p.Search(key) != null)
                    {
                        nodePos = p.LastSearchPosition;
                        if (p.LastMatch != 0)
                        {
                            nodePos--;
                            p.LastSearchPosition--;
                        }

                        if (nodePos != 0)
                            leftmostPage = false;

                        rightmostPage = false;
                    }
                    else
                    {
                        nodePos = (ushort)(p.LastSearchPosition - 1);

                        leftmostPage = false;
                    }
                }

                var pageNode = p.GetNode(nodePos);
                p = _tx.GetReadOnlyPage(pageNode->PageNumber);
                Debug.Assert(pageNode->PageNumber == p.PageNumber,
                    string.Format("Requested Page: #{0}. Got Page: #{1}", pageNode->PageNumber, p.PageNumber));

                c.Push(p);
            }

            if (p.IsLeaf == false)
                throw new DataException("Index points to a non leaf page");

            node = p.Search(key); // will set the LastSearchPosition
            if(p.NumberOfEntries > 0)
                AddToRecentlyFoundPages(c, p, leftmostPage, rightmostPage);

            cursor = new Lazy<Cursor>(() => c);
            return p;
        }

        private void AddToRecentlyFoundPages(Cursor c, Page p, bool? leftmostPage, bool? rightmostPage)
        {
            MemorySlice firstKey;
            if (leftmostPage == true)
            {
                if (p.KeysPrefixed)
                    firstKey = PrefixedSlice.BeforeAllKeys;
                else
                    firstKey = Slice.BeforeAllKeys;
            }
            else
            {
                firstKey = p.GetNodeKey(0);
            }

            MemorySlice lastKey;
            if (rightmostPage == true)
            {
                if (p.KeysPrefixed)
                    lastKey = PrefixedSlice.AfterAllKeys;
                else
                    lastKey = Slice.AfterAllKeys;
            }
            else
            {
                lastKey = p.GetNodeKey(p.NumberOfEntries - 1);
            }

            var cursorPath = new long[c.Pages.Count];

            var cur = c.Pages.First;
            int pos = cursorPath.Length - 1;
            while (cur != null)
            {
                cursorPath[pos--] = cur.Value.PageNumber;
                cur = cur.Next;
            }

            var foundPage = new RecentlyFoundPages.FoundPage(p.PageNumber, p, firstKey, lastKey, cursorPath);

            RecentlyFoundPages.Add(foundPage);
        }

        private bool TryUseRecentTransactionPage(MemorySlice key, out Lazy<Cursor> cursor, out Page page, out NodeHeader* node)
        {
            node = null;
            page = null;
            cursor = null;

            var recentPages = RecentlyFoundPages;
            if (recentPages == null)
                return false;

            var foundPage = recentPages.Find(key);
            if (foundPage == null)
                return false;

            var lastFoundPageNumber = foundPage.Number;

            if (foundPage.Page != null)
            {
                // we can't share the same instance, Page instance may be modified by
                // concurrently run iterators
                page = new Page(foundPage.Page.Base, foundPage.Page.Source, foundPage.Page.PageSize);
            }
            else
            {
                page = _tx.GetReadOnlyPage(lastFoundPageNumber);
            }

            if (page.IsLeaf == false)
                throw new DataException("Index points to a non leaf page");

            node = page.Search(key); // will set the LastSearchPosition

            var cursorPath = foundPage.CursorPath;
            var pageCopy = page;
            cursor = new Lazy<Cursor>(() =>
            {
                var c = new Cursor();
                foreach (var p in cursorPath)
                {
                    if (p == lastFoundPageNumber)
                    {
                        c.Push(pageCopy);
                    }
                    else
                    {
                        var cursorPage = _tx.GetReadOnlyPage(p);
                        if (key.Options == SliceOptions.BeforeAllKeys)
                        {
                            cursorPage.LastSearchPosition = 0;
                        }
                        else if (key.Options == SliceOptions.AfterAllKeys)
                        {
                            cursorPage.LastSearchPosition = (ushort)(cursorPage.NumberOfEntries - 1);
                        }
                        else if (cursorPage.Search(key) != null)
                        {
                            if (cursorPage.LastMatch != 0)
                            {
                                cursorPage.LastSearchPosition--;
                            }
                        }

                        c.Push(cursorPage);
                    }
                }

                return c;
            });

            return true;
        }

        internal Page NewPage(PageFlags flags, int num)
        {
            Page page;
            using (IsFreeSpaceTree ? _tx.Environment.FreeSpaceHandling.Disable() : null)
            {
                page = _tx.AllocatePage(num, flags);
            }

            State.RecordNewPage(page, num);

            return page;
        }

        internal void FreePage(Page p)
        {
            if (p.IsOverflow)
            {
                var numberOfPages = _tx.DataPager.GetNumberOfOverflowPages(p.OverflowSize);
                for (int i = 0; i < numberOfPages; i++)
                {
                    if (IsFreeSpaceTree)
                        _tx.FreePageOnCommit(p.PageNumber + i);
                    else
                        _tx.FreePage(p.PageNumber + i);
                }

                State.RecordFreedPage(p, numberOfPages);
            }
            else
            {
                if (IsFreeSpaceTree)
                    _tx.FreePageOnCommit(p.PageNumber);
                else
                    _tx.FreePage(p.PageNumber);
                State.RecordFreedPage(p, 1);
            }
        }

        public void Delete(Slice key, ushort? version = null)
        {
            if (_tx.Flags == (TransactionFlags.ReadWrite) == false)
                throw new ArgumentException("Cannot delete a value in a read only transaction");

            State.IsModified = true;
            Lazy<Cursor> lazy;
            NodeHeader* node;
            var page = FindPageFor(key, out node, out lazy);

            if (page.LastMatch != 0)
                return; // not an exact match, can't delete

            page = _tx.ModifyPage(page.PageNumber, this, page);

            State.EntriesCount--;
            ushort nodeVersion;
            RemoveLeafNode(page, out nodeVersion);

            CheckConcurrency(key, version, nodeVersion, TreeActionType.Delete);

            using ( var cursor = lazy.Value )
            {
                var treeRebalancer = new TreeRebalancer(_tx, this, cursor);
            var changedPage = page;
            while (changedPage != null)
            {
                changedPage = treeRebalancer.Execute(changedPage);
            }

            }

            page.DebugValidate(_tx, State.RootPageNumber);
        }

        public TreeIterator Iterate(bool prefetch = true)
        {
            return new TreeIterator(this, _tx, prefetch);
        }

        public ReadResult Read(Slice key)
        {
            Lazy<Cursor> lazy;
            NodeHeader* node;
            var p = FindPageFor(key, out node, out lazy);

            if (p.LastMatch != 0)
                return null;

            return new ReadResult(NodeHeader.Reader(_tx, node), node->Version);
        }

        public int GetDataSize(Slice key)
        {
            Lazy<Cursor> lazy;
            NodeHeader* node;
            var p = FindPageFor(key, out node, out lazy);
            if (p == null || p.LastMatch != 0)
                return -1;

            if (node == null || p.GetNodeKey(node).Compare(key) != 0)
                return -1;

            return NodeHeader.GetDataSize(_tx, node);
        }

        public ushort ReadVersion(Slice key)
        {
            Lazy<Cursor> lazy;
            NodeHeader* node;
            var p = FindPageFor(key, out node, out lazy);
            if (p == null || p.LastMatch != 0)
                return 0;

            if (node == null || p.GetNodeKey(node).Compare(key) != 0)
                return 0;

            return node->Version;
        }

        internal byte* DirectRead(Slice key)
        {
            Lazy<Cursor> lazy;
            NodeHeader* node;
            var p = FindPageFor(key, out node, out lazy);
            if (p == null || p.LastMatch != 0)
                return null;

            Debug.Assert(node != null);

            if (node->Flags == (NodeFlags.PageRef))
            {
                var overFlowPage = _tx.GetReadOnlyPage(node->PageNumber);
                return overFlowPage.Base + Constants.PageHeaderSize;
            }

            return (byte*)node + node->KeySize + Constants.NodeHeaderSize;
        }

        public List<long> AllPages()
        {
            var results = new List<long>();
            var stack = new Stack<Page>();
            var root = _tx.GetReadOnlyPage(State.RootPageNumber);
            stack.Push(root);

            while (stack.Count > 0)
            {
                var p = stack.Pop();
                results.Add(p.PageNumber);

                var key = p.CreateNewEmptyKey();

                for (int i = 0; i < p.NumberOfEntries; i++)
                {
                    var node = p.GetNode(i);
                    var pageNumber = node->PageNumber;
                    if (p.IsBranch)
                    {
                        stack.Push(_tx.GetReadOnlyPage(pageNumber));
                    }
                    else if (node->Flags == NodeFlags.PageRef)
                    {
                        // This is an overflow page
                        var overflowPage = _tx.GetReadOnlyPage(pageNumber);
                        var numberOfPages = _tx.DataPager.GetNumberOfOverflowPages(overflowPage.OverflowSize);
                        for (long j = 0; j < numberOfPages; ++j)
                            results.Add(overflowPage.PageNumber + j);
                    }
                    else if (node->Flags == NodeFlags.MultiValuePageRef)
                    {
                        // this is a multi value
                        p.SetNodeKey(node, ref key);
                        var tree = OpenMultiValueTree(_tx, (Slice)key, node);
                        results.AddRange(tree.AllPages());
                    }
                    else
                    {
                        if (State.Flags.HasFlag(TreeFlags.FixedSizeTrees))
                        {
                            var valueReader = NodeHeader.Reader(_tx, node);
                            byte valueSize = *valueReader.Base;

                            var fixedSizeTreeName = p.GetNodeKey(i);

                            var fixedSizeTree = new FixedSizeTree(_tx, this, (Slice) fixedSizeTreeName, valueSize);

                            var pages = fixedSizeTree.AllPages();
                            results.AddRange(pages);
                }
            }
                }
            }
            return results;
        }

        public override string ToString()
        {
            return Name + " " + State.EntriesCount;
        }


        private void CheckConcurrency(MemorySlice key, ushort? expectedVersion, ushort nodeVersion, TreeActionType actionType)
        {
            if (expectedVersion.HasValue && nodeVersion != expectedVersion.Value)
                throw new ConcurrencyException(string.Format("Cannot {0} '{1}' to '{4}' tree. Version mismatch. Expected: {2}. Actual: {3}.", actionType.ToString().ToLowerInvariant(), key, expectedVersion.Value, nodeVersion, Name));
        }


        private void CheckConcurrency(Slice key, Slice value, ushort? expectedVersion, ushort nodeVersion, TreeActionType actionType)
        {
            if (expectedVersion.HasValue && nodeVersion != expectedVersion.Value)
                throw new ConcurrencyException(string.Format("Cannot {0} value '{5}' to key '{1}' to '{4}' tree. Version mismatch. Expected: {2}. Actual: {3}.", actionType.ToString().ToLowerInvariant(), key, expectedVersion.Value, nodeVersion, Name, value));
        }

        private enum TreeActionType
        {
            Add,
            Delete
        }

        internal Tree Clone(Transaction tx)
        {
            return new Tree(tx, _state.Clone()) { Name = Name };
        }

        private bool TryOverwriteOverflowPages(NodeHeader* updatedNode,
                                                      MemorySlice key, int len, ushort? version, out byte* pos)
        {
            if (updatedNode->Flags == NodeFlags.PageRef)
            {
                var readOnlyOverflowPage = _tx.GetReadOnlyPage(updatedNode->PageNumber);

                if (len <= readOnlyOverflowPage.OverflowSize)
                {
                    CheckConcurrency(key, version, updatedNode->Version, TreeActionType.Add);

                    if (updatedNode->Version == ushort.MaxValue)
                        updatedNode->Version = 0;
                    updatedNode->Version++;

                    var availableOverflows = _tx.DataPager.GetNumberOfOverflowPages(readOnlyOverflowPage.OverflowSize);

                    var requestedOverflows = _tx.DataPager.GetNumberOfOverflowPages(len);

                    var overflowsToFree = availableOverflows - requestedOverflows;

                    for (int i = 0; i < overflowsToFree; i++)
                    {
                        _tx.FreePage(readOnlyOverflowPage.PageNumber + requestedOverflows + i);
                    }

                    State.RecordFreedPage(readOnlyOverflowPage, overflowsToFree);

                    var writtableOverflowPage = _tx.AllocatePage(requestedOverflows, PageFlags.Overflow, updatedNode->PageNumber);

                    writtableOverflowPage.OverflowSize = len;
                    pos = writtableOverflowPage.Base + Constants.PageHeaderSize;
                    return true;
                }
            }
            pos = null;
            return false;
        }

        public Slice LastKeyOrDefault()
        {
            using (var it = Iterate(false))
            {
                if (it.Seek(Slice.AfterAllKeys) == false)
                    return null;
                return it.CurrentKey.Clone();
            }
        }

        public Slice FirstKeyOrDefault()
        {
            using (var it = Iterate(false))
            {
                if (it.Seek(Slice.BeforeAllKeys) == false)
                    return null;
                return it.CurrentKey.Clone();
            }
        }

        public void ClearRecentFoundPages()
        {
            if (_recentlyFoundPages != null)
                _recentlyFoundPages.Clear();
        }

        public FixedSizeTree FixedTreeFor(string key, byte valSize = 0)
        {
            if (_fixedSizeTrees == null)
                _fixedSizeTrees= new Dictionary<string, FixedSizeTree>();

            FixedSizeTree fixedTree;
            if (_fixedSizeTrees.TryGetValue(key, out fixedTree) == false)
            {
                _fixedSizeTrees[key] = fixedTree = new FixedSizeTree(_tx, this, key, valSize);
    }

            State.Flags |= TreeFlags.FixedSizeTrees;

            return fixedTree;
}

        public long DeleteFixedTreeFor(string key, byte valSize = 0)
        {
            var fixedSizeTree = FixedTreeFor(key, valSize);
            var numberOfEntries = fixedSizeTree.NumberOfEntries;

            foreach (var page in fixedSizeTree.AllPages())
            {
                _tx.FreePage(page);
            }
            _fixedSizeTrees.Remove(key);
            Delete(key);
            
            return numberOfEntries;
        }

        [Conditional("DEBUG")]
        public void DebugRenderAndShow()
        {
            DebugStuff.RenderAndShow(this);
        }
    }
}

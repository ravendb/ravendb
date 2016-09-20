using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Runtime.CompilerServices;
using Sparrow;
using Voron.Data.Fixed;
using Voron.Debugging;
using Voron.Exceptions;
using Voron.Global;
using Voron.Impl;
using Voron.Impl.Paging;

namespace Voron.Data.BTrees
{
    public unsafe partial class Tree
    {
        private Dictionary<Slice, FixedSizeTree> _fixedSizeTrees;
        private readonly TreeMutableState _state;

        public event Action<long> PageModified;
        public event Action<long> PageFreed;

        private readonly RecentlyFoundTreePages _recentlyFoundPages;

        public Slice Name { get; set; }

        public TreeMutableState State
        {
            get { return _state; }
        }

        private readonly LowLevelTransaction _llt;
        private readonly Transaction _tx;

        public LowLevelTransaction Llt
        {
            get { return _llt; }
        }

        private Tree(LowLevelTransaction llt, Transaction tx, long root)
        {
            _llt = llt;
            _tx = tx;
            _recentlyFoundPages = new RecentlyFoundTreePages(llt.Flags == TransactionFlags.Read ? 8 : 2);
            _state = new TreeMutableState(llt)
            {
                RootPageNumber = root
            };
        }

        public Tree(LowLevelTransaction llt, Transaction tx, TreeMutableState state)
        {
            _llt = llt;
            _tx = tx;
            _recentlyFoundPages = new RecentlyFoundTreePages(llt.Flags == TransactionFlags.Read ? 8 : 2);
            _state = new TreeMutableState(llt);
            _state = state;
        }

        public static Tree Open(LowLevelTransaction llt, Transaction tx, TreeRootHeader* header, RootObjectType type = RootObjectType.VariableSizeTree)
        {
            return new Tree(llt, tx, header->RootPageNumber)
            {
                _state =
                {
                    RootObjectType = type,
                    PageCount = header->PageCount,
                    BranchPages = header->BranchPages,
                    Depth = header->Depth,
                    OverflowPages = header->OverflowPages,
                    LeafPages = header->LeafPages,
                    NumberOfEntries = header->NumberOfEntries,
                    Flags = header->Flags,
                    InWriteTransaction = (llt.Flags == TransactionFlags.ReadWrite),
                }
            };
        }

        public static Tree Create(LowLevelTransaction llt, Transaction tx, TreeFlags flags = TreeFlags.None, RootObjectType type = RootObjectType.VariableSizeTree)
        {
            if (type != RootObjectType.VariableSizeTree && type != RootObjectType.Table )
                throw new ArgumentException($"Only valid types are {nameof(RootObjectType.VariableSizeTree)} or {nameof(RootObjectType.Table)}.", nameof(type));

            var newRootPage = AllocateNewPage(llt, TreePageFlags.Leaf, 1);
            var tree = new Tree(llt, tx, newRootPage.PageNumber)
            {
                _state =
                {
                    RootObjectType = type,
                    Depth = 1,
                    Flags = flags,
                    InWriteTransaction = true,
                }
            };

            tree.State.RecordNewPage(newRootPage, 1);
            return tree;
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

        /// <summary>
        /// This is using little endian
        /// </summary>
        public bool AddMax(Slice key, long value, ushort? version = null)
        {
            var read = Read(key);
            if (read != null)
            {
                var currentValue = *(long*)read.Reader.Base;
                if (currentValue >= value)
                    return false;
            }

            State.IsModified = true;

            var result = (long*)DirectAdd(key, sizeof(long), version: version);
            *result = value;

            return true;
        }

        public void Add(Slice key, Stream value, ushort? version = null)
        {
            ValidateValueLength(value);

            State.IsModified = true;

            var length = (int)value.Length;

            var pos = DirectAdd(key, length, version: version);

            CopyStreamToPointer(_llt, value, pos);
        }

        private static void ValidateValueLength(Stream value)
        {
            if (value == null) throw new ArgumentNullException(nameof(value));

            if (value.Length > int.MaxValue)
                throw new ArgumentException("Cannot add a value that is over 2GB in size", nameof(value));
        }

        public void Add(Slice key, byte[] value, ushort? version = null)
        {
            if (value == null) throw new ArgumentNullException(nameof(value));

            State.IsModified = true;
            var pos = DirectAdd(key, value.Length, version: version);

            fixed (byte* src = value)
            {
                Memory.Copy(pos, src, value.Length);
            }
        }

        public void Add(Slice key, Slice value, ushort? version = null)
        {
            if (!value.HasValue)
                throw new ArgumentNullException(nameof(value));

            State.IsModified = true;
            var pos = DirectAdd(key, value.Size, version: version);

            value.CopyTo(pos);
        }

        private static void CopyStreamToPointer(LowLevelTransaction tx, Stream value, byte* pos)
        {
            TemporaryPage tmp;
            using (tx.Environment.GetTemporaryPage(tx, out tmp))
            {
                var tempPageBuffer = tmp.TempPageBuffer;
                var tempPagePointer = tmp.TempPagePointer;
                int copied = 0;

                while (true)
                {
                    var read = value.Read(tempPageBuffer, 0, tempPageBuffer.Length);
                    if (read == 0)
                        break;

                    Memory.CopyInline(pos, tempPagePointer, read);
                    pos += read;
                    copied += read;

                    if (read != tempPageBuffer.Length)
                        break;
                }
            }
        }

        public byte* DirectAdd(Slice key, int len, TreeNodeFlags nodeType = TreeNodeFlags.Data, ushort? version = null)
        {
            Debug.Assert(nodeType == TreeNodeFlags.Data || nodeType == TreeNodeFlags.MultiValuePageRef);

            if (State.InWriteTransaction)
                State.IsModified = true;

            if (_llt.Flags == (TransactionFlags.ReadWrite) == false)
                throw new ArgumentException("Cannot add a value in a read only transaction");

            if (AbstractPager.IsKeySizeValid(key.Size) == false)
                throw new ArgumentException($"Key size is too big, must be at most {AbstractPager.MaxKeySize} bytes, but was {(key.Size + AbstractPager.RequiredSpaceForNewNode)}", nameof(key));

            Func<TreeCursor> cursorConstructor;
            TreeNodeHeader* node;
            var foundPage = FindPageFor(key, out node, out cursorConstructor);

            var page = ModifyPage(foundPage);

            ushort nodeVersion = 0;
            bool? shouldGoToOverflowPage = null;
            if (page.LastMatch == 0) // this is an update operation
            {
                node = page.GetNode(page.LastSearchPosition);                

                Debug.Assert(SliceComparer.EqualsInline(TreeNodeHeader.ToSlicePtr(_llt.Allocator, node), key));

                shouldGoToOverflowPage = ShouldGoToOverflowPage(len);

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
                State.NumberOfEntries++;
            }

            CheckConcurrency(key, version, nodeVersion, TreeActionType.Add);

            var lastSearchPosition = page.LastSearchPosition; // searching for overflow pages might change this
            byte* overFlowPos = null;
            var pageNumber = -1L;
            if (shouldGoToOverflowPage ?? ShouldGoToOverflowPage(len))
            {
                pageNumber = WriteToOverflowPages(len, out overFlowPos);
                len = -1;
                nodeType = TreeNodeFlags.PageRef;
            }

            byte* dataPos;
            if (page.HasSpaceFor(_llt, key, len) == false)
            {
                using (var cursor = cursorConstructor())
                {
                    cursor.Update(cursor.Pages.First, page);

                    var pageSplitter = new TreePageSplitter(_llt, this, key, len, pageNumber, nodeType, nodeVersion, cursor);
                    dataPos = pageSplitter.Execute();
                }

                DebugValidateTree(State.RootPageNumber);
            }
            else
            {
                switch (nodeType)
                {
                    case TreeNodeFlags.PageRef:
                        dataPos = page.AddPageRefNode(lastSearchPosition, key, pageNumber);
                        break;
                    case TreeNodeFlags.Data:
                        dataPos = page.AddDataNode(lastSearchPosition, key, len, nodeVersion);
                        break;
                    case TreeNodeFlags.MultiValuePageRef:
                        dataPos = page.AddMultiValueNode(lastSearchPosition, key, len, nodeVersion);
                        break;
                    default:
                        throw new NotSupportedException("Unknown node type for direct add operation: " + nodeType);
                }
                page.DebugValidate(_llt, State.RootPageNumber);
            }
            if (overFlowPos != null)
                return overFlowPos;
            return dataPos;
        }

        public TreePage ModifyPage(TreePage page)
        {
            if (page.Dirty)
                return page;

            var newPage = ModifyPage(page.PageNumber);
            newPage.LastSearchPosition = page.LastSearchPosition;
            newPage.LastMatch = page.LastMatch;

            return newPage;
        }

        public TreePage ModifyPage(long pageNumber)
        {
            var newPage = _llt.ModifyPage(pageNumber).ToTreePage();
            newPage.Dirty = true;
            _recentlyFoundPages.Reset(pageNumber);

            PageModified?.Invoke(pageNumber);

            return newPage;
        }

        public bool ShouldGoToOverflowPage(int len)
        {
            return len + Constants.NodeHeaderSize > _llt.DataPager.NodeMaxSize;
        }

        private long WriteToOverflowPages(int overflowSize, out byte* dataPos)
        {
            var numberOfPages = _llt.DataPager.GetNumberOfOverflowPages(overflowSize);
            var overflowPageStart = AllocateNewPage(_llt, TreePageFlags.Value, numberOfPages);
            overflowPageStart.Flags = PageFlags.Overflow | PageFlags.VariableSizeTreePage;
            overflowPageStart.OverflowSize = overflowSize;
            dataPos = overflowPageStart.Base + Constants.TreePageHeaderSize;

            State.RecordNewPage(overflowPageStart, numberOfPages);

            PageModified?.Invoke(overflowPageStart.PageNumber);

            return overflowPageStart.PageNumber;
        }

        private void RemoveLeafNode(TreePage page, out ushort nodeVersion)
        {
            var node = page.GetNode(page.LastSearchPosition);
            nodeVersion = node->Version;
            if (node->Flags == (TreeNodeFlags.PageRef)) // this is an overflow pointer
            {
                var overflowPage = _llt.GetReadOnlyTreePage(node->PageNumber);
                FreePage(overflowPage);
            }

            page.RemoveNode(page.LastSearchPosition);
        }

        [Conditional("VALIDATE")]
        public void DebugValidateTree(long rootPageNumber)
        {
            var pages = new HashSet<long>();
            var stack = new Stack<TreePage>();
            var root = _llt.GetReadOnlyTreePage(rootPageNumber);
            stack.Push(root);
            pages.Add(rootPageNumber);
            while (stack.Count > 0)
            {
                var p = stack.Pop();
                if (p.NumberOfEntries == 0 && p != root)
                {
                    DebugStuff.RenderAndShowTree(_llt, rootPageNumber);
                    throw new InvalidOperationException("The page " + p.PageNumber + " is empty");

                }
                p.DebugValidate(_llt, rootPageNumber);
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
                        DebugStuff.RenderAndShowTree(_llt, rootPageNumber);
                        throw new InvalidOperationException("The page " + page + " already appeared in the tree!");
                    }
                    stack.Push(_llt.GetReadOnlyTreePage(page));
                }
            }
        }

        internal TreePage FindPageFor(Slice key, out TreeNodeHeader* node)
        {
            TreePage p;

            if (TryUseRecentTransactionPage(key, out p, out node))
            {
                return p;
            }

            return SearchForPage(key, out node);
        }

        internal TreePage FindPageFor(Slice key, out TreeNodeHeader* node, out Func<TreeCursor> cursor)
        {
            TreePage p;

            if (TryUseRecentTransactionPage(key, out cursor, out p, out node))
            {
                return p;
            }

            return SearchForPage(key, out cursor, out node);
        }

        private TreePage SearchForPage(Slice key, out TreeNodeHeader* node) 
        {
            var p = _llt.GetReadOnlyTreePage(State.RootPageNumber);

            var cursorPath = new List<long>();
            cursorPath.Add(p.PageNumber);

            bool rightmostPage = true;
            bool leftmostPage = true;

            while ((p.TreeFlags & TreePageFlags.Branch) == TreePageFlags.Branch)
            {
                int nodePos;

                if ( key.Options == SliceOptions.Key)
                {
                    if (p.Search(_llt, key) != null)
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
                else if (key.Options == SliceOptions.BeforeAllKeys)
                {
                    p.LastSearchPosition = nodePos = 0;
                    rightmostPage = false;
                }
                else // if (key.Options == SliceOptions.AfterAllKeys)
                {
                    p.LastSearchPosition = nodePos = (ushort)(p.NumberOfEntries - 1);
                    leftmostPage = false;
                }

                var pageNode = p.GetNode(nodePos);
                p = _llt.GetReadOnlyTreePage(pageNode->PageNumber);
                Debug.Assert(pageNode->PageNumber == p.PageNumber,
                    string.Format("Requested Page: #{0}. Got Page: #{1}", pageNode->PageNumber, p.PageNumber));

                cursorPath.Add(p.PageNumber);
            }

            if (p.IsLeaf == false)
                throw new InvalidDataException("Index points to a non leaf page " + p.PageNumber);

            node = p.Search(_llt, key); // will set the LastSearchPosition

            AddToRecentlyFoundPages(cursorPath, p, leftmostPage, rightmostPage);

            return p;
        }

        private TreePage SearchForPage(Slice key, out Func<TreeCursor> cursorConstructor, out TreeNodeHeader* node)
        {
            var p = _llt.GetReadOnlyTreePage(State.RootPageNumber);

            var cursor = new TreeCursor();
            cursor.Push(p);

            bool rightmostPage = true;
            bool leftmostPage = true;

            while ((p.TreeFlags & TreePageFlags.Branch) == TreePageFlags.Branch)
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
                    if (p.Search(_llt, key) != null)
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
                p = _llt.GetReadOnlyTreePage(pageNode->PageNumber);
                Debug.Assert(pageNode->PageNumber == p.PageNumber,
                    string.Format("Requested Page: #{0}. Got Page: #{1}", pageNode->PageNumber, p.PageNumber));

                cursor.Push(p);
            }

            cursorConstructor = () => cursor;

            if (p.IsLeaf == false)
                throw new InvalidDataException("Index points to a non leaf page");

            node = p.Search(_llt, key); // will set the LastSearchPosition

            AddToRecentlyFoundPages(cursor, p, leftmostPage, rightmostPage);

            return p;
        }

        private void AddToRecentlyFoundPages(List<long> c, TreePage p, bool leftmostPage, bool rightmostPage)
        {            
            Slice firstKey;
            if (leftmostPage == true)
            {
                firstKey = Slices.BeforeAllKeys;
            }
            else
            {
                // We are going to store the slice, therefore we copy.
                firstKey = p.GetNodeKey(_llt, 0, ByteStringType.Immutable);
            }

            Slice lastKey;
            if (rightmostPage == true)
            {
                lastKey = Slices.AfterAllKeys;
            }
            else
            {
                // We are going to store the slice, therefore we copy.
                lastKey = p.GetNodeKey(_llt, p.NumberOfEntries - 1, ByteStringType.Immutable);
            }

            var foundPage = new RecentlyFoundTreePages.FoundTreePage(p.PageNumber, p, firstKey, lastKey, c.ToArray());

            _recentlyFoundPages.Add(foundPage);
        }

        private void AddToRecentlyFoundPages(TreeCursor c, TreePage p, bool leftmostPage, bool rightmostPage)
        {
            Slice firstKey;
            if (leftmostPage == true)
            {
                firstKey = Slices.BeforeAllKeys;
            }
            else
            {
                // We are going to store the slice, therefore we copy.
                firstKey = p.GetNodeKey(_llt, 0, ByteStringType.Immutable);
            }

            Slice lastKey;
            if (rightmostPage == true)
            {
                lastKey = Slices.AfterAllKeys;
            }
            else
            {
                // We are going to store the slice, therefore we copy.
                lastKey = p.GetNodeKey(_llt, p.NumberOfEntries - 1, ByteStringType.Immutable);
            }

            var cursorPath = new long[c.Pages.Count];

            var cur = c.Pages.First;
            int pos = cursorPath.Length - 1;
            while (cur != null)
            {
                cursorPath[pos--] = cur.Value.PageNumber;
                cur = cur.Next;
            }

            var foundPage = new RecentlyFoundTreePages.FoundTreePage(p.PageNumber, p, firstKey, lastKey, cursorPath);

            _recentlyFoundPages.Add(foundPage);
        }

        private bool TryUseRecentTransactionPage(Slice key, out TreePage page, out TreeNodeHeader* node)
        {
            node = null;
            page = null;

            var recentPages = _recentlyFoundPages;
            if (recentPages == null)
                return false;

            var foundPage = recentPages.Find(key);
            if (foundPage == null)
                return false;

            if (foundPage.Page != null)
            {
                // we can't share the same instance, Page instance may be modified by
                // concurrently run iterators
                page = new TreePage(foundPage.Page.Base, foundPage.Page.Source, foundPage.Page.PageSize);
            }
            else
            {
                page = _llt.GetReadOnlyTreePage(foundPage.Number);
            }

            if (page.IsLeaf == false)
                throw new InvalidDataException("Index points to a non leaf page");

            node = page.Search(_llt, key); // will set the LastSearchPosition

            return true;
        }

        private bool TryUseRecentTransactionPage(Slice key, out Func<TreeCursor> cursor, out TreePage page, out TreeNodeHeader* node)
        {
            node = null;
            page = null;
            cursor = null;

            var recentPages = _recentlyFoundPages;
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
                page = new TreePage(foundPage.Page.Base, foundPage.Page.Source, foundPage.Page.PageSize);
            }
            else
            {
                page = _llt.GetReadOnlyTreePage(lastFoundPageNumber);
            }

            if (page.IsLeaf == false)
                throw new InvalidDataException("Index points to a non leaf page");
            
            node = page.Search(_llt, key); // will set the LastSearchPosition

            var cursorPath = foundPage.CursorPath;
            var pageCopy = page;

            cursor = () =>
            {
                var c = new TreeCursor();
                foreach (var p in cursorPath)
                {
                    if (p == lastFoundPageNumber)
                    {
                        c.Push(pageCopy);
                    }
                    else
                    {
                        var cursorPage = _llt.GetReadOnlyTreePage(p);
                        if (key.Options == SliceOptions.Key)
                        {
                            if (cursorPage.Search(_llt, key) != null && cursorPage.LastMatch != 0)
                                cursorPage.LastSearchPosition--;
                        }
                        else if (key.Options == SliceOptions.BeforeAllKeys)
                        {
                            cursorPage.LastSearchPosition = 0;
                        }
                        else if (key.Options == SliceOptions.AfterAllKeys)
                        {
                            cursorPage.LastSearchPosition = (ushort)(cursorPage.NumberOfEntries - 1);
                        }
                        else throw new ArgumentException();

                        c.Push(cursorPage);
                    }
                }
                return c;
            };

            return true;
        }

        internal TreePage NewPage(TreePageFlags flags, int num)
        {
            var page = AllocateNewPage(_llt, flags, num);
            State.RecordNewPage(page, num);

            PageModified?.Invoke(page.PageNumber);

            return page;
        }

        private static TreePage AllocateNewPage(LowLevelTransaction tx, TreePageFlags flags, int num, long? pageNumber = null)
        {
            var page = tx.AllocatePage(num, pageNumber).ToTreePage();
            page.Flags = PageFlags.VariableSizeTreePage | (num == 1 ? PageFlags.Single : PageFlags.Overflow);
            page.Lower = (ushort)Constants.TreePageHeaderSize;
            page.TreeFlags = flags;
            page.Upper = (ushort)page.PageSize;
            page.Dirty = true;

            return page;
        }

        internal void FreePage(TreePage p)
        {
            PageFreed?.Invoke(p.PageNumber);

            if (p.IsOverflow)
            {
                var numberOfPages = _llt.DataPager.GetNumberOfOverflowPages(p.OverflowSize);
                for (int i = 0; i < numberOfPages; i++)
                {
                    _llt.FreePage(p.PageNumber + i);
                }

                State.RecordFreedPage(p, numberOfPages);
            }
            else
            {
                _llt.FreePage(p.PageNumber);
                State.RecordFreedPage(p, 1);
            }
        }

        public void Delete(Slice key, ushort? version = null)
        {
            if (_llt.Flags == (TransactionFlags.ReadWrite) == false)
                throw new ArgumentException("Cannot delete a value in a read only transaction");

            State.IsModified = true;
            Func<TreeCursor> cursorConstructor;
            TreeNodeHeader* node;
            var page = FindPageFor(key, out node, out cursorConstructor);

            if (page.LastMatch != 0)
                return; // not an exact match, can't delete

            page = ModifyPage(page);

            State.NumberOfEntries--;
            ushort nodeVersion;
            RemoveLeafNode(page, out nodeVersion);

            CheckConcurrency(key, version, nodeVersion, TreeActionType.Delete);

            using (var cursor = cursorConstructor())
            {
                var treeRebalancer = new TreeRebalancer(_llt, this, cursor);
                var changedPage = page;
                while (changedPage != null)
                {
                    changedPage = treeRebalancer.Execute(changedPage);
                }
            }

            page.DebugValidate(_llt, State.RootPageNumber);
        }

        public TreeIterator Iterate(bool prefetch)
        {
            return new TreeIterator(this, _llt, prefetch);
        }

        public ReadResult Read(Slice key)       
        {
            TreeNodeHeader* node;
            var p = FindPageFor(key, out node);

            if (p.LastMatch != 0)
                return null;

            return new ReadResult(TreeNodeHeader.Reader(_llt, node), node->Version);
        }

        public int GetDataSize(Slice key)
        {            
            TreeNodeHeader* node;
            var p = FindPageFor(key, out node);
            if (p == null || p.LastMatch != 0)
                return -1;

            if (node == null || !SliceComparer.EqualsInline(TreeNodeHeader.ToSlicePtr(_llt.Allocator, node), key))
                return -1;

            return TreeNodeHeader.GetDataSize(_llt, node);
        }

        public long GetParentPageOf(TreePage page)
        {
            Func<TreeCursor> cursorConstructor;
            TreeNodeHeader* node;
            var p = FindPageFor(page.IsLeaf ? page.GetNodeKey(_llt, 0) : page.GetNodeKey(_llt, 1), out node, out cursorConstructor);
            if (p == null || p.LastMatch != 0)
                return -1;

            using (var cursor = cursorConstructor())
            {
                while (cursor.PageCount > 0)
                {
                    if (cursor.CurrentPage.PageNumber == page.PageNumber)
                    {
                        if (cursor.PageCount == 1)
                            return -1;// root page

                        return cursor.ParentPage.PageNumber;
                    }
                    cursor.Pop();
                }
            }

            return -1;
        }

        public ushort ReadVersion(Slice key)
        {
            TreeNodeHeader* node;
            var p = FindPageFor(key, out node);
            if (p == null || p.LastMatch != 0)
                return 0;

            if (node == null || !SliceComparer.EqualsInline(TreeNodeHeader.ToSlicePtr(_llt.Allocator, node),key))
                return 0;

            return node->Version;
        }

        internal byte* DirectRead(Slice key)
        {
            TreeNodeHeader* node;
            var p = FindPageFor(key, out node);
            if (p == null || p.LastMatch != 0)
                return null;

            Debug.Assert(node != null);

            if (node->Flags == (TreeNodeFlags.PageRef))
            {
                var overFlowPage = _llt.GetReadOnlyTreePage(node->PageNumber);
                return overFlowPage.Base + Constants.TreePageHeaderSize;
            }

            return (byte*)node + node->KeySize + Constants.NodeHeaderSize;
        }

        public List<long> AllPages()
        {
            var results = new List<long>();
            var stack = new Stack<TreePage>();
            var root = _llt.GetReadOnlyTreePage(State.RootPageNumber);
            stack.Push(root);

            Slice key = default(Slice);
            while (stack.Count > 0)
            {
                var p = stack.Pop();
                results.Add(p.PageNumber);
                
                for (int i = 0; i < p.NumberOfEntries; i++)
                {
                    var node = p.GetNode(i);
                    var pageNumber = node->PageNumber;
                    if (p.IsBranch)
                    {
                        stack.Push(_llt.GetReadOnlyTreePage(pageNumber));
                    }
                    else if (node->Flags == TreeNodeFlags.PageRef)
                    {
                        // This is an overflow page
                        var overflowPage = _llt.GetReadOnlyTreePage(pageNumber);
                        var numberOfPages = _llt.DataPager.GetNumberOfOverflowPages(overflowPage.OverflowSize);
                        for (long j = 0; j < numberOfPages; ++j)
                            results.Add(overflowPage.PageNumber + j);
                    }
                    else if (node->Flags == TreeNodeFlags.MultiValuePageRef)
                    {
                        key = TreeNodeHeader.ToSlicePtr(_tx.Allocator, node, ByteStringType.Mutable);
                        var tree = OpenMultiValueTree(key, node);
                        results.AddRange(tree.AllPages());
                    }
                    else
                    {
                        if ((State.Flags & TreeFlags.FixedSizeTrees) == TreeFlags.FixedSizeTrees)
                        {
                            var valueReader = TreeNodeHeader.Reader(_llt, node);
                            var valueSize = ((FixedSizeTreeHeader.Embedded*)valueReader.Base)->ValueSize;

                            var fixedSizeTreeName = p.GetNodeKey(_llt, i);

                            var fixedSizeTree = new FixedSizeTree(_llt, this, fixedSizeTreeName, valueSize);

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
            return Name + " " + State.NumberOfEntries;
        }


        private void CheckConcurrency(Slice key, ushort? expectedVersion, ushort nodeVersion, TreeActionType actionType)
        {
            if (expectedVersion.HasValue && nodeVersion != expectedVersion.Value)
                throw new ConcurrencyException(string.Format("Cannot {0} '{1}' to '{4}' tree. Version mismatch. Expected: {2}. Actual: {3}.", actionType.ToString().ToLowerInvariant(), key, expectedVersion.Value, nodeVersion, Name))
                {
                    ActualETag = nodeVersion,
                    ExpectedETag = expectedVersion.Value,
                };
        }


        private void CheckConcurrency(Slice key, Slice value, ushort? expectedVersion, ushort nodeVersion, TreeActionType actionType)
        {
            if (expectedVersion.HasValue && nodeVersion != expectedVersion.Value)
                throw new ConcurrencyException(string.Format("Cannot {0} value '{5}' to key '{1}' to '{4}' tree. Version mismatch. Expected: {2}. Actual: {3}.", actionType.ToString().ToLowerInvariant(), key, expectedVersion.Value, nodeVersion, Name, value))
                {
                    ActualETag = nodeVersion,
                    ExpectedETag = expectedVersion.Value,
                };
        }

        private enum TreeActionType
        {
            Add,
            Delete
        }
        
        private bool TryOverwriteOverflowPages(TreeNodeHeader* updatedNode, Slice key, int len, ushort? version, out byte* pos)
        {
            if (updatedNode->Flags == TreeNodeFlags.PageRef)
            {
                var readOnlyOverflowPage = _llt.GetReadOnlyTreePage(updatedNode->PageNumber);

                if (len <= readOnlyOverflowPage.OverflowSize)
                {
                    CheckConcurrency(key, version, updatedNode->Version, TreeActionType.Add);

                    if (updatedNode->Version == ushort.MaxValue)
                        updatedNode->Version = 0;
                    updatedNode->Version++;

                    var availableOverflows = _llt.DataPager.GetNumberOfOverflowPages(readOnlyOverflowPage.OverflowSize);

                    var requestedOverflows = _llt.DataPager.GetNumberOfOverflowPages(len);

                    var overflowsToFree = availableOverflows - requestedOverflows;

                    for (int i = 0; i < overflowsToFree; i++)
                    {
                        _llt.FreePage(readOnlyOverflowPage.PageNumber + requestedOverflows + i);
                    }

                    State.RecordFreedPage(readOnlyOverflowPage, overflowsToFree);

                    var writtableOverflowPage = AllocateNewPage(_llt, TreePageFlags.Value, requestedOverflows, updatedNode->PageNumber);

                    writtableOverflowPage.Flags = PageFlags.Overflow | PageFlags.VariableSizeTreePage;
                    writtableOverflowPage.OverflowSize = len;
                    pos = writtableOverflowPage.Base + Constants.TreePageHeaderSize;

                    PageModified?.Invoke(writtableOverflowPage.PageNumber);

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
                if (it.Seek(Slices.AfterAllKeys) == false)
                    return new Slice();

                return it.CurrentKey.Clone(_tx.Allocator);
            }
        }

        public Slice FirstKeyOrDefault()
        {
            using (var it = Iterate(false))
            {
                if (it.Seek(Slices.BeforeAllKeys) == false)
                    return new Slice();

                return it.CurrentKey.Clone(_tx.Allocator);
            }
        }

        public void ClearRecentFoundPages()
        {
            if (_recentlyFoundPages != null)
                _recentlyFoundPages.Clear();
        }

        [MethodImpl(MethodImplOptions.NoInlining)]
        public FixedSizeTree FixedTreeFor(string key, byte valSize = 0)
        {
            return FixedTreeFor(Slice.From(_llt.Allocator, key, ByteStringType.Immutable), valSize);
        }

        public FixedSizeTree FixedTreeFor(Slice key, byte valSize = 0)
        {
            if (_fixedSizeTrees == null)
                _fixedSizeTrees = new Dictionary<Slice, FixedSizeTree>(SliceComparer.Instance);

            FixedSizeTree fixedTree;
            if (_fixedSizeTrees.TryGetValue(key, out fixedTree) == false)
            {
                _fixedSizeTrees[key] = fixedTree = new FixedSizeTree(_llt, this, key, valSize);
            }

            State.Flags |= TreeFlags.FixedSizeTrees;

            return fixedTree;
        }

        [MethodImpl(MethodImplOptions.NoInlining)]
        public long DeleteFixedTreeFor(string key, byte valSize = 0)
        {
            return DeleteFixedTreeFor(Slice.From(_llt.Allocator, key, ByteStringType.Immutable), valSize);
        }

        public long DeleteFixedTreeFor(Slice key, byte valSize = 0)
        {
            var fixedSizeTree = FixedTreeFor(key, valSize);
            var numberOfEntries = fixedSizeTree.NumberOfEntries;

            foreach (var page in fixedSizeTree.AllPages())
            {
                _llt.FreePage(page);
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

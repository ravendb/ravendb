using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Runtime.CompilerServices;
using Sparrow;
using Voron.Data.Compression;
using Voron.Data.Fixed;
using Voron.Data.Tables;
using Voron.Debugging;
using Voron.Exceptions;
using Voron.Impl;
using Voron.Impl.Paging;
using Sparrow.Server;
using Voron.Data.CompactTrees;
using Voron.Data.Lookups;
using Constants = Voron.Global.Constants;
using System.Diagnostics.CodeAnalysis;
using Sparrow.Json;

namespace Voron.Data.BTrees
{
    public unsafe partial class Tree
    {
        private int _directAddUsage;

#if VALIDATE_DIRECT_ADD_STACKTRACE
        private string _allocationStacktrace;
#endif

        private readonly TreeMutableState _state;

        // in memory of the transaction's allocator, taken when the first leaf is cached, see AddToRecentlyFoundPages
        private RecentlyFoundTreePages* _recentlyFoundPages;
        private ByteString _recentlyFoundPagesMemory;

        // Iterate hands it out again once it is disposed, nested iterators are rare but they are valid (see OngoingTasksBase.GetData).
        // When there is nesting, we'll create a new instance
        private TreeIterator _reusableIterator;

        // Page pool to reduce allocations
        // Nested ops (e.g. a write from the free space handling of a write) rents the next instance, so two operations never share one.
        // the deepest nesting is 3, e.g. MultiAdd -> DirectAdd -> DescendToLeaf, a deeper one gets an instance that is not pooled
        private const int PagePoolSize = 3;

        [InlineArray(PagePoolSize)]
        private struct PagePool
        {
            private TreePage _element;
        }

        private PagePool _pagePool;
        private int _rentedPages;

        private TreeCursor _cursor;
        private bool _cursorInUse;

        private Dictionary<Slice, FixedSizeTree> _fixedSizeTrees;
        private Dictionary<Slice, FixedSizeTree<double>> _fixedSizeTreesForDouble;

        private SliceSmallSet<IPrepareForCommit> _prepareLocator;

        public event Action<long, PageFlags> PageModified;
        public event Action<long, PageFlags> PageFreed;

        public Slice Name { get; private set; }

        public TreeMutableState State => _state;

        private readonly LowLevelTransaction _llt;
        private readonly Transaction _tx;
        public readonly bool IsIndexTree;
        private NewPageAllocator _newPageAllocator;

        public LowLevelTransaction Llt => _llt;

        private Tree(LowLevelTransaction llt, Transaction tx, in TreeRootHeader header, Slice name, bool isIndexTree, NewPageAllocator newPageAllocator)
        {
            _llt = llt;
            _tx = tx;
            IsIndexTree = isIndexTree;
            Name = name;

            if (newPageAllocator != null)
            {
                Debug.Assert(isIndexTree, "If newPageAllocator is set, we must be in a isIndexTree = true");
                SetNewPageAllocator(newPageAllocator);
            }

            _state = new TreeMutableState(llt, in header);

            llt.RegisterDisposable(new TreeDisposable(this));
        }

        private Tree(LowLevelTransaction llt, Transaction tx, Slice name, bool isIndexTree, NewPageAllocator newPageAllocator)
        {
            _llt = llt;
            _tx = tx;
            IsIndexTree = isIndexTree;
            Name = name;

            if (newPageAllocator != null)
            {
                Debug.Assert(isIndexTree, "If newPageAllocator is set, we must be in a isIndexTree = true");
                SetNewPageAllocator(newPageAllocator);
            }

            _state = new TreeMutableState(llt);

            llt.RegisterDisposable(new TreeDisposable(this));
        }

        public Tree(LowLevelTransaction llt, Transaction tx, Slice name, TreeMutableState state)
        {
            _llt = llt;
            _tx = tx;
            Name = name;

            _state = state;

            llt.RegisterDisposable(new TreeDisposable(this));
        }

        private class TreeDisposable(Tree tree) : IDisposable
        {
            void IDisposable.Dispose()
            {
                // the allocator may outlive the transaction (a context's allocator), so the memory goes back to it now
                if (tree._recentlyFoundPages == null)
                    return;

                tree._recentlyFoundPages = null;
                tree._llt.Allocator.Release(ref tree._recentlyFoundPagesMemory);
            }
        }

        public bool IsLeafCompressionSupported
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get { return (State.Header.Flags & TreeFlags.LeafsCompressed) == TreeFlags.LeafsCompressed; }
        }

        public bool HasNewPageAllocator { get; private set; }

        public static Tree Open(LowLevelTransaction llt, Transaction tx, Slice name, in TreeRootHeader header, bool isIndexTree = false, NewPageAllocator newPageAllocator = null)
        {
            var tree = new Tree(llt, tx, header, name, isIndexTree, newPageAllocator);

            if ((tree.State.Header.Flags & TreeFlags.LeafsCompressed) == TreeFlags.LeafsCompressed)
                tree.InitializeCompression();

            return tree;
        }

        public static Tree Create(LowLevelTransaction llt, Transaction tx, Slice name, TreeFlags flags = TreeFlags.None, RootObjectType type = RootObjectType.VariableSizeTree,
            bool isIndexTree = false, NewPageAllocator newPageAllocator = null)
        {
            if (type != RootObjectType.VariableSizeTree && type != RootObjectType.Table)
                ThrowInvalidTreeCreateType();

            if (llt.Flags != TransactionFlags.ReadWrite)
                ThrowCannotCreateTreeInReadTx();

            var newPage = newPageAllocator?.AllocateSinglePage(0) ?? llt.AllocatePage(1);

            TreePage newRootPage = PrepareTreePage(TreePageFlags.Leaf, 1, newPage);

            var tree = new Tree(llt, tx, name, isIndexTree, newPageAllocator);

            ref var state = ref tree.State.Modify();
            state.RootPageNumber = newRootPage.PageNumber;
            state.RootObjectType = type;
            state.Depth = 1;
            state.Flags = flags;

            if ((flags & TreeFlags.LeafsCompressed) == TreeFlags.LeafsCompressed)
                tree.InitializeCompression();

            tree.RecordNewPage(newRootPage, 1);
            return tree;
        }

        [DoesNotReturn]
        private static void ThrowCannotCreateTreeInReadTx()
        {
            throw new ArgumentException("Cannot create a tree in a read only transaction");
        }

        [DoesNotReturn]
        private static void ThrowInvalidTreeCreateType()
        {
            throw new ArgumentException($"Only valid types are {nameof(RootObjectType.VariableSizeTree)} or {nameof(RootObjectType.Table)}.");
        }

        internal void RecordNewPage(TreePage p, int num)
        {
            ref var header = ref State.Modify();

            header.PageCount += num;

            if (p.IsBranch)
            {
                header.BranchPages++;
            }
            else if (p.IsLeaf)
            {
                header.LeafPages++;
            }
            else if (p.IsOverflow)
            {
                header.OverflowPages += num;
            }
        }

        internal void RecordFreedPage(TreePage p, int num)
        {
            ref var header = ref State.Modify();

            header.PageCount -= num;
            Debug.Assert(header.PageCount >= 0);

            if (p.IsBranch)
            {
                header.BranchPages--;
                Debug.Assert(header.BranchPages >= 0);
            }
            else if (p.IsLeaf)
            {
                header.LeafPages--;
                Debug.Assert(header.LeafPages >= 0);
            }
            else if (p.IsOverflow)
            {
                header.OverflowPages -= num;
                Debug.Assert(header.OverflowPages >= 0);
            }
        }

        /// <summary>
        /// This is using little endian
        /// </summary>
        public long Increment(Slice key, long delta)
        {
            Debug.Assert((State.Header.Flags & TreeFlags.MultiValue) == TreeFlags.None,"(State.Flags & TreeFlags.MultiValue) == TreeFlags.None");
            
            long currentValue = 0;

            if (TryRead(key, out var read))
                currentValue = *(long*)read.Base;

            var value = currentValue + delta;
            using (DirectAdd(key, sizeof(long), out byte* ptr))
                *(long*)ptr = value;

            return value;
        }

        /// <summary>
        /// This is using little endian
        /// </summary>
        public long? ReadInt64(Slice key)
        {
            if (TryRead(key, out var reader) == false)
                return null;
            
            Debug.Assert(reader.Length == sizeof(long));
            return *(long*)reader.Base;
        }

        /// <summary>
        /// This is using little endian
        /// </summary>
        public int? ReadInt32(Slice key)
        {
            if (TryRead(key, out var reader) == false)
                return null;
            
            Debug.Assert(reader.Length == sizeof(int));
            return *(int*)reader.Base;
        }
        
        /// <summary>
        /// This is using little endian
        /// </summary>
        public T? ReadStructure<T>(Slice key)
            where T : unmanaged
        {
            if (TryRead(key, out var reader) == false)
                return null;
            
            Debug.Assert(reader.Length == sizeof(T));
            return *(T*)reader.Base;
        }

        public void Add(Slice key, byte value)
        {
            using (DirectAdd(key, sizeof(byte), out byte* ptr))
                *ptr = value;
        }

        public void Add(Slice key, long value)
        {
            Debug.Assert((State.Header.Flags & TreeFlags.MultiValue) == TreeFlags.None,"(State.Flags & TreeFlags.MultiValue) == TreeFlags.None");
            using (DirectAdd(key, sizeof(long), out byte* ptr))
                *(long*)ptr = value;
        }

        public void Add(Slice key, int value)
        {
            using (DirectAdd(key, sizeof(int), out byte* ptr))
                *(int*)ptr = value;
        }


        public void Add(Slice key, Stream value)
        {
            ValidateValueLength(value);

            var length = (int)value.Length;

            using (DirectAdd(key, length, out byte* ptr))
            {
                value.ReadExactly(new Span<byte>(ptr, length));
            }
        }

        private static void ValidateValueLength(Stream value)
        {
            if (value == null)
                ThrowNullReferenceException();
            Debug.Assert(value != null);
            if (value.Length > int.MaxValue)
                ThrowValueTooLarge();
        }

        [DoesNotReturn]
        private static void ThrowValueTooLarge()
        {
            throw new ArgumentException("Cannot add a value that is over 2GB in size");
        }

        [DoesNotReturn]
        private static void ThrowNullReferenceException()
        {
            throw new ArgumentNullException();
        }

        public void Add(Slice key, ReadOnlySpan<byte> value)
        {
            if (value == ReadOnlySpan<byte>.Empty)
                ThrowNullReferenceException();

            Debug.Assert(value != ReadOnlySpan<byte>.Empty);

            using (DirectAdd(key, value.Length, out byte* ptr))
                value.CopyTo(new Span<byte>(ptr, value.Length));
        }

        public void Add(Slice key, Slice value)
        {
            Debug.Assert((State.Header.Flags & TreeFlags.MultiValue) == TreeFlags.None,"(State.Flags & TreeFlags.MultiValue) == TreeFlags.None");
            
            if (!value.HasValue)
                ThrowNullReferenceException();

            using (DirectAdd(key, value.Size, out byte* ptr))
                value.CopyTo(ptr);
        }
        

        public static int CalcSizeOfEmbeddedEntry(int keySize, int entrySize)
        {
            var size = (Constants.Tree.NodeHeaderSize + keySize + entrySize);
            return size + (size & 1);
        }

        public DirectAddScope DirectAdd(Slice key, int len, out byte* ptr)
        {
            return DirectAdd(key, len, TreeNodeFlags.Data, out ptr);
        }
        
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private DirectAddScope AddKeyOnly(Slice key)
        {
            return DirectAdd(key, 0, TreeNodeFlags.Data, out _, populateDataPtr: false);
        }

        public DirectAddScope DirectAdd(Slice key, int len, TreeNodeFlags nodeType, out byte* ptr, bool populateDataPtr = true)
        {
            using var rental = RentPage(out var leafInstance);
            return DirectAdd(key, len, nodeType, out ptr, populateDataPtr, leafInstance);
        }

        // nothing may keep the page after the rental ends, the next operations on this tree point it at other pages
        internal PageRental RentPage(out TreePage page)
        {
            page = _rentedPages < PagePoolSize
                ? _pagePool[_rentedPages] ??= new TreePage(null, Constants.Storage.PageSize)
                : new TreePage(null, Constants.Storage.PageSize); // nested deeper than the pool
            _rentedPages++;
            return new PageRental(this);
        }

        internal readonly struct PageRental(Tree tree) : IDisposable
        {
            public void Dispose() => tree._rentedPages--;
        }

        // a pooled instance is pointed at other pages by the next operations, so nothing may keep it after the one that rented it
        internal bool IsPooled(TreePage page)
        {
            for (int i = 0; i < PagePoolSize; i++)
            {
                if (ReferenceEquals(_pagePool[i], page))
                    return true;
            }

            return false;
        }

        private DirectAddScope DirectAdd(Slice key, int len, TreeNodeFlags nodeType, out byte* ptr, bool populateDataPtr, TreePage leafInstance)
        {
            AssertNotDisposed();

            if (_llt.Flags is TransactionFlags.Read)
                ThrowCannotAddInReadTx();

            if (AbstractPager.IsKeySizeValid(key.Size) == false)
                ThrowInvalidKeySize(key);

            var foundPage = FindPageFor(key, node: out TreeNodeHeader* node, allowCompressed: true, leafInstance: leafInstance);

            if (populateDataPtr == false && len == 0 && nodeType == TreeNodeFlags.Data && foundPage.LastMatch == 0)
            {
#if DEBUG
                node = foundPage.GetNode(foundPage.LastSearchPosition);
                using (TreeNodeHeader.ToSlicePtr(_llt.Allocator, node, out Slice nodeCheck))
                {
                    Debug.Assert(SliceComparer.Equals(nodeCheck, key));
                }
#endif
                
                ptr = null;
                return new(this);
            }
            
            var page = ModifyPage(foundPage);

            bool? shouldGoToOverflowPage = null;
            if (page.LastMatch == 0) // this is an update operation
            {
                if ((nodeType & TreeNodeFlags.NewOnly) == TreeNodeFlags.NewOnly)
                    ThrowConcurrencyException();
                
                node = page.GetNode(page.LastSearchPosition);

#if DEBUG
                using (TreeNodeHeader.ToSlicePtr(_llt.Allocator, node, out Slice nodeCheck))
                {
                    Debug.Assert(SliceComparer.EqualsInline(nodeCheck, key));
                }
#endif
                shouldGoToOverflowPage = ShouldGoToOverflowPage(len);

                byte* pos;
                if (shouldGoToOverflowPage == false)
                {
                    // optimization for Data and MultiValuePageRef - try to overwrite existing node space
                    if (TryOverwriteDataOrMultiValuePageRefNode(node, len, nodeType, out pos))
                    {
                        ptr = pos;
                        return new DirectAddScope(this);
                    }
                }
                else
                {
                    // optimization for PageRef - try to overwrite existing overflows
                    if (TryOverwriteOverflowPages(node, len, out pos))
                    {
                        ptr = pos;
                        return new DirectAddScope(this);
                    }
                }

                RemoveLeafNode(page);
            }
            else // new item should be recorded
            {
                ref var state = ref State.Modify();
                state.NumberOfEntries++;
            }
            
            nodeType &= ~TreeNodeFlags.NewOnly;
            Debug.Assert(nodeType == TreeNodeFlags.Data || nodeType == TreeNodeFlags.MultiValuePageRef);

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
                DecompressedLeafPage decompressedPageToSplit = null;

                if (IsLeafCompressionSupported == false || TryCompressPageNodes(key, len, page, out decompressedPageToSplit) == false)
                {
                    using var cursorRental = BuildCursor(key, page, out var cursor);

                    var pageSplitter = new TreePageSplitter(_llt, this, key, len, pageNumber, nodeType, cursor, pageDecompressed: decompressedPageToSplit);
                    dataPos = pageSplitter.Execute();

                    DebugValidateTree(State.Header.RootPageNumber);

                    ptr = overFlowPos == null ? dataPos : overFlowPos;
                    return new DirectAddScope(this);
                }

                // existing values compressed and put at the end of the page, let's insert from Upper position
                lastSearchPosition = 0;
            }

            switch (nodeType)
            {
                case TreeNodeFlags.PageRef:
                    dataPos = page.AddPageRefNode(lastSearchPosition, key, pageNumber);
                    break;
                case TreeNodeFlags.Data:
                    dataPos = page.AddDataNode(lastSearchPosition, key, len);
                    break;
                case TreeNodeFlags.MultiValuePageRef:
                    dataPos = page.AddMultiValueNode(lastSearchPosition, key, len);
                    break;
                default:
                    ThrowUnknownNodeTypeAddOperation(nodeType);
                    dataPos = null; // never executed
                    break;
            }

            page.DebugValidate(this, State.Header.RootPageNumber);

            ptr = overFlowPos == null ? dataPos : overFlowPos;
            return new DirectAddScope(this);
        }

        [DoesNotReturn]
        private static void ThrowConcurrencyException()
        {
            throw new VoronConcurrencyErrorException("Value already exists, but requested NewOnly");
        }

        public readonly struct DirectAddScope : IDisposable
        {
            private readonly Tree _parent;

            public DirectAddScope(Tree parent)
            {
                _parent = parent;
                if (_parent._directAddUsage++ != 0)
                {
                    ThrowScopeAlreadyOpen();
                }

#if VALIDATE_DIRECT_ADD_STACKTRACE
                _parent._allocationStacktrace = Environment.StackTrace;
#endif
            }

            public void Dispose()
            {
                _parent._directAddUsage--;
            }


            [DoesNotReturn]
            private void ThrowScopeAlreadyOpen()
            {
                var message = $"Write operation already requested on a tree name: {_parent}. " +
                              $"{nameof(Tree.DirectAdd)} method cannot be called recursively while the scope is already opened.";

#if VALIDATE_DIRECT_ADD_STACKTRACE
                message += Environment.NewLine + _parent._allocationStacktrace;
#endif

                throw new InvalidOperationException(message);
            }

        }

        private static void ThrowUnknownNodeTypeAddOperation(TreeNodeFlags nodeType)
        {
            throw new NotSupportedException("Unknown node type for direct add operation: " + nodeType);
        }

        [DoesNotReturn]
        private static void ThrowInvalidKeySize(Slice key)
        {
            throw new ArgumentException(
                $"Key size is too big, must be at most {AbstractPager.MaxKeySize} bytes, but was {(key.Size + AbstractPager.RequiredSpaceForNewNode)}",
                nameof(key));
        }

        private void ThrowCannotAddInReadTx()
        {
            throw new ArgumentException("Cannot add a value in a read only transaction on " + Name + " in " + _llt.Flags);
        }

        public TreePage ModifyPage(TreePage page)
        {
            AssertNotDisposed();

            if (page.Dirty)
                return page;

            if (page.GetType() != typeof(TreePage)) // e.g. DecompressedLeafPage
            {
                var newPage = ModifyPage(page.PageNumber);
                newPage.LastSearchPosition = page.LastSearchPosition;
                newPage.LastMatch = page.LastMatch;
                return newPage;
            }

            return ModifyPage(page.PageNumber, page); // reuse the instance
        }

        internal TreePage ModifyPage(long pageNumber, TreePage instance)
        {
            AssertNotDisposed();

            instance.Base = _llt.ModifyPage(pageNumber).Pointer;
            instance.Dirty = true;
            OnPageModified(instance);

            return instance;
        }

        public TreePage ModifyPage(long pageNumber) => ModifyPage(pageNumber, new TreePage(null, Constants.Storage.PageSize));

        private void OnPageModified(TreePage page)
        {
            var pageNumber = page.PageNumber;

            if (page.IsBranch)
            {
                // a separator changed, keys may have moved between leaves
                ClearPagesCache();
            }
            else if (IsLeafCompressionSupported)
            {
                // compressing the page moves keys out of the range its cache entry was created for
                if (_recentlyFoundPages != null)
                    _recentlyFoundPages->Reset(pageNumber);
            }
            else
            {
                // a write to a leaf keeps its keys within its cached range
                if (_recentlyFoundPages != null)
                    _recentlyFoundPages->Refresh(pageNumber, page.Base);
            }

            if (IsLeafCompressionSupported && page.IsCompressed)
                DecompressionsCache.Invalidate(pageNumber, DecompressionUsage.Read);

            PageModified?.Invoke(pageNumber, page.Flags);
        }

        public bool ShouldGoToOverflowPage(int len)
        {
            return len + Constants.Tree.NodeHeaderSize > _llt.DataPager.NodeMaxSize;
        }

        private long WriteToOverflowPages(int overflowSize, out byte* dataPos)
        {
            var numberOfPages = VirtualPagerLegacyExtensions.GetNumberOfOverflowPages(overflowSize);
            var newPage = _llt.AllocatePage(numberOfPages);

            TreePage overflowPageStart = PrepareTreePage(TreePageFlags.Value, numberOfPages, newPage);

            overflowPageStart.Flags = PageFlags.Overflow | PageFlags.VariableSizeTreePage;
            overflowPageStart.OverflowSize = overflowSize;
            dataPos = overflowPageStart.Base + Constants.Tree.PageHeaderSize;

            RecordNewPage(overflowPageStart, numberOfPages);

            PageModified?.Invoke(overflowPageStart.PageNumber, overflowPageStart.Flags);

            return overflowPageStart.PageNumber;
        }

        internal void RemoveLeafNode(TreePage page)
        {
            var node = page.GetNode(page.LastSearchPosition);
            if (node->Flags == (TreeNodeFlags.PageRef)) // this is an overflow pointer
            {
                var overflowPage = GetReadOnlyTreePage(node->PageNumber);
                FreePage(overflowPage);
            }

            page.RemoveNode(page.LastSearchPosition);
        }

        [Conditional("VALIDATE")]
        public void DebugValidateTree(long rootPageNumber)
        {
            ValidateTree_Forced(rootPageNumber);
        }

        public void ValidateTree_Forced(long rootPageNumber)
        {


            var pages = new HashSet<long>();
            var stack = new Stack<TreePage>();
            var root = GetReadOnlyTreePage(rootPageNumber);
            stack.Push(root);
            pages.Add(rootPageNumber);
            var leafKeys = new HashSet<Slice>(SliceComparer.Instance);

            try
            {
                while (stack.Count > 0)
                {
                    var p = stack.Pop();

                    using (p.IsCompressed ? (DecompressedLeafPage)(p = DecompressPage(p, DecompressionUsage.Read, skipCache: true)) : null)
                    {
                        if (p.NumberOfEntries == 0 && p != root)
                        {
                            DebugStuff.RenderAndShowTree(this, rootPageNumber);
                            throw new InvalidOperationException("The page " + p.PageNumber + " is empty");

                        }
                        p.DebugValidate(this, rootPageNumber);

                        if (p.IsBranch == false)
                        {
                            for (int i = 0; i < p.NumberOfEntries; i++)
                            {
                                using (TreeNodeHeader.ToSlicePtr(_llt.Allocator, p.GetNode(i), out Slice keySlice))
                                {
                                    var clonedKey = keySlice.Clone(_llt.Allocator);

                                    if (leafKeys.Add(clonedKey) == false)
                                    {
                                        DebugStuff.RenderAndShowTree(this, rootPageNumber);
                                        throw new InvalidOperationException("The key '" + keySlice + "' already appeared in the tree");
                                    }
                                }
                            }

                            continue;
                        }

                        if (p.NumberOfEntries < 2)
                        {
                            throw new InvalidOperationException("The branch page " + p.PageNumber + " has " +
                                                                p.NumberOfEntries + " entry");
                        }

                        for (int i = 0; i < p.NumberOfEntries; i++)
                        {
                            var page = p.GetNode(i)->PageNumber;
                            if (pages.Add(page) == false)
                            {
                                DebugStuff.RenderAndShowTree(this, rootPageNumber);
                                throw new InvalidOperationException("The page " + page + " already appeared in the tree!");
                            }
                            stack.Push(GetReadOnlyTreePage(page));
                        }
                    }
                }
            }
            finally
            {
                foreach (var key in leafKeys)
                {
                    key.Release(_llt.Allocator);
                }
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal TreePage GetReadOnlyTreePage(long pageNumber) => UseInstance(null, _llt.GetPage(pageNumber).Pointer);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal Page GetReadOnlyPage(long pageNumber)
        {
            return _llt.GetPage(pageNumber);
        }

        internal TreePage FindPageFor(Slice key, out TreeNodeHeader* node, bool allowCompressed = false, bool backward = false, TreePage leafInstance = null)
        {
            if (TryUseRecentTransactionPage(key, out TreePage p, out node, backward, leafInstance))
            {
                if (allowCompressed == false && p.IsCompressed)
                    ThrowOnCompressedPage(p);
            }
            else
            {
                p = SearchForPage(key, allowCompressed, out node, backward: backward, leafInstance: leafInstance);
            }

            return p;
        }

        /// <summary>
        /// Also pushes the branch pages above the returned leaf into <paramref name="branches"/>.
        /// </summary>
        internal TreePage FindPageFor(Slice key, ITreeDescentPath branches, TreePage leafInstance, out TreeNodeHeader* node, bool allowCompressed, bool backward)
        {
            return SearchForPage(key, allowCompressed, out node, branches, backward: backward, leafInstance: leafInstance);
        }

        internal CursorRental BuildCursor(Slice key, TreePage leaf, out TreeCursor cursor)
        {
            var rented = _cursorInUse == false;
            cursor = rented ? _cursor ??= new TreeCursor() : new TreeCursor();
            _cursorInUse = true;
            cursor.StartPath();

            var leafPageNumber = ((TreePageHeader*)DescendToLeaf(key, cursor, out _, out _))->PageNumber;
            if (leafPageNumber != leaf.PageNumber)
                VoronUnrecoverableErrorException.Raise(_llt,
                    $"Could not build the cursor for leaf page {leaf.PageNumber}: descending by key '{key}' reaches page {leafPageNumber}, tree: {Name}");

            cursor.Push(leaf);
            return new CursorRental(rented ? this : null);
        }

        internal readonly struct CursorRental(Tree tree) : IDisposable
        {
            public void Dispose() => tree?._cursorInUse = false;
        }

        private TreePage SearchForPage(Slice key, out TreeNodeHeader* node) => SearchForPage(key, allowCompressed: false, out node);

        private TreePage SearchForPage(Slice key, bool allowCompressed, out TreeNodeHeader* node, ITreeDescentPath branches = null, bool addToRecentlyFoundPages = true,
            bool backward = false, TreePage leafInstance = null)
        {
            var leaf = DescendToLeaf(key, branches, out bool leftmostPage, out bool rightmostPage);
            var p = UseInstance(leafInstance, leaf);

            if (p.IsLeaf == false)
                VoronUnrecoverableErrorException.Raise(_llt, "Index points to a non leaf page " + p.PageNumber);

            if (allowCompressed == false && p.IsCompressed)
                ThrowOnCompressedPage(p);

            node = p.Search(_llt, key, backward); // will set the LastSearchPosition

            if (p.NumberOfEntries > 0 && addToRecentlyFoundPages) // compressed page can have no ordinary entries
                AddToRecentlyFoundPages(p, leftmostPage, rightmostPage);

            return p;
        }

        // Points the instance at the page, or creates one when there is no instance to reuse.
        private static TreePage UseInstance(TreePage instance, byte* pagePointer)
        {
            if (instance == null)
                return new TreePage(pagePointer, Constants.Storage.PageSize);

            instance.Base = pagePointer;
            instance.Dirty = false;
            return instance;
        }

        // Returns the leaf page that the key belongs to. The branch pages on the way are pushed to branches, when given.
        internal byte* DescendToLeaf(Slice key, ITreeDescentPath branches, out bool leftmostPage, out bool rightmostPage)
        {
            using var rental = RentPage(out var p);
            var ptr = _llt.GetPage(State.Header.RootPageNumber).Pointer;

            leftmostPage = true;
            rightmostPage = true;

            while ((((TreePageHeader*)ptr)->TreeFlags & TreePageFlags.Branch) == TreePageFlags.Branch)
            {
                p.Base = ptr;

                int nodePos;
                if (key.Options == SliceOptions.Key)
                {
                    nodePos = SetLastSearchPosition(key, p, ref leftmostPage, ref rightmostPage);
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

                branches?.PushBranch(p);

                var pageNode = p.GetNode(nodePos);
                ptr = _llt.GetPage(pageNode->PageNumber).Pointer;
                Debug.Assert(pageNode->PageNumber == ((TreePageHeader*)ptr)->PageNumber,
                    $"Requested Page: #{pageNode->PageNumber}. Got Page: #{((TreePageHeader*)ptr)->PageNumber}");
            }

            return ptr;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private int SetLastSearchPosition(Slice key, TreePage p, ref bool leftmostPage, ref bool rightmostPage)
        {
            if (p.Search(_llt, key) != null)
            {
                if (p.LastMatch != 0)
                {
                    p.LastSearchPosition--;
                }

                if (p.LastSearchPosition != 0)
                    leftmostPage = false;

                rightmostPage = false;
            }
            else
            {
                p.LastSearchPosition--;

                leftmostPage = false;
            }

            Debug.Assert(p.LastSearchPosition >= 0, $"Page LastSearchPosition should be positive, LastSearchPosition: {p.LastSearchPosition}, PageNumber: {p.PageNumber}");
            return p.LastSearchPosition;
        }

        [DoesNotReturn]
        private static void ThrowOnCompressedPage(TreePage p)
        {
            throw new PageCompressedException($"Page {p} is compressed. You need to decompress it to be able to access its content.");
        }

        private void AddToRecentlyFoundPages(TreePage p, bool leftmostPage, bool rightmostPage)
        {
            if (_recentlyFoundPages == null)
                AllocateRecentlyFoundPages();

            SliceOptions firstKeyOption, lastKeyOption;
            ReadOnlySpan<byte> firstKey, lastKey;

            if (leftmostPage)
            {
                firstKey = ReadOnlySpan<byte>.Empty;
                firstKeyOption = Slices.BeforeAllKeys.Options;
            }
            else
            {
                p.GetNodeKey(0, out firstKey);
                firstKeyOption = SliceOptions.Key;
            }

            if (rightmostPage)
            {
                lastKey = ReadOnlySpan<byte>.Empty;
                lastKeyOption = Slices.AfterAllKeys.Options;
            }
            else
            {
                p.GetNodeKey(p.NumberOfEntries - 1, out lastKey);
                lastKeyOption = SliceOptions.Key;
            }

            _recentlyFoundPages->Add(p, firstKeyOption, firstKey, lastKeyOption, lastKey);
        }

        private void AllocateRecentlyFoundPages()
        {
            // 63 bytes more, so the cache starts on a 64 bytes boundary and its Vector512 loads are aligned
            _llt.Allocator.Allocate(sizeof(RecentlyFoundTreePages) + 63, out _recentlyFoundPagesMemory);
            _recentlyFoundPages = (RecentlyFoundTreePages*)(((nuint)_recentlyFoundPagesMemory.Ptr + 63) & ~(nuint)63);
            _recentlyFoundPages->Initialize();
        }

        private bool TryUseRecentTransactionPage(Slice key, out TreePage page, out TreeNodeHeader* node, bool backward, TreePage leafInstance)
        {
            if (_recentlyFoundPages == null || _recentlyFoundPages->TryFind(key, out byte* pagePointer) == false)
            {
                page = null;
                node = null;
                return false;
            }

            AssertCachedLeafIsCurrent(key, pagePointer);

            page = UseInstance(leafInstance, pagePointer);

            if (page.IsLeaf == false)
                VoronUnrecoverableErrorException.Raise(_llt, "Index points to a non leaf page");

            node = page.Search(_llt, key, backward); // will set the LastSearchPosition
            return true;
        }

        [Conditional("DEBUG")]
        private void AssertCachedLeafIsCurrent(Slice key, byte* cachedPagePointer)
        {
            var cachedPageNumber = ((TreePageHeader*)cachedPagePointer)->PageNumber;
            var leaf = DescendToLeaf(key, branches: null, out _, out _);
            var leafPageNumber = ((TreePageHeader*)leaf)->PageNumber;

            Debug.Assert(leafPageNumber == cachedPageNumber,
                $"{Name}: the pages cache has page {cachedPageNumber} for key '{key}', but the key belongs to page {leafPageNumber}. A change that moved keys between leaves did not clear the cache.");

            // the same page can be mapped at more than one address, so compare the content: an old copy differs once the page is written
            Debug.Assert(leaf == cachedPagePointer || Memory.Compare(leaf, cachedPagePointer, Constants.Storage.PageSize) == 0,
                $"{Name}: the pages cache points to an old copy of page {cachedPageNumber}. The page was modified without Tree.ModifyPage.");
        }


        internal TreePage NewPage(TreePageFlags flags, long nearbyPage)
        {
            var newPage = _newPageAllocator?.AllocateSinglePage(nearbyPage) ?? _llt.AllocatePage(1);

            var page = PrepareTreePage(flags, 1, newPage);

            RecordNewPage(page, 1);

            PageModified?.Invoke(page.PageNumber, page.Flags);

            return page;
        }

        private static TreePage PrepareTreePage(TreePageFlags flags, int num, Page newPage)
        {
            var page = new TreePage(newPage.Pointer, Constants.Storage.PageSize)
            {
                Flags = PageFlags.VariableSizeTreePage | (num == 1 ? PageFlags.Single : PageFlags.Overflow),
                Lower = Constants.Tree.PageHeaderSize,
                TreeFlags = flags,
                Upper = Constants.Storage.PageSize,
                Dirty = true
            };
            return page;
        }


        internal void FreePage(TreePage p)
        {
#if VALIDATE
            p.Freed = true;
#endif
            PageFreed?.Invoke(p.PageNumber, p.Flags);

            if (p.IsOverflow)
            {
                var numberOfPages = VirtualPagerLegacyExtensions.GetNumberOfOverflowPages(p.OverflowSize);
                for (int i = 0; i < numberOfPages; i++)
                {
                    _llt.FreePage(p.PageNumber + i);
                }

                RecordFreedPage(p, numberOfPages);
            }
            else
            {
                if (_newPageAllocator != null)
                {
                    if (IsIndexTree == false)
                        ThrowAttemptToFreePageToNewPageAllocator(Name, p.PageNumber);

                    _newPageAllocator.FreePage(p.PageNumber);
                }
                else
                {
                    if (IsIndexTree)
                        ThrowAttemptToFreeIndexPageToFreeSpaceHandling(Name, p.PageNumber);

                    _llt.FreePage(p.PageNumber);
                }

                RecordFreedPage(p, 1);
            }
        }

        [DoesNotReturn]
        public static void ThrowAttemptToFreeIndexPageToFreeSpaceHandling(Slice treeName, long pageNumber)
        {
            throw new InvalidOperationException($"Attempting to free page #{pageNumber} of '{treeName}' index tree to the free space handling. The page was allocated by {nameof(NewPageAllocator)} so it needs to be returned there.");
        }

        [DoesNotReturn]
        public static void ThrowAttemptToFreePageToNewPageAllocator(Slice treeName, long pageNumber)
        {
            throw new InvalidOperationException($"Attempting to free page #{pageNumber} of '{treeName}' tree to {nameof(NewPageAllocator)} while it wasn't allocated by it");
        }

        public void Delete(Slice key)
        {
            using var rental = RentPage(out var leafInstance);
            Delete(key, leafInstance);
        }

        private void Delete(Slice key, TreePage leafInstance)
        {
            AssertNotDisposed();

            if (_llt.Flags == (TransactionFlags.ReadWrite) == false)
                throw new ArgumentException("Cannot delete a value in a read only transaction");

            var page = FindPageFor(key, node: out TreeNodeHeader* _, allowCompressed: true, leafInstance: leafInstance);
            if (page.IsCompressed)
            {
                DeleteOnCompressedPage(page, key);
                return;
            }

            if (page.LastMatch != 0)
                return; // not an exact match, can't delete

            page = ModifyPage(page);

            ref var state = ref State.Modify();
            state.NumberOfEntries--;

            RemoveLeafNode(page);

            // a leaf above the fill threshold needs no rebalancing, saving the TreeRebalancer work
            if (page.NumberOfEntries > 0 && page.UseMoreSizeThan(_llt.DataPager.PageMinSpace))
            {
                page.DebugValidate(this, State.Header.RootPageNumber);
                return;
            }

            using var cursorRental = BuildCursor(key, page, out var cursor);
            var treeRebalancer = new TreeRebalancer(_llt, this, cursor);
            var changedPage = page;
            while (changedPage != null)
            {
                changedPage = treeRebalancer.Execute(changedPage);
            }

            page.DebugValidate(this, State.Header.RootPageNumber);
        }

        public TreeIterator Iterate(bool prefetch)
        {
            AssertNotDisposed();

            var iterator = _reusableIterator;
            if (iterator == null)
                return _reusableIterator = new TreeIterator(this, _llt, prefetch);

            if (iterator.IsDisposed == false)
                return new TreeIterator(this, _llt, prefetch);

            iterator.Reuse(prefetch);
            return iterator;
        }

        public bool TryRead(Slice key, out ValueReader value)
        {
            AssertNotDisposed();

            Unsafe.SkipInit(out value);
            
            using var rental = RentPage(out var leafInstance);
            var p = FindPageFor(key, out TreeNodeHeader* node, leafInstance: leafInstance);

            if (p.LastMatch != 0)
                return false;

            value = GetValueReaderFromHeader(node);
            return true;
        }

        /// <summary>
        /// This is using little endian.
        /// </summary>
        public int ReadInt32OrDefault(Slice key, int defaultValue = 0)
        {
            return TryRead(key, out var reader) ? reader.ReadLittleEndianInt32() : defaultValue;
        }
        
        /// <summary>
        /// This is using little endian.
        /// </summary>
        public long ReadInt64OrDefault(Slice key, long defaultValue = 0)
        {
            return TryRead(key, out var reader) ? reader.ReadLittleEndianInt64() : defaultValue;
        }
        
        public string ReadStringOrDefault(Slice key, string defaultValue = null)
        {
            return TryRead(key, out var reader) ? reader.ToStringValue() : defaultValue;
        }
        
        public bool Exists(Slice key)
        {
            AssertNotDisposed();

            using var rental = RentPage(out var leafInstance);
            var p = FindPageFor(key, out _, leafInstance: leafInstance);
            return p.LastMatch == 0;
        }

        public int GetDataSize(Slice key)
        {
            AssertNotDisposed();

            using var rental = RentPage(out var leafInstance);
            var p = FindPageFor(key, out TreeNodeHeader* node, leafInstance: leafInstance);

            if (p.LastMatch != 0)
                return -1;

            if (node == null)
                return -1;

            using (TreeNodeHeader.ToSlicePtr(_llt.Allocator, node, out Slice nodeKey))
            {
                if (SliceComparer.EqualsInline(nodeKey, key) == false)
                    return -1;
            }

            return GetDataSize(node);
        }

        public int GetDataSize(TreeNodeHeader* node)
        {
            AssertNotDisposed();

            if (node->Flags == (TreeNodeFlags.PageRef))
            {
                var overFlowPage = GetReadOnlyPage(node->PageNumber);
                return overFlowPage.OverflowSize;
            }
            return node->DataSize;
        }

        public void RemoveEmptyDecompressedPage(DecompressedLeafPage emptyPage)
        {
            AssertNotDisposed();

            using (emptyPage.Original.GetNodeKey(_llt, 0, out var key))
            {
                var p = FindPageFor(key, node: out _, allowCompressed: true);

                Debug.Assert(p.IsLeaf && p.IsCompressed && p.PageNumber == emptyPage.PageNumber);

                using var cursorRental = BuildCursor(key, p, out var cursor);
                var treeRebalancer = new TreeRebalancer(_llt, this, cursor);
                var changedPage = (TreePage)emptyPage;
                while (changedPage != null)
                {
                    changedPage = treeRebalancer.Execute(changedPage);
                }
            }
        }

        public long GetParentPageOf(TreePage page)
        {
            AssertNotDisposed();

            Debug.Assert(page.IsCompressed == false);

            using (page.IsLeaf ? page.GetNodeKey(_llt, 0, out Slice key) : page.GetNodeKey(_llt, 1, out key))
            {
                TreePage p = FindPageFor(key, node: out TreeNodeHeader* _, allowCompressed: true);

                if (page.IsLeaf)
                {
                    if (page.PageNumber != p.PageNumber)
                    {
                        VoronUnrecoverableErrorException.Raise(_tx.LowLevelTransaction,
                            $"Got different leaf page when looking for a parent of {page} using the key '{key}' from that page. Page {p} was found, last match: {p.LastMatch}.");
                    }
                    else if (p.LastMatch != 0)
                    {
                        if (p.IsCompressed == false)
                        {
                            // if a found page is compressed then we could not find the exact match because 
                            // the key we were looking for might belong to an compressed entry
                            // if the page isn't compressed then it's a corruption

                            VoronUnrecoverableErrorException.Raise(_tx.LowLevelTransaction,
                                $"Could not find a page containing '{key}' when looking for a parent of {page}. Page {p} was found, last match: {p.LastMatch}.");
                        }
#if DEBUG
                        using (var decompressed = DecompressPage(p, DecompressionUsage.Read, skipCache: true))
                        {
                            decompressed.Search(_llt, key);
                            Debug.Assert(decompressed.LastMatch == 0);
                        }
#endif
                    }
                }

                using var cursorRental = BuildCursor(key, p, out var cursor);
                while (cursor.PageCount > 0)
                {
                    if (cursor.CurrentPage.PageNumber == page.PageNumber)
                    {
                        if (cursor.PageCount == 1)
                            return -1; // root page

                        return cursor.ParentPage.PageNumber;
                    }
                    cursor.Pop();
                }
            }

            return -1;
        }

        internal byte* DirectRead(Slice key)
        {
            using var rental = RentPage(out var leafInstance);
            var p = FindPageFor(key, out TreeNodeHeader* node, leafInstance: leafInstance);

            if (p == null || p.LastMatch != 0)
                return null;

            Debug.Assert(node != null);

            if (node->Flags == TreeNodeFlags.PageRef)
            {
                var overFlowPage = GetReadOnlyTreePage(node->PageNumber);
                return overFlowPage.Base + Constants.Tree.PageHeaderSize;
            }

            return (byte*)node + node->KeySize + Constants.Tree.NodeHeaderSize;
        }

        public List<long> AllPages()
        {
            AssertNotDisposed();

            var results = new List<long>();
            var stack = new Stack<TreePage>();
            var root = GetReadOnlyTreePage(State.Header.RootPageNumber);
            stack.Push(root);

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
                        stack.Push(GetReadOnlyTreePage(pageNumber));
                    }
                    else if (node->Flags == TreeNodeFlags.PageRef)
                    {
                        // This is an overflow page
                        var overflowPage = GetReadOnlyTreePage(pageNumber);
                        var numberOfPages = VirtualPagerLegacyExtensions.GetNumberOfOverflowPages(overflowPage.OverflowSize);
                        for (long j = 0; j < numberOfPages; ++j)
                            results.Add(overflowPage.PageNumber + j);
                    }
                    else if (node->Flags == TreeNodeFlags.MultiValuePageRef)
                    {
                        using (TreeNodeHeader.ToSlicePtr(_tx.Allocator, node, out Slice key))
                        {
                            var tree = OpenMultiValueTree(key, node);
                            results.AddRange(tree.AllPages());
                        }
                    }
                    else
                    {
                        if (State.Header.RootObjectType == RootObjectType.Table) // tables might have mixed values, fixed size trees inside have dedicated handling
                            continue;
                        
                        if ((State.Header.Flags & TreeFlags.FixedSizeTrees) == TreeFlags.FixedSizeTrees ||
                            (State.Header.Flags & TreeFlags.Streams) == TreeFlags.Streams)
                        {
                            var valueReader = GetValueReaderFromHeader(node);

                            // Check if this is an inline stream - no extra pages to collect
                            if (valueReader.Length >= 1 && ((RootObjectType*)valueReader.Base)[0] == RootObjectType.InlineStream)
                                continue;

                            var valueSize = ((FixedSizeTreeHeader.Embedded*)valueReader.Base)->ValueSize;

                            using (p.GetNodeKey(_llt, i, out Slice fixedSizeTreeName))
                            {
                                FixedSizeTree fixedSizeTree;

                                try
                                {
                                    fixedSizeTree = new FixedSizeTree(_llt, this, fixedSizeTreeName, valueSize);

                                    var pages = fixedSizeTree.AllPages();
                                    results.AddRange(pages);
                                }
                                catch (InvalidFixedSizeTree)
                                {
                                    // ignored - we sometimes have trees with mixed types of values - regular values reside next to fixed size tree headers
                                    continue;
                                }

                                if ((State.Header.Flags & TreeFlags.Streams) == TreeFlags.Streams)
                                {
                                    Debug.Assert(fixedSizeTree.ValueSize == ChunkDetails.SizeOf);

                                    var streamPages = GetStreamPages(fixedSizeTree, GetStreamInfo(fixedSizeTree.Name, writable: false));

                                    results.AddRange(streamPages);
                                }
                            }
                        }
                    }
                }
            }
            return results;
        }

        public override string ToString()
        {
            return Name + " " + State.Header.NumberOfEntries;
        }

        internal void PrepareForCommit()
        {
            if (_prepareLocator == null) return;
            foreach (var ct in _prepareLocator.Values)
                ct.PrepareForCommit();
        }

        [Conditional("DEBUG")]
        internal void AssertNotDisposed()
        {
            if (_llt.IsDisposed)
                throw new InvalidOperationException("Cannot call this operation on a disposed instance.");
        }

        private bool TryOverwriteOverflowPages(TreeNodeHeader* updatedNode, int len, out byte* pos)
        {
            if (updatedNode->Flags == TreeNodeFlags.PageRef)
            {
                var readOnlyOverflowPage = GetReadOnlyTreePage(updatedNode->PageNumber);

                var availableOverflows = VirtualPagerLegacyExtensions.GetNumberOfOverflowPages(readOnlyOverflowPage.OverflowSize);

                if (len <= (availableOverflows * Constants.Storage.PageSize - Constants.Tree.PageHeaderSize))
                {
                    var requestedOverflows = VirtualPagerLegacyExtensions.GetNumberOfOverflowPages(len);

                    var overflowsToFree = availableOverflows - requestedOverflows;

                    for (int i = 0; i < overflowsToFree; i++)
                    {
                        _llt.FreePage(readOnlyOverflowPage.PageNumber + requestedOverflows + i);
                    }

                    _llt.DiscardScratchModificationOn(readOnlyOverflowPage.PageNumber);

                    RecordFreedPage(readOnlyOverflowPage, overflowsToFree);

                    var page = _llt.AllocatePage(requestedOverflows, updatedNode->PageNumber);
                    var writtableOverflowPage = PrepareTreePage(TreePageFlags.Value, requestedOverflows, page);

                    writtableOverflowPage.Flags = PageFlags.Overflow | PageFlags.VariableSizeTreePage;
                    writtableOverflowPage.OverflowSize = len;
                    pos = writtableOverflowPage.Base + Constants.Tree.PageHeaderSize;

                    PageModified?.Invoke(writtableOverflowPage.PageNumber, writtableOverflowPage.Flags);

                    return true;
                }
            }
            pos = null;
            return false;
        }

        public void ClearPagesCache()
        {
            if (_recentlyFoundPages != null)
                _recentlyFoundPages->Clear();
        }

        public CompactTree CompactTreeFor(string key)
        {
            using var _ = Slice.From(_llt.Allocator, key, ByteStringType.Immutable, out var keySlice);
            return CompactTreeFor(keySlice);
        }
        
        // RavenDB-21678: If you're keeping a reference to CompactTree, it may happen that you can actually override the prepareLocator and 
        // have more than one object of the same tree in memory but with different states. It's dangerous to mix usage since the object retains states.
        public CompactTree CompactTreeFor(Slice key)
        {
            // RavenDB-21678: Indexes with dynamic fields can generate a lot of fields, which can lead to overflow in the locator.
            // We've observed indexes with more than ~150 fields, and since performance is crucial, we increased it by one order of magnitude.
            if (_prepareLocator == null)
            {
                _prepareLocator = new SliceSmallSet<IPrepareForCommit>(4096);
                _llt.RegisterDisposable(_prepareLocator);
            }

            if (_prepareLocator.TryGetValue(key, out var prep) == false)
            {
                var compactTree = CompactTree.InternalCreate(this, key);
                if (compactTree == null) // missing value on read transaction
                    return null;

                var keyClone = key.Clone(_llt.Allocator);
                _prepareLocator.Add(keyClone, compactTree);
                prep = compactTree;
            }

            Debug.Assert(State.Header.Flags.HasFlag(TreeFlags.CompactTrees));

            return (CompactTree)prep;
        }


        public Lookup<TKey> LookupFor<TKey>(Slice key)
            where TKey : struct, ILookupKey
        {
            if (_prepareLocator == null)
            {
                _prepareLocator = new SliceSmallSet<IPrepareForCommit>(128);
                _llt.RegisterDisposable(_prepareLocator);
            }

            if (_prepareLocator.TryGetValue(key, out var prep) == false)
            {
                var lookup = Lookup<TKey>.InternalCreate(this, key);
                if (lookup == null) // missing value on read transaction
                    return null;

                var keyClone = key.Clone(_llt.Allocator);
                _prepareLocator.Add(keyClone, lookup);
                prep = lookup;
            }

            Debug.Assert(State.Header.Flags.HasFlag(TreeFlags.Lookups));

            return (Lookup<TKey>)prep;
        }
        
        public bool TryGetLookupFor<TKey>(Slice key, out Lookup<TKey> lookup)
            where TKey : struct, ILookupKey
        {
            if (_prepareLocator == null)
            {
                _prepareLocator = new SliceSmallSet<IPrepareForCommit>(128);
                _llt.RegisterDisposable(_prepareLocator);
            }

            if (_prepareLocator.TryGetValue(key, out var prep) == false)
            {
                lookup = Lookup<TKey>.InternalCreate(this, key);
                if (lookup == null)
                    return false;
                
                var keyClone = key.Clone(_llt.Allocator);
                _prepareLocator.Add(keyClone, lookup);
                prep = lookup;
            }

            Debug.Assert(_state.Header.Flags.HasFlag(TreeFlags.Lookups));

            lookup = (Lookup<TKey>)prep;
            return true;
        }

        public FixedSizeTree FixedTreeFor(Slice key, byte valSize = 0)
        {
            _fixedSizeTrees ??= new Dictionary<Slice, FixedSizeTree>(SliceComparer.Instance);

            if (_fixedSizeTrees.TryGetValue(key, out var fixedTree) == false)
            {
                fixedTree = new FixedSizeTree(_llt, this, key, valSize);

                if (_llt.Flags is TransactionFlags.ReadWrite && (State.Header.Flags & TreeFlags.FixedSizeTrees) != TreeFlags.FixedSizeTrees)
                {
                    ref var state = ref State.Modify();
                    state.Flags |= TreeFlags.FixedSizeTrees;
                }

                _fixedSizeTrees[fixedTree.Name] = fixedTree;
            }

            // RavenDB-22261: It may happen that the FixedSizeTree requested does not exist, and if it does not
            // it would still return an instance. This is a workaround because the check in debug is correct.
            // https://issues.hibernatingrhinos.com/issue/RavenDB-22261/Inconsistency-in-Tree-external-API
            Debug.Assert(fixedTree.NumberOfEntries == 0 || State.Header.Flags.HasFlag(TreeFlags.FixedSizeTrees));

            return fixedTree;
        }

        public FixedSizeTree<double> FixedTreeForDouble(Slice key, byte valSize = 0)
        {
            _fixedSizeTreesForDouble ??= new Dictionary<Slice, FixedSizeTree<double>>(SliceComparer.Instance);

            if (_fixedSizeTreesForDouble.TryGetValue(key, out var fixedTree) == false)
            {
                fixedTree = new FixedSizeTree<double>(_llt, this, key, valSize);
                _fixedSizeTreesForDouble[fixedTree.Name] = fixedTree;
            }

            // RavenDB-22261: It may happen that the FixedSizeTree requested does not exist, and if it does not
            // it would still return an instance. This is a workaround because the check in debug is correct.
            // https://issues.hibernatingrhinos.com/issue/RavenDB-22261/Inconsistency-in-Tree-external-API
            Debug.Assert(fixedTree.NumberOfEntries == 0 || State.Header.Flags.HasFlag(TreeFlags.FixedSizeTrees));


            return fixedTree;
        }

        public long DeleteFixedTreeFor(Slice key, byte valSize = 0)
        {
            var fixedSizeTree = FixedTreeFor(key, valSize);
            var numberOfEntries = fixedSizeTree.NumberOfEntries;

            foreach (var page in fixedSizeTree.AllPages())
            {
                if (_newPageAllocator != null)
                {
                    if (IsIndexTree == false)
                        ThrowAttemptToFreePageToNewPageAllocator(Name, page);

                    _newPageAllocator.FreePage(page);
                }
                else
                {
                    if (IsIndexTree)
                        ThrowAttemptToFreeIndexPageToFreeSpaceHandling(Name, page);

                    _llt.FreePage(page);
                }
            }
            _fixedSizeTrees.Remove(key);
            _fixedSizeTreesForDouble?.Remove(key);
            Delete(key);

            return numberOfEntries;
        }

        [Conditional("DEBUG")]
        public void DebugRenderAndShow()
        {
            DebugStuff.RenderAndShow(this);
        }

        public byte* DirectAccessFromHeader(TreeNodeHeader* node)
        {
            if (node->Flags == TreeNodeFlags.PageRef)
            {
                var overFlowPage = GetReadOnlyTreePage(node->PageNumber);
                return overFlowPage.Base + Constants.Tree.PageHeaderSize;
            }

            return (byte*)node + node->KeySize + Constants.Tree.NodeHeaderSize;
        }

        public Slice GetData(TreeNodeHeader* node)
        {
            Slice outputDataSlice;

            if (node->Flags == TreeNodeFlags.PageRef)
            {
                var overFlowPage = GetReadOnlyPage(node->PageNumber);
                if (overFlowPage.OverflowSize > ushort.MaxValue)
                    throw new InvalidOperationException("Cannot convert big data to a slice, too big");
                Slice.External(Llt.Allocator, overFlowPage.Pointer + Constants.Tree.PageHeaderSize,
                    (ushort)overFlowPage.OverflowSize, out outputDataSlice);
            }
            else
            {
                Slice.External(Llt.Allocator, (byte*)node + node->KeySize + Constants.Tree.NodeHeaderSize,
                    (ushort)node->DataSize, out outputDataSlice);
            }

            return outputDataSlice;
        }

        public ValueReader GetValueReaderFromHeader(TreeNodeHeader* node)
        {
            if (node->Flags == TreeNodeFlags.PageRef)
            {
                var overFlowPage = GetReadOnlyPage(node->PageNumber);

                Debug.Assert(overFlowPage.IsOverflow, "Requested overflow page but got " + overFlowPage.Flags);
                Debug.Assert(overFlowPage.OverflowSize > 0, "Overflow page cannot be size equal 0 bytes");

                return new ValueReader(overFlowPage.Pointer + Constants.Tree.PageHeaderSize, overFlowPage.OverflowSize);
            }
            return new ValueReader((byte*)node + node->KeySize + Constants.Tree.NodeHeaderSize, node->DataSize);
        }

        public void Rename(Slice newName)
        {
            Name = newName;

            // This is a special case, since we are renaming the tree we need the transaction to consider it as changed
            // even though we are not going to be modifying the state per se. 
            State.Modify();
        }

        // Not Rename: MultiRead registers a multi-tree in read transactions too, where State.Modify() throws.
        internal void SetNameOwnedByTransaction(Slice name)
        {
            Name = name;
        }

        internal void SetNewPageAllocator(NewPageAllocator newPageAllocator)
        {
            Debug.Assert(newPageAllocator != null);

            _newPageAllocator = newPageAllocator;
            HasNewPageAllocator = true;
        }

        internal void DebugValidateBranchReferences()
        {
            var rootPageNumber = State.Header.RootPageNumber;

            var pages = new HashSet<long>();
            var stack = new Stack<TreePage>();
            var root = GetReadOnlyTreePage(rootPageNumber);
            stack.Push(root);
            pages.Add(rootPageNumber);

            while (stack.Count > 0)
            {
                var p = stack.Pop();

                if (p.IsBranch == false)
                    continue;

                if (p.NumberOfEntries < 2)
                {
                    throw new InvalidOperationException("The branch page " + p.PageNumber + " has " +
                                                        p.NumberOfEntries + " entry");
                }

                for (int i = 0; i < p.NumberOfEntries; i++)
                {
                    var page = p.GetNode(i)->PageNumber;

                    if (pages.Add(page) == false)
                    {
                        DebugStuff.RenderAndShow(this);
                        throw new InvalidOperationException("The page " + page + " already appeared in the tree!");
                    }

                    var refPage = GetReadOnlyTreePage(page);

                    using (p.GetNodeKey(_llt, i, out var referenceKey))
                    {
                        Validate(refPage, referenceKey);

                        if (refPage.IsCompressed)
                        {
                            using (var decompressedRefPage = DecompressPage(refPage, DecompressionUsage.Read, skipCache: true))
                            {
                                Validate(decompressedRefPage, referenceKey);
                            }
                        }
                    }

                    if (refPage.IsBranch == false)
                        continue;

                    stack.Push(refPage);

                    void Validate(TreePage pageRef, Slice refKey)
                    {
                        if (refKey.Options == SliceOptions.Key)
                        {
                            for (int j = 0; j < pageRef.NumberOfEntries; j++)
                            {
                                using (pageRef.GetNodeKey(_llt, j, out var key))
                                {
                                    if (key is { HasValue: true, Size: > 0 } && SliceComparer.Compare(key, refKey) < 0)
                                    {
                                        DebugStuff.RenderAndShow(this);
                                        throw new InvalidOperationException($"Found invalid reference in branch page: {p}. Reference key: {refKey}, key found in referenced {pageRef} page: {key}");
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        public void Forget(Slice name)
        {
            _prepareLocator?.Remove(name);
        }
    }
}

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
using Sparrow.Collections;
using Sparrow.Server;
using Voron.Data.Lookups;
using Constants = Voron.Global.Constants;
using System.Diagnostics.CodeAnalysis;

using static Sparrow.DisposableExceptions;
using static Sparrow.PortableExceptions;
using static Voron.VoronExceptions;

using CompactTree = Voron.Data.CompactTrees.CompactTree;
using FixedSizeTree = Voron.Data.Fixed.FixedSizeTree;

namespace Voron.Data.BTrees
{
    public unsafe partial class Tree 
    {
        private int _directAddUsage;

        private static readonly ObjectPool<RecentlyFoundTreePages> FoundPagesPool = new(() => new RecentlyFoundTreePages(), 128);

        private RecentlyFoundTreePages _recentlyFoundPages;

        private Dictionary<Slice, FixedSizeTree> _fixedSizeTrees;

        private SliceSmallSet<IPrepareForCommit> _prepareLocator;

        public event Action<long, PageFlags> PageModified;
        public event Action<long, PageFlags> PageFreed;

        public Slice Name { get; private set; }

        private TreeRootHeader _header;
        public bool HeaderModified;

        public ref readonly TreeRootHeader ReadHeader()
        {
            return ref _header;
        }
        public ref TreeRootHeader ModifyHeader()
        {
            HeaderModified = true;
            return ref _header;
        }
        

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

            _recentlyFoundPages = FoundPagesPool.Allocate();

            _header = header;

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
                ThrowIfNullOnDebug(isIndexTree, "If newPageAllocator is set, we must be in a isIndexTree = true");
                SetNewPageAllocator(newPageAllocator);
            }

            _recentlyFoundPages = FoundPagesPool.Allocate();
            
            llt.RegisterDisposable(new TreeDisposable(this));
        }

        private Tree(LowLevelTransaction llt, Slice name, in TreeRootHeader header)
        {
            _llt = llt;
            Name = name;

            _recentlyFoundPages = FoundPagesPool.Allocate();
            _header = header;

            llt.RegisterDisposable(new TreeDisposable(this));
        }

        private class TreeDisposable(Tree tree) : IDisposable
        {
            void IDisposable.Dispose()
            {
                tree._recentlyFoundPages.Clear();
                FoundPagesPool.Free(tree._recentlyFoundPages);
                tree._recentlyFoundPages = null;
            }
        }

        public bool IsLeafCompressionSupported
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get { return (_header.Flags & TreeFlags.LeafsCompressed) == TreeFlags.LeafsCompressed; }
        }

        public bool HasNewPageAllocator { get; private set; }

        public static Tree GetRoot(LowLevelTransaction llt, Slice name, in TreeRootHeader parentHeader)
        {
            return new Tree(llt, name, in parentHeader);
        }
        
        public static Tree Open(LowLevelTransaction llt, Transaction tx, Slice name, in TreeRootHeader header, bool isIndexTree = false, NewPageAllocator newPageAllocator = null)
        {
            var tree = new Tree(llt, tx, header, name, isIndexTree, newPageAllocator);

            if ((tree._header.Flags & TreeFlags.LeafsCompressed) == TreeFlags.LeafsCompressed)
                tree.InitializeCompression();

            return tree;
        }

        public static Tree Create(LowLevelTransaction llt, Transaction tx, Slice name, TreeFlags flags = TreeFlags.None, RootObjectType type = RootObjectType.VariableSizeTree,
            bool isIndexTree = false, NewPageAllocator newPageAllocator = null)
        {
            ThrowIfReadOnly(llt);
            ThrowIf<ArgumentException>(type != RootObjectType.VariableSizeTree && type != RootObjectType.Table,
                $"Only valid types are {nameof(RootObjectType.VariableSizeTree)} or {nameof(RootObjectType.Table)}.");

            var newPage = newPageAllocator?.AllocateSinglePage(0) ?? llt.AllocatePage(1);

            TreePage newRootPage = PrepareTreePage(TreePageFlags.Leaf, 1, newPage);

            var tree = new Tree(llt, tx, name, isIndexTree, newPageAllocator);

            ref var state = ref tree.ModifyHeader();
            state.RootPageNumber = newRootPage.PageNumber;
            state.RootObjectType = type;
            state.Depth = 1;
            state.Flags = flags;

            if ((flags & TreeFlags.LeafsCompressed) == TreeFlags.LeafsCompressed)
                tree.InitializeCompression();

            tree.RecordNewPage(newRootPage, 1);
            return tree;
        }

        private void RecordNewPage(TreePage p, int num)
        {
            ref var header = ref ModifyHeader();

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

        private void RecordFreedPage(TreePage p, int num)
        {
            ref var header = ref ModifyHeader();

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
            Debug.Assert((_header.Flags & TreeFlags.MultiValue) == TreeFlags.None,"(State.Flags & TreeFlags.MultiValue) == TreeFlags.None");
            
            long currentValue = 0;

            if (TryRead(key, out var reader))
                currentValue = reader.Read<long>();

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
            Debug.Assert((_header.Flags & TreeFlags.MultiValue) == TreeFlags.None,"(State.Flags & TreeFlags.MultiValue) == TreeFlags.None");
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
            ThrowIfNull(value);
            ThrowIf<ArgumentException>(value.Length > int.MaxValue, "Cannot add a value that is over 2GB in size");

            var length = (int)value.Length;

            using (DirectAdd(key, length, out byte* ptr))
            {
                value.ReadExactly(new Span<byte>(ptr, length));
            }
        }

        public void Add(Slice key, ReadOnlySpan<byte> value)
        {
            using (DirectAdd(key, value.Length, out byte* ptr))
                value.CopyTo(new Span<byte>(ptr, value.Length));
        }

        public void Add(Slice key, Slice value)
        {
            VoronExceptions.ThrowIfNull(value);
            
            Debug.Assert((ReadHeader().Flags & TreeFlags.MultiValue) == TreeFlags.None,"(State.Flags & TreeFlags.MultiValue) == TreeFlags.None");

            if (!value.HasValue)
                throw new NullReferenceException();

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

        public DirectAddScope DirectAdd(Slice key, int len, TreeNodeFlags nodeType, out byte* ptr)
        {
            ThrowIfDisposedOnDebug(_llt);
            ThrowIfReadOnly(_llt);

            if (key.Size > Constants.Tree.MaxKeySize)
                ThrowInvalidKeySize(key);

            var foundPage = FindPageFor(key, node: out TreeNodeHeader* node, cursor: out TreeCursorConstructor cursorConstructor, allowCompressed: true);
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
                ref var state = ref ModifyHeader();
                state.NumberOfEntries++;
            }
            
            nodeType &= ~TreeNodeFlags.NewOnly;
            
            ThrowIfOnDebug<InvalidOperationException>(nodeType != TreeNodeFlags.Data && nodeType != TreeNodeFlags.MultiValuePageRef,
                $"Node should be either {nameof(TreeNodeFlags.Data)} or {nameof(TreeNodeFlags.MultiValuePageRef)}");

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
                if (IsLeafCompressionSupported == false || TryCompressPageNodes(key, len, page) == false)
                {
                    using (var cursor = cursorConstructor.Build(key))
                    {
                        cursor.Update(cursor.Pages, page);

                        var pageSplitter = new TreePageSplitter(_llt, this, key, len, pageNumber, nodeType, cursor);
                        dataPos = pageSplitter.Execute();
                    }

                    DebugValidateTree(_header.RootPageNumber);

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

            page.DebugValidate(this, _header.RootPageNumber);

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

            }

            public void Dispose()
            {
                _parent._directAddUsage--;
            }


            [DoesNotReturn]
            private void ThrowScopeAlreadyOpen()
            {
                var message = $"Write operation already requested on a tree name: {_parent}. " +
                              $"{nameof(DirectAdd)} method cannot be called recursively while the scope is already opened.";


                throw new InvalidOperationException(message);
            }

        }

        [DoesNotReturn]
        private static void ThrowUnknownNodeTypeAddOperation(TreeNodeFlags nodeType)
        {
            throw new NotSupportedException("Unknown node type for direct add operation: " + nodeType);
        }

        [DoesNotReturn]
        private static void ThrowInvalidKeySize(Slice key)
        {
            throw new ArgumentException(
                $"Key size is too big, must be at most {Constants.Tree.MaxKeySize} bytes, but was {(key.Size + Constants.Tree.RequiredSpaceForNewNode)}",
                nameof(key));
        }

        public TreePage ModifyPage(TreePage page)
        {
            ThrowIfDisposedOnDebug(_llt);

            if (page.Dirty)
                return page;

            var newPage = ModifyPage(page.PageNumber);
            newPage.LastSearchPosition = page.LastSearchPosition;
            newPage.LastMatch = page.LastMatch;

            return newPage;
        }

        public TreePage ModifyPage(long pageNumber)
        {
            ThrowIfDisposedOnDebug(_llt);

            var newPage = GetWriteableTreePage(pageNumber);
            newPage.Dirty = true;
            _recentlyFoundPages?.Reset(pageNumber);

            if (IsLeafCompressionSupported && newPage.IsCompressed)
                DecompressionsCache.Invalidate(pageNumber, DecompressionUsage.Read);

            PageModified?.Invoke(pageNumber, newPage.Flags);

            return newPage;
        }

        public bool ShouldGoToOverflowPage(int len)
        {
            return len + Constants.Tree.NodeHeaderSize > Constants.Tree.NodeMaxSize;
        }

        private long WriteToOverflowPages(int overflowSize, out byte* dataPos)
        {
            var numberOfPages = Paging.GetNumberOfOverflowPages(overflowSize);
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
        internal TreePage GetReadOnlyTreePage(long pageNumber)
        {
            var page = _llt.GetPage(pageNumber);
            return new TreePage(page.Pointer, Constants.Storage.PageSize);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal Page GetReadOnlyPage(long pageNumber)
        {
            return _llt.GetPage(pageNumber);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal TreePage GetWriteableTreePage(long pageNumber)
        {
            var page = _llt.ModifyPage(pageNumber);
            return new TreePage(page.Pointer, Constants.Storage.PageSize);
        }

        internal TreePage FindPageFor(Slice key, out TreeNodeHeader* node)
        {
            if (TryUseRecentTransactionPage(key, out TreePage p, out node))
            {
                return p;
            }

            return SearchForPage(key, out node);
        }

        internal TreePage FindPageFor(Slice key, out TreeNodeHeader* node, out TreeCursorConstructor cursor, bool allowCompressed = false)
        {
            if (TryUseRecentTransactionPage(key, out cursor, out TreePage p, out node))
            {
                if (allowCompressed == false && p.IsCompressed)
                    ThrowOnCompressedPage(p);

                return p;
            }

            return SearchForPage(key, allowCompressed, out cursor, out node);
        }

        [ThreadStatic]
        private static FastList<long> CursorPathBuffer;

        private TreePage SearchForPage(Slice key, out TreeNodeHeader* node)
        {
            var p = GetReadOnlyTreePage(_header.RootPageNumber);

            if (CursorPathBuffer == null)
                CursorPathBuffer = new FastList<long>();
            else
                CursorPathBuffer.Clear();

            CursorPathBuffer.Add(p.PageNumber);

            bool rightmostPage = true;
            bool leftmostPage = true;

            while ((p.TreeFlags & TreePageFlags.Branch) == TreePageFlags.Branch)
            {
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

                var pageNode = p.GetNode(nodePos);
                p = GetReadOnlyTreePage(pageNode->PageNumber);
                Debug.Assert(pageNode->PageNumber == p.PageNumber, $"Requested Page: #{pageNode->PageNumber}. Got Page: #{p.PageNumber}");

                CursorPathBuffer.Add(p.PageNumber);
            }

            if (p.IsLeaf == false)
                VoronUnrecoverableErrorException.Raise(_llt, "Index points to a non leaf page " + p.PageNumber);

            if (p.IsCompressed)
                ThrowOnCompressedPage(p);

            node = p.Search(_llt, key); // will set the LastSearchPosition

            AddToRecentlyFoundPages(CursorPathBuffer, p, leftmostPage, rightmostPage);

            return p;
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

        private TreePage SearchForPage(Slice key, bool allowCompressed, out TreeCursorConstructor cursorConstructor, out TreeNodeHeader* node, bool addToRecentlyFoundPages = true)
        {
            var p = GetReadOnlyTreePage(_header.RootPageNumber);

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
                    nodePos = SetLastSearchPosition(key, p, ref leftmostPage, ref rightmostPage);
                }

                var pageNode = p.GetNode(nodePos);
                p = GetReadOnlyTreePage(pageNode->PageNumber);
                Debug.Assert(pageNode->PageNumber == p.PageNumber, $"Requested Page: #{pageNode->PageNumber}. Got Page: #{p.PageNumber}");

                cursor.Push(p);
            }

            cursorConstructor = new TreeCursorConstructor(cursor);

            if (p.IsLeaf == false)
                VoronUnrecoverableErrorException.Raise(_llt, "Index points to a non leaf page");

            if (allowCompressed == false && p.IsCompressed)
                ThrowOnCompressedPage(p);

            node = p.Search(_llt, key); // will set the LastSearchPosition

            if (p.NumberOfEntries > 0 && addToRecentlyFoundPages) // compressed page can have no ordinary entries
                AddToRecentlyFoundPages(cursor, p, leftmostPage, rightmostPage);

            return p;
        }

        [DoesNotReturn]
        private static void ThrowOnCompressedPage(TreePage p)
        {
            throw new PageCompressedException($"Page {p} is compressed. You need to decompress it to be able to access its content.");
        }

        private void AddToRecentlyFoundPages(FastList<long> c, TreePage p, bool leftmostPage, bool rightmostPage)
        {
            if (_recentlyFoundPages == null)
                return;

            Debug.Assert(p.IsCompressed == false);

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

            _recentlyFoundPages.Add(p, firstKeyOption, firstKey, lastKeyOption, lastKey, c.AsUnsafeSpan());
        }

        [SkipLocalsInit]
        private void AddToRecentlyFoundPages(TreeCursor c, TreePage p, bool leftmostPage, bool rightmostPage)
        {
            if (_recentlyFoundPages == null)
                return;

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

            Span<long> cursorPath = stackalloc long[c.Pages.Count];
            int pos = cursorPath.Length - 1;
            foreach (var page in c.Pages)
                cursorPath[pos--] = page.PageNumber;

            _recentlyFoundPages.Add(p, firstKeyOption, firstKey, lastKeyOption, lastKey, cursorPath);
        }

        private bool TryUseRecentTransactionPage(Slice key, out TreePage page, out TreeNodeHeader* node)
        {
            if (_recentlyFoundPages == null || _recentlyFoundPages.TryFind(key, out var foundPage) == false)
            {
                page = null;
                node = null;
                return false;
            }

            if (foundPage.Page != null)
            {
                // we can't share the same instance, Page instance may be modified by
                // concurrently run iterators
                page = new TreePage(foundPage.Page.Base, foundPage.Page.PageSize);
            }
            else
            {
                page = GetReadOnlyTreePage(foundPage.Number);
            }

            if (page.IsLeaf == false)
                VoronUnrecoverableErrorException.Raise(_llt, "Index points to a non leaf page");

            node = page.Search(_llt, key); // will set the LastSearchPosition

            return true;
        }

        private bool TryUseRecentTransactionPage(Slice key, out TreeCursorConstructor cursor, out TreePage page, out TreeNodeHeader* node)
        {
            if (_recentlyFoundPages == null || _recentlyFoundPages.TryFind(key, out var foundPage) == false)
            {
                page = null;
                node = null;
                cursor = default;
                return false;
            }

            var lastFoundPageNumber = foundPage.Number;

            if (foundPage.Page != null)
            {
                // we can't share the same instance, Page instance may be modified by
                // concurrently run iterators
                page = new TreePage(foundPage.Page.Base, foundPage.Page.PageSize);
            }
            else
            {
                page = GetReadOnlyTreePage(lastFoundPageNumber);
            }

            if (page.IsLeaf == false)
                VoronUnrecoverableErrorException.Raise(_llt, "Index points to a non leaf page");

            node = page.Search(_llt, key); // will set the LastSearchPosition

            cursor = new TreeCursorConstructor(_llt, this, page, foundPage.Cursor.ToArray(), lastFoundPageNumber);
            return true;
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
            PageFreed?.Invoke(p.PageNumber, p.Flags);

            if (p.IsOverflow)
            {
                var numberOfPages = Paging.GetNumberOfOverflowPages(p.OverflowSize);
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
            ThrowIfDisposedOnDebug(_llt);
            ThrowIfReadOnly(_llt, "Cannot delete a value in a read only transaction");

            var page = FindPageFor(key, node: out TreeNodeHeader* _, cursor: out var cursorConstructor, allowCompressed: true);
            if (page.IsCompressed)
            {
                DeleteOnCompressedPage(page, key, ref cursorConstructor);
                return;
            }

            if (page.LastMatch != 0)
                return; // not an exact match, can't delete

            page = ModifyPage(page);

            ref var state = ref ModifyHeader();
            state.NumberOfEntries--;

            RemoveLeafNode(page);

            using (var cursor = cursorConstructor.Build(key))
            {
                var treeRebalancer = new TreeRebalancer(_llt, this, cursor);
                var changedPage = page;
                while (changedPage != null)
                {
                    changedPage = treeRebalancer.Execute(changedPage);
                }
            }

            page.DebugValidate(this, _header.RootPageNumber);
        }

        public TreeIterator Iterate(bool prefetch)
        {
            ThrowIfDisposedOnDebug(_llt);

            return new TreeIterator(this, _llt, prefetch);
        }

        public ReadResult Read(Slice key)
        {
            ThrowIfDisposedOnDebug(_llt);

            var p = FindPageFor(key, out TreeNodeHeader* node);

            if (p.LastMatch != 0)
                return null;

            return new ReadResult(GetValueReaderFromHeader(node));
        }

        public bool TryRead(Slice key, out ValueReader reader)
        {
            ThrowIfDisposedOnDebug(_llt);

            var p = FindPageFor(key, out TreeNodeHeader* node);

            if (p.LastMatch != 0)
            {
                Unsafe.SkipInit(out reader);
                return false;
            }

            reader = GetValueReaderFromHeader(node);
            return true;
        }

        public bool Exists(Slice key)
        {
            ThrowIfDisposedOnDebug(_llt);

            var p = FindPageFor(key, out _);
            return p.LastMatch == 0;
        }

        public int GetDataSize(Slice key)
        {
            ThrowIfDisposedOnDebug(_llt);

            var p = FindPageFor(key, out TreeNodeHeader* node);

            if (p.LastMatch != 0)
                return -1;

            if (node == null)
                return -1;

            using (TreeNodeHeader.ToSlicePtr(_llt.Allocator, node, out Slice nodeKey))
            {
                if (!SliceComparer.EqualsInline(nodeKey, key))
                    return -1;
            }

            return GetDataSize(node);
        }

        public int GetDataSize(TreeNodeHeader* node)
        {
            ThrowIfDisposedOnDebug(_llt);

            if (node->Flags == (TreeNodeFlags.PageRef))
            {
                var overFlowPage = GetReadOnlyPage(node->PageNumber);
                return overFlowPage.OverflowSize;
            }
            return node->DataSize;
        }

        public void RemoveEmptyDecompressedPage(DecompressedLeafPage emptyPage)
        {
            ThrowIfDisposedOnDebug(_llt);

            using (emptyPage.Original.GetNodeKey(_llt, 0, out var key))
            {
                var p = FindPageFor(key, node: out _, cursor: out var cursorConstructor, allowCompressed: true);

                Debug.Assert(p.IsLeaf && p.IsCompressed && p.PageNumber == emptyPage.PageNumber);

                using (var cursor = cursorConstructor.Build(key))
                {
                    var treeRebalancer = new TreeRebalancer(_llt, this, cursor);
                    var changedPage = (TreePage)emptyPage;
                    while (changedPage != null)
                    {
                        changedPage = treeRebalancer.Execute(changedPage);
                    }
                }
            }
        }

        public long GetParentPageOf(TreePage page)
        {
            ThrowIfDisposedOnDebug(_llt);

            Debug.Assert(page.IsCompressed == false);

            using (page.IsLeaf ? page.GetNodeKey(_llt, 0, out Slice key) : page.GetNodeKey(_llt, 1, out key))
            {
                TreePage p = FindPageFor(key, node: out TreeNodeHeader* _, cursor: out TreeCursorConstructor cursorConstructor, allowCompressed: true);

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

                using (var cursor = cursorConstructor.Build(key))
                {
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
            }

            return -1;
        }

        internal byte* DirectRead(Slice key)
        {
            var p = FindPageFor(key, out TreeNodeHeader* node);

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
            ThrowIfDisposedOnDebug(_llt);

            var results = new List<long>();
            var stack = new Stack<TreePage>();
            var root = GetReadOnlyTreePage(_header.RootPageNumber);
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
                        var numberOfPages = Paging.GetNumberOfOverflowPages(overflowPage.OverflowSize);
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
                        if (_header.RootObjectType == RootObjectType.Table) // tables might have mixed values, fixed size trees inside have dedicated handling
                            continue;
                        
                        if ((_header.Flags & TreeFlags.FixedSizeTrees) == TreeFlags.FixedSizeTrees)
                        {
                            var valueReader = GetValueReaderFromHeader(node);

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

                                if ((_header.Flags & TreeFlags.Streams) == TreeFlags.Streams)
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
            return Name + " " + _header.NumberOfEntries;
        }

        internal void PrepareForCommit()
        {
            if (_prepareLocator == null) return;
            foreach (var ct in _prepareLocator.Values)
                ct.PrepareForCommit();
        }

        private bool TryOverwriteOverflowPages(TreeNodeHeader* updatedNode, int len, out byte* pos)
        {
            if (updatedNode->Flags == TreeNodeFlags.PageRef)
            {
                var readOnlyOverflowPage = GetReadOnlyTreePage(updatedNode->PageNumber);

                var availableOverflows = Paging.GetNumberOfOverflowPages(readOnlyOverflowPage.OverflowSize);

                if (len <= (availableOverflows * Constants.Storage.PageSize - Constants.Tree.PageHeaderSize))
                {
                    var requestedOverflows = Paging.GetNumberOfOverflowPages(len);

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
            _recentlyFoundPages?.Clear();
        }

        public CompactTree CompactTreeFor(string key)
        {
            using var _ = Slice.From(_llt.Allocator, key, ByteStringType.Immutable, out var keySlice);
            return CompactTreeFor(keySlice);
        }

        // RavenDB-21678: If you're keeping a reference to CompactTree, it may happen that you can actually override the prepareLocator and 
        // have more than one object of the same tree in memory but with different states. It's dangerous to mix usage since the object retains states.
        public bool TryGetCompactTreeFor(Slice key, out CompactTree tree)
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
                tree = CompactTree.InternalCreate(this, key);
                if (tree == null) // missing value on read transaction
                    return false;

                var keyClone = key.Clone(_llt.Allocator);
                _prepareLocator.Add(keyClone, tree);
                prep = tree;
            }

            Debug.Assert(_header.Flags.HasFlag(TreeFlags.CompactTrees));

            tree = (CompactTree)prep;
            return true;
        }

        public CompactTree CompactTreeFor(Slice key)
        {
            if (TryGetCompactTreeFor(key, out var tree))
                return tree;

            throw new InvalidOperationException($"{nameof(CompactTree)} with key '{key}' does not exist.");
        }

        public Lookup<TKey> LookupFor<TKey>(Slice key)
            where TKey : struct, ILookupKey
        {
            if (TryGetLookupFor<TKey>(key, out var lookup))
                return lookup;

            throw new InvalidOperationException($"{nameof(Lookup<TKey>)} with key '{key}' does not exist.");
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

            Debug.Assert(_header.Flags.HasFlag(TreeFlags.Lookups));

            lookup = (Lookup<TKey>)prep;
            return true;
        }

        public FixedSizeTree FixedTreeFor(Slice key, byte valSize = 0)
        {
            _fixedSizeTrees ??= new Dictionary<Slice, FixedSizeTree>(SliceComparer.Instance);

            if (_fixedSizeTrees.TryGetValue(key, out var fixedTree) == false)
            {
                fixedTree = new FixedSizeTree(_llt, this, key, valSize);

                if (_llt.Flags is TransactionFlags.ReadWrite && (_header.Flags & TreeFlags.FixedSizeTrees) != TreeFlags.FixedSizeTrees)
                {
                    ref var state = ref ModifyHeader();
                    state.Flags |= TreeFlags.FixedSizeTrees;
                }

                _fixedSizeTrees[fixedTree.Name] = fixedTree;

                _llt.RegisterDisposable(fixedTree);
            }

            // RavenDB-22261: It may happen that the FixedSizeTree requested does not exist, and if it does not
            // it would still return an instance. This is a workaround because the check in debug is correct.
            // https://issues.hibernatingrhinos.com/issue/RavenDB-22261/Inconsistency-in-Tree-external-API
            Debug.Assert(fixedTree.NumberOfEntries == 0 || _header.Flags.HasFlag(TreeFlags.FixedSizeTrees));

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
            ModifyHeader();
        }

        internal void SetNewPageAllocator(NewPageAllocator newPageAllocator)
        {
            Debug.Assert(newPageAllocator != null);

            _newPageAllocator = newPageAllocator;
            HasNewPageAllocator = true;
        }

        internal void DebugValidateBranchReferences()
        {
            var rootPageNumber = _header.RootPageNumber;

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

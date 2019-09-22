using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Text;
using Sparrow;
using Sparrow.Server;
using Voron.Data.Compression;
using Voron.Debugging;
using Voron.Impl;
using Voron.Impl.Paging;
using Constants = Voron.Global.Constants;

namespace Voron.Data.BTrees
{
    public unsafe class TreePage
    {
        public readonly int PageSize;
        public byte* Base;

        public int LastMatch;
        public int LastSearchPosition;
        public bool Dirty;

#if VALIDATE
        public bool Freed;
#endif
        public TreePage(byte* basePtr, int pageSize)
        {
            Base = basePtr;
            PageSize = pageSize;
        }

        private TreePageHeader* Header
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get { return (TreePageHeader*)Base; }
        }

        public long PageNumber
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get { return Header->PageNumber; }
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            set { Header->PageNumber = value; }
        }

        public TreePageFlags TreeFlags
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get { return Header->TreeFlags; }
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            set { Header->TreeFlags = value; }
        }

        public ushort Lower
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get { return Header->Lower; }
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            set { Header->Lower = value; }
        }

        public ushort Upper
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get { return Header->Upper; }
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            set { Header->Upper = value; }
        }

        public int OverflowSize
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get { return Header->OverflowSize; }
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            set { Header->OverflowSize = value; }
        }

        public ushort* KeysOffsets
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get { return (ushort*)(Base + Constants.Tree.PageHeaderSize); }
        }


        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public TreeNodeHeader* Search(LowLevelTransaction tx, Slice key)
        {
            int numberOfEntries = NumberOfEntries;
            if (numberOfEntries == 0)
                goto NoEntries;

            int lastMatch = -1;
            int lastSearchPosition = 0;

            SliceOptions options = key.Options;
            if (options == SliceOptions.Key)
            {
                if (numberOfEntries == 1)
                    goto SingleEntryKey;

                int low = IsLeaf ? 0 : 1;
                int high = numberOfEntries - 1;
                int position = 0;

                ByteStringContext allocator = tx.Allocator;
                ushort* offsets = KeysOffsets;
                byte* @base = Base;
                while (low <= high)
                {
                    position = (low + high) >> 1;

                    var node = (TreeNodeHeader*)(@base + offsets[position]);

                    Slice pageKey;
                    using (TreeNodeHeader.ToSlicePtr(allocator, node, out pageKey))
                    {
                        lastMatch = SliceComparer.CompareInline(key, pageKey);
                    }

                    if (lastMatch == 0)
                        break;

                    if (lastMatch > 0)
                        low = position + 1;
                    else
                        high = position - 1;
                }

                if (lastMatch > 0) // found entry less than key
                {
                    position++; // move to the smallest entry larger than the key
                }

                Debug.Assert(position < ushort.MaxValue);
                lastSearchPosition = position;
                goto MultipleEntryKey;
            }
            if (options == SliceOptions.BeforeAllKeys)
            {
                lastMatch = 1;
                goto MultipleEntryKey;
            }
            if (options == SliceOptions.AfterAllKeys)
            {
                lastSearchPosition = numberOfEntries - 1;
                goto MultipleEntryKey;
            }

            ThrowNotSupportedException();         

            NoEntries:
            {
                LastSearchPosition = 0;
                LastMatch = 1;
                return null;
            }

            SingleEntryKey:
            {
                var node = GetNode(0);

                Slice pageKey;
                using (TreeNodeHeader.ToSlicePtr(tx.Allocator, node, out pageKey))
                {
                    LastMatch = SliceComparer.CompareInline(key, pageKey);
                }

                LastSearchPosition = LastMatch > 0 ? 1 : 0;
                return LastSearchPosition == 0 ? node : null;
            }

            MultipleEntryKey:
            {
                LastMatch = lastMatch;
                LastSearchPosition = lastSearchPosition;

                if (lastSearchPosition >= numberOfEntries)
                    return null;

                return GetNode(lastSearchPosition);
            }
        }

        private void ThrowNotSupportedException()
        {
            throw new NotSupportedException("This SliceOptions is not supported. Make sure you have updated this code when adding a new one.");
        }



        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public TreeNodeHeader* GetNode(int n)
        {
            Debug.Assert(n >= 0 && n < NumberOfEntries);

            return (TreeNodeHeader*)(Base + KeysOffsets[n]);
        }

        public bool IsLeaf
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get { return (Header->TreeFlags & TreePageFlags.Leaf) == TreePageFlags.Leaf; }
        }

        public bool IsBranch
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get { return (Header->TreeFlags & TreePageFlags.Branch) == TreePageFlags.Branch; }
        }

        public bool IsOverflow
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get{ return (Header->Flags & PageFlags.Overflow) == PageFlags.Overflow; }
        }

        public bool IsCompressed
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get { return (Header->Flags & PageFlags.Compressed) == PageFlags.Compressed; }
        }

        public CompressedNodesHeader* CompressionHeader
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get { return (CompressedNodesHeader*)(Base + PageSize - Constants.Compression.HeaderSize); }
        }

        public ushort NumberOfEntries
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get
            {
                // Because we store the keys offset from the end of the head to lower
                // we can calculate the number of entries by getting the size and dividing
                // in 2, since that is the size of the offsets we use

                return (ushort)((Header->Lower - Constants.Tree.PageHeaderSize) >> 1);
            }
        }

        public void RemoveNode(int index)
        {
            Debug.Assert(index >= 0 || index < NumberOfEntries);

            var node = GetNode(index);
            Memory.Set((byte*) node, 0, node->GetNodeSize() - Constants.Tree.NodeOffsetSize);

            ushort* offsets = KeysOffsets;
            int numberOfEntries = NumberOfEntries;
            for (int i = index + 1; i < numberOfEntries; i++)
            {
                offsets[i - 1] = offsets[i];
            }

            Lower -= (ushort)Constants.Tree.NodeOffsetSize;
        }

        public byte* AddPageRefNode(int index, Slice key, long pageNumber)
        {
            var node = CreateNode(index, key, TreeNodeFlags.PageRef, -1);
            node->PageNumber = pageNumber;

            return null; // nothing to write into page ref node
        }

        public byte* AddDataNode(int index, Slice key, int dataSize)
        {
            Debug.Assert(dataSize >= 0);
            Debug.Assert(key.Options == SliceOptions.Key);

            var node = CreateNode(index, key, TreeNodeFlags.Data, dataSize);
            node->DataSize = dataSize;

            return (byte*)node + Constants.Tree.NodeHeaderSize + key.Size;
        }

        public byte* AddMultiValueNode(int index, Slice key, int dataSize)
        {
            Debug.Assert(dataSize == sizeof(TreeRootHeader));
            Debug.Assert(key.Options == SliceOptions.Key);

            var node = CreateNode(index, key, TreeNodeFlags.MultiValuePageRef, dataSize);
            node->DataSize = dataSize;

            return (byte*)node + Constants.Tree.NodeHeaderSize + key.Size;
        }

        public void AddCompressionTombstoneNode(int index, Slice key)
        {
            var node = CreateNode(index, key, TreeNodeFlags.CompressionTombstone, 0);
            node->PageNumber = 0;
        }

        public void ChangeImplicitRefPageNode(long implicitRefPageNumber)
        {
            const int implicitRefIndex = 0;

            var node = GetNode(implicitRefIndex);

            node->KeySize = 0;
            node->Flags = TreeNodeFlags.PageRef;
            node->PageNumber = implicitRefPageNumber;
        }

        private TreeNodeHeader* CreateNode(int index, Slice key, TreeNodeFlags flags, int len)
        {
            Debug.Assert(index <= NumberOfEntries && index >= 0);
            Debug.Assert(IsBranch == false || index != 0 || key.Size == 0);// branch page's first item must be the implicit ref
            if (HasSpaceFor(key, len) == false)
                throw new InvalidOperationException(string.Format("The page is full and cannot add an entry, this is probably a bug. Key: {0}, data length: {1}, size left: {2}", key, len, SizeLeft));

            // move higher pointers up one slot
            ushort* offsets = KeysOffsets;
            for (int i = NumberOfEntries; i > index; i--)
            {
                offsets[i] = offsets[i - 1];
            }

            var nodeSize = TreeSizeOf.NodeEntry(PageMaxSpace, key, len);
            var node = AllocateNewNode(index, nodeSize);

            node->Flags = flags;

            Debug.Assert(key.Size <= ushort.MaxValue);
            node->KeySize = (ushort) key.Size;
            if (key.Options == SliceOptions.Key && node->KeySize > 0)
                key.CopyTo((byte*)node + Constants.Tree.NodeHeaderSize);

            return node;
        }

        /// <summary>
        /// Internal method that is used when splitting pages
        /// No need to do any work here, we are always adding at the end
        /// </summary>
        internal void CopyNodeDataToEndOfPage(TreeNodeHeader* other, Slice key)
        {
            var index = NumberOfEntries;

            Debug.Assert(HasSpaceFor(TreeSizeOf.NodeEntryWithAnotherKey(other, key) + Constants.Tree.NodeOffsetSize));

            var nodeSize = TreeSizeOf.NodeEntryWithAnotherKey(other, key);

            Debug.Assert(IsBranch == false || index != 0 || key.Size == 0);// branch page's first item must be the implicit ref

            var newNode = AllocateNewNode(index, nodeSize);

            Debug.Assert(key.Size <= ushort.MaxValue);
            newNode->KeySize = (ushort)key.Size;
            newNode->Flags = other->Flags;

            if (key.Options == SliceOptions.Key && key.Size > 0)
                key.CopyTo((byte*)newNode + Constants.Tree.NodeHeaderSize);

            if (IsBranch || other->Flags == (TreeNodeFlags.PageRef))
            {
                newNode->PageNumber = other->PageNumber;
                newNode->Flags = TreeNodeFlags.PageRef;
                return;
            }
            newNode->DataSize = other->DataSize;
            Memory.Copy((byte*)newNode + Constants.Tree.NodeHeaderSize + key.Size,
                                 (byte*)other + Constants.Tree.NodeHeaderSize + other->KeySize,
                                 other->DataSize);
        }

        private TreeNodeHeader* AllocateNewNode(int index, int nodeSize)
        {
            TreePageHeader* header = Header;

            int upper;

            if (Upper == ushort.MaxValue && PageSize == Constants.Compression.MaxPageSize)
                upper = Constants.Compression.MaxPageSize;
            else
                upper = header->Upper;
            
            var newNodeOffset = (ushort)(upper - nodeSize);
            Debug.Assert(newNodeOffset >= header->Lower + Constants.Tree.NodeOffsetSize);

            var node = (TreeNodeHeader*)(Base + newNodeOffset);
            KeysOffsets[index] = newNodeOffset;            

            header->Upper = newNodeOffset;
            header->Lower += Constants.Tree.NodeOffsetSize;
                        
            node->Flags = 0;
            return node;
        }

        public int SizeLeft
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get
            {
                TreePageHeader* header = Header;
                return header->Upper - header->Lower;
            }
        }

        public int SizeUsed
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get { return PageSize - SizeLeft; }
        }

        public int LastSearchPositionOrLastEntry
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get
            {
                return LastSearchPosition >= NumberOfEntries
                         ? NumberOfEntries - 1 // after the last entry, but we want to update the last entry
                         : LastSearchPosition;
            }
        }

        public void Truncate(LowLevelTransaction tx, int i)
        {
            if (i >= NumberOfEntries)
                return;

            // when truncating, we copy the values to a tmp page
            // this has the effect of compacting the page data and avoiding
            // internal page fragmentation
            TemporaryPage tmp;
            using (PageSize <= Constants.Storage.PageSize ? 
                tx.Environment.GetTemporaryPage(tx, out tmp) : 
                tx.Environment.DecompressionBuffers.GetTemporaryPage(tx, PageSize, out tmp))
            {
                var copy = tmp.GetTempPage();
                copy.TreeFlags = TreeFlags;

                var slice = default(Slice);
                for (int j = 0; j < i; j++)
                {
                    var node = GetNode(j);
                    using (TreeNodeHeader.ToSlicePtr(tx.Allocator, node, out slice))
                        copy.CopyNodeDataToEndOfPage(node, slice);
                }

                Memory.Copy(Base + Constants.Tree.PageHeaderSize,
                            copy.Base + Constants.Tree.PageHeaderSize,
                            PageSize - Constants.Tree.PageHeaderSize);

                Upper = copy.Upper;
                Lower = copy.Lower;
            }

            if (LastSearchPosition > i)
                LastSearchPosition = i;
        }

        public int NodePositionFor(LowLevelTransaction tx, Slice key)
        {
            Search(tx, key);
            return LastSearchPosition;
        }

        public int NodePositionReferencing(long pageNumber)
        {
            Debug.Assert(IsBranch);

            int referencingNode = 0;

            for (; referencingNode < NumberOfEntries; referencingNode++)
            {
                if (GetNode(referencingNode)->PageNumber == pageNumber)
                    break;
            }

            Debug.Assert(GetNode(referencingNode)->PageNumber == pageNumber);

            return referencingNode;
        }

        public override string ToString()
        {
            var result = $"#{PageNumber} (count: {NumberOfEntries}) {TreeFlags}";

            if (IsCompressed)
                result += $" Compressed ({CompressionHeader->NumberOfCompressedEntries} entries [uncompressed/compressed: {CompressionHeader->UncompressedSize}/{CompressionHeader->CompressedSize}]";

            return result;
        }

        public string Dump()
        {
            var sb = new StringBuilder();

            for (var i = 0; i < NumberOfEntries; i++)
            {

                var node = GetNode(i);

                sb.Append(TreeNodeHeader.ToDebugString(node)).Append(", ");
            }
            return sb.ToString();
        }

        public bool HasSpaceFor(LowLevelTransaction tx, int len)
        {
            if (len <= SizeLeft)
                return true;
            if (len > CalcSizeLeft())
                return false;

            Defrag(tx);

            Debug.Assert(len <= SizeLeft);

            return true;
        }

        internal void Defrag(LowLevelTransaction tx)
        {
            TemporaryPage tmp;
            using (PageSize <= Constants.Storage.PageSize ?
               tx.Environment.GetTemporaryPage(tx, out tmp) :
               tx.Environment.DecompressionBuffers.GetTemporaryPage(tx, PageSize, out tmp))
            {
                var tempPage = tmp.GetTempPage();
                Memory.Copy(tempPage.Base, Base, PageSize);

                var numberOfEntries = NumberOfEntries;

                int upper;

                if (IsCompressed)
                    upper = PageSize - Constants.Compression.HeaderSize - CompressionHeader->SectionSize;
                else
                    upper = PageSize;

                ushort* offsets = KeysOffsets;
                for (int i = 0; i < numberOfEntries; i++)
                {
                    var node = tempPage.GetNode(i);
                    var size = node->GetNodeSize() - Constants.Tree.NodeOffsetSize;
                    size += size & 1;
                    Memory.Copy(Base + upper - size, (byte*)node, size);
                    upper -= size;
                    offsets[i] = (ushort)upper;
                }

                Upper = (ushort)upper;
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private bool HasSpaceFor(int len)
        {
            return len <= SizeLeft;
        }

        public bool HasSpaceFor(LowLevelTransaction tx, Slice key, int len)
        {
            var requiredSpace = GetRequiredSpace(key, len);
            return HasSpaceFor(tx, requiredSpace);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private bool HasSpaceFor(Slice key, int len)
        {
            return HasSpaceFor(GetRequiredSpace(key, len));
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int GetRequiredSpace(Slice key, int len)
        {
            return TreeSizeOf.NodeEntry(PageMaxSpace, key, len) + Constants.Tree.NodeOffsetSize;
        }

        public int PageMaxSpace
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get{ return PageSize - Constants.Tree.PageHeaderSize; }
        }

        public PageFlags Flags
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get { return Header->Flags; }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            set { Header->Flags = value; }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ByteStringContext.Scope GetNodeKey(LowLevelTransaction tx, int nodeNumber, out Slice result)
        {
            return GetNodeKey(tx, nodeNumber, ByteStringType.Mutable | ByteStringType.External, out result);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ByteStringContext.Scope GetNodeKey(LowLevelTransaction tx, int nodeNumber, ByteStringType type/* = ByteStringType.Mutable | ByteStringType.External*/, out Slice result)
        {            
            var node = GetNode(nodeNumber);

            // This will ensure that we can create a copy or just use the pointer instead.
            if ((type & ByteStringType.External) == 0)
            {
                return TreeNodeHeader.ToSlice(tx.Allocator, node, type, out result);
            }
            return TreeNodeHeader.ToSlicePtr(tx.Allocator, node, type, out result);
        }  

        public string DebugView(LowLevelTransaction tx)
        {
            var sb = new StringBuilder();
            for (int i = 0; i < NumberOfEntries; i++)
            {
                Slice slice;
                using (GetNodeKey(tx, i, out slice))
                {
                    sb.Append(i)
                        .Append(": ")
                        .Append(slice)
                        .Append(" - ")
                        .Append(KeysOffsets[i])
                        .AppendLine();
                }
            }
            return sb.ToString();
        }

        [Conditional("VALIDATE")]
        public void DebugValidate(Tree tree, long root)
        {
            if (NumberOfEntries == 0)
                return;
#if VALIDATE
            if (Freed)
                return;
#endif
            if (IsBranch && NumberOfEntries < 2)
            {
                throw new InvalidOperationException("The branch page " + PageNumber + " has " + NumberOfEntries + " entry");
            }

            Slice prev;
            var prevScope = GetNodeKey(tree.Llt, 0, out prev);
            try
            {
                var pages = new HashSet<long>();
                for (int i = 1; i < NumberOfEntries; i++)
                {
                    var node = GetNode(i);
                    Slice current;
                    var currentScope = GetNodeKey(tree.Llt, i, out current);

                    if (SliceComparer.CompareInline(prev, current) >= 0)
                    {
                        DebugStuff.RenderAndShowTree(tree, root);
                        throw new InvalidOperationException("The page " + PageNumber + " is not sorted");
                    }

                    if (node->Flags == (TreeNodeFlags.PageRef))
                    {
                        if (pages.Add(node->PageNumber) == false)
                        {
                            DebugStuff.RenderAndShowTree(tree, root);
                            throw new InvalidOperationException("The page " + PageNumber + " references same page multiple times");
                        }
                    }
                    prevScope.Dispose();
                    prev = current;
                    prevScope = currentScope;
                }
            }
            finally
            {
                prevScope.Dispose();
            }
        }

        public bool UseMoreSizeThan(int len)
        {
            if (SizeUsed <= len)
                return false;
            var sizeUsed = CalcSizeUsed();
            return sizeUsed > len;
        }

        public int CalcSizeUsed()
        {
            int numberOfEntries = NumberOfEntries;
           
            var size = 0;
           
            byte* basePtr = Base;
            ushort* keysOffset = KeysOffsets;
            for (int i = 0; i < numberOfEntries; i++)
            {
                var node = (TreeNodeHeader*)(basePtr + keysOffset[i]);
                var nodeSize = node->GetNodeSize();
                size += nodeSize + (nodeSize & 1);
            }

            if (IsCompressed)
                size += CompressionHeader->SectionSize + Constants.Compression.HeaderSize;

            Debug.Assert(size <= PageSize);
            Debug.Assert(SizeUsed >= size);

            return size;
        }

        public int CalcSizeLeft()
        {
            var sl = PageMaxSpace - CalcSizeUsed();

            Debug.Assert(sl >= 0);

            return sl;
        }

        public void EnsureHasSpaceFor(LowLevelTransaction tx, Slice key, int len)
        {
            if (HasSpaceFor(tx, key, len) == false)
                throw new InvalidOperationException("Could not ensure that we have enough space, this is probably a bug");
        }

        public struct PagesReferencedEnumerator : IEnumerator<long>
        {
            private readonly TreePage _page;
            private int _index;

            public PagesReferencedEnumerator(TreePage treePage)
            {
                this._page = treePage;
                this._index = 0;
                this.Current = -1;
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public bool MoveNext()
            {
                // If the index is out of bounds, we are done. 
                while (this._index < _page.NumberOfEntries)
                {
                    var nodeOffset = _page.KeysOffsets[this._index];
                    var nodeHeader = (TreeNodeHeader*)(_page.Base + nodeOffset);

                    // We increment the index
                    this._index++;

                    // We use HasFlag instead because equality may fail if it comes with NewOnly applied.
                    if (nodeHeader->Flags.HasFlag(TreeNodeFlags.PageRef))
                    {
                        Current = nodeHeader->PageNumber;
                        return true;
                    }
                }

                return false;
            }

            public void Reset()
            {
                this._index = 0;
            }

            public long Current
            {
                [MethodImpl(MethodImplOptions.AggressiveInlining)]
                get;
                [MethodImpl(MethodImplOptions.AggressiveInlining)]
                private set;
            }

            object IEnumerator.Current => Current;

            public void Dispose() {}
        }
    }
}

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Text;
using Sparrow;
using Voron.Debugging;
using Voron.Global;
using Voron.Impl;
using Voron.Impl.Paging;

namespace Voron.Data.BTrees
{
    public unsafe class TreePage
    {
        public readonly int PageSize;
        public readonly string Source;
        public readonly byte* Base;

        public int LastMatch;
        public int LastSearchPosition;
        public bool Dirty;

        public TreePage(byte* basePtr, string source, int pageSize)
        {
            Base = basePtr;
            Source = source;
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
            get { return (ushort*)(Base + Constants.TreePageHeaderSize); }
        }


        // TODO: Refactor this.
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public TreeNodeHeader* Search(LowLevelTransaction tx, Slice key)
        {
            int numberOfEntries = NumberOfEntries;
            if (numberOfEntries == 0)
            {
                LastSearchPosition = 0;
                LastMatch = 1;
                return null;
            }

            switch (key.Options)
            {
                case SliceOptions.Key:
                    {   
                        if (numberOfEntries == 1)
                        {
                            var node = GetNode(0);

                            Slice pageKey;
                            using (TreeNodeHeader.ToSlicePtr(tx.Allocator, node, out pageKey))
                                LastMatch = SliceComparer.CompareInline(key, pageKey);
                            LastSearchPosition = LastMatch > 0 ? 1 : 0;
                            return LastSearchPosition == 0 ? node : null;
                        }

                        int low = IsLeaf ? 0 : 1;
                        int high = numberOfEntries - 1;
                        int position = 0;

                        ByteStringContext allocator = tx.Allocator;
                        ushort* offsets = KeysOffsets;
                        while (low <= high)
                        {
                            position = (low + high) >> 1;

                            var node = (TreeNodeHeader*)(Base + offsets[position]);

                            Slice pageKey;
                            using (TreeNodeHeader.ToSlicePtr(allocator, node, out pageKey))
                                LastMatch = SliceComparer.CompareInline(key, pageKey);

                            if (LastMatch == 0)
                                break;

                            if (LastMatch > 0)
                                low = position + 1;
                            else
                                high = position - 1;
                        }

                        if (LastMatch > 0) // found entry less than key
                        {
                            position++; // move to the smallest entry larger than the key
                        }

                        Debug.Assert(position < ushort.MaxValue);
                        LastSearchPosition = position;

                        if (position >= numberOfEntries)
                            return null;

                        return GetNode(position);
                    }
                case SliceOptions.BeforeAllKeys:
                    {
                        LastSearchPosition = 0;
                        LastMatch = 1;
                        return GetNode(0);
                    }
                case SliceOptions.AfterAllKeys:
                    {
                        LastMatch = -1;
                        LastSearchPosition = numberOfEntries - 1;
                        return GetNode(LastSearchPosition);
                    }
                default:
                    throw new NotSupportedException("This SliceOptions is not supported. Make sure you have updated this code when adding a new one.");
            }
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

        public ushort NumberOfEntries
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get
            {
                // Because we store the keys offset from the end of the head to lower
                // we can calculate the number of entries by getting the size and dividing
                // in 2, since that is the size of the offsets we use

                return (ushort)((Header->Lower - Constants.TreePageHeaderSize) >> 1);
            }
        }

        public void RemoveNode(int index)
        {
            Debug.Assert(index >= 0 || index < NumberOfEntries);

            var node = GetNode(index);
            Memory.Set((byte*) node, 0, node->GetNodeSize() - Constants.NodeOffsetSize);

            ushort* offsets = KeysOffsets;
            for (int i = index + 1; i < NumberOfEntries; i++)
            {
                offsets[i - 1] = offsets[i];
            }

            Lower -= (ushort)Constants.NodeOffsetSize;
        }

        public byte* AddPageRefNode(int index, Slice key, long pageNumber)
        {
            var node = CreateNode(index, key, TreeNodeFlags.PageRef, -1, 0);
            node->PageNumber = pageNumber;

            return null; // nothing to write into page ref node
        }

        public byte* AddDataNode(int index, Slice key, int dataSize, ushort previousNodeVersion)
        {
            Debug.Assert(dataSize >= 0);
            Debug.Assert(key.Options == SliceOptions.Key);

            var node = CreateNode(index, key, TreeNodeFlags.Data, dataSize, previousNodeVersion);
            node->DataSize = dataSize;

            return (byte*)node + Constants.NodeHeaderSize + key.Size;
        }

        public byte* AddMultiValueNode(int index, Slice key, int dataSize, ushort previousNodeVersion)
        {
            Debug.Assert(dataSize == sizeof(TreeRootHeader));
            Debug.Assert(key.Options == SliceOptions.Key);

            var node = CreateNode(index, key, TreeNodeFlags.MultiValuePageRef, dataSize, previousNodeVersion);
            node->DataSize = dataSize;

            return (byte*)node + Constants.NodeHeaderSize + key.Size;
        }

        public void ChangeImplicitRefPageNode(long implicitRefPageNumber)
        {
            const int implicitRefIndex = 0;

            var node = GetNode(implicitRefIndex);

            node->KeySize = 0;
            node->Flags = TreeNodeFlags.PageRef;
            node->Version = 1;
            node->PageNumber = implicitRefPageNumber;
        }

        private TreeNodeHeader* CreateNode(int index, Slice key, TreeNodeFlags flags, int len, ushort previousNodeVersion)
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
            var node = AllocateNewNode(index, nodeSize, previousNodeVersion);

            node->Flags = flags;

            Debug.Assert(key.Size <= ushort.MaxValue);
            node->KeySize = (ushort) key.Size;
            if (key.Options == SliceOptions.Key && node->KeySize > 0)
                key.CopyTo((byte*)node + Constants.NodeHeaderSize);

            return node;
        }

        /// <summary>
        /// Internal method that is used when splitting pages
        /// No need to do any work here, we are always adding at the end
        /// </summary>
        internal void CopyNodeDataToEndOfPage(TreeNodeHeader* other, Slice key)
        {
            var index = NumberOfEntries;

            Debug.Assert(HasSpaceFor(TreeSizeOf.NodeEntryWithAnotherKey(other, key) + Constants.NodeOffsetSize));

            var nodeSize = TreeSizeOf.NodeEntryWithAnotherKey(other, key);

            Debug.Assert(IsBranch == false || index != 0 || key.Size == 0);// branch page's first item must be the implicit ref

            var nodeVersion = other->Version; // every time new node is allocated the version is increased, but in this case we do not want to increase it
            if (nodeVersion > 0)
                nodeVersion -= 1;

            var newNode = AllocateNewNode(index, nodeSize, nodeVersion);

            Debug.Assert(key.Size <= ushort.MaxValue);
            newNode->KeySize = (ushort)key.Size;
            newNode->Flags = other->Flags;

            if (key.Options == SliceOptions.Key && key.Size > 0)
                key.CopyTo((byte*)newNode + Constants.NodeHeaderSize);

            if (IsBranch || other->Flags == (TreeNodeFlags.PageRef))
            {
                newNode->PageNumber = other->PageNumber;
                newNode->Flags = TreeNodeFlags.PageRef;
                return;
            }
            newNode->DataSize = other->DataSize;
            Memory.Copy((byte*)newNode + Constants.NodeHeaderSize + key.Size,
                                 (byte*)other + Constants.NodeHeaderSize + other->KeySize,
                                 other->DataSize);
        }

        private TreeNodeHeader* AllocateNewNode(int index, int nodeSize, ushort previousNodeVersion)
        {
            int newSize = previousNodeVersion + 1;
            if (newSize > ushort.MaxValue)
                previousNodeVersion = 0;

            TreePageHeader* header = Header;

            var newNodeOffset = (ushort)(header->Upper - nodeSize);
            Debug.Assert(newNodeOffset >= header->Lower + Constants.NodeOffsetSize);

            var node = (TreeNodeHeader*)(Base + newNodeOffset);
            KeysOffsets[index] = newNodeOffset;            

            header->Upper = newNodeOffset;
            header->Lower += Constants.NodeOffsetSize;
                        
            node->Flags = 0;
            node->Version = ++previousNodeVersion;
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
            using (tx.Environment.GetTemporaryPage(tx, out tmp))
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

                Memory.Copy(Base + Constants.TreePageHeaderSize,
                            copy.Base + Constants.TreePageHeaderSize,
                            PageSize - Constants.TreePageHeaderSize);

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
            return "#" + PageNumber + " (count: " + NumberOfEntries + ") " + TreeFlags;
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

        private void Defrag(LowLevelTransaction tx)
        {
            TemporaryPage tmp;
            using (tx.Environment.GetTemporaryPage(tx, out tmp))
            {
                var tempPage = tmp.GetTempPage();
                Memory.Copy(tempPage.Base, Base, PageSize);

                var numberOfEntries = NumberOfEntries;

                Upper = (ushort)PageSize;

                ushort* offsets = KeysOffsets;
                for (int i = 0; i < numberOfEntries; i++)
                {
                    var node = tempPage.GetNode(i);
                    var size = node->GetNodeSize() - Constants.NodeOffsetSize;
                    size += size & 1;
                    Memory.Copy(Base + Upper - size, (byte*)node, size);
                    Upper -= (ushort)size;
                    offsets[i] = Upper;
                }
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
            return TreeSizeOf.NodeEntry(PageMaxSpace, key, len) + Constants.NodeOffsetSize;
        }

        public int PageMaxSpace
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get{ return PageSize - Constants.TreePageHeaderSize; }
        }

        public PageFlags Flags
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get { return Header->Flags; }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            set { Header->Flags = value; }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ByteStringContext.Scope GetNodeKey(LowLevelTransaction tx, int nodeNumber,
            out Slice result)
        {
            return GetNodeKey(tx, nodeNumber, ByteStringType.Mutable | ByteStringType.External, out result);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ByteStringContext.Scope GetNodeKey(LowLevelTransaction tx, int nodeNumber, ByteStringType type/* = ByteStringType.Mutable | ByteStringType.External*/,
            out Slice result)
        {            
            var node = GetNode(nodeNumber);

            // This will ensure that we can create a copy or just use the pointer instead.
            if ((type & ByteStringType.External) == 0)
            {
                result = TreeNodeHeader.ToSlice(tx.Allocator, node, type);
                return new ByteStringContext<ByteStringMemoryCache>.Scope();
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
        public void DebugValidate(LowLevelTransaction tx, long root)
        {
            if (NumberOfEntries == 0)
                return;

            if (IsBranch && NumberOfEntries < 2)
            {
                throw new InvalidOperationException("The branch page " + PageNumber + " has " + NumberOfEntries + " entry");
            }

            Slice prev;
            var prevScope = GetNodeKey(tx, 0, out prev);
            try
            {
                var pages = new HashSet<long>();
                for (int i = 1; i < NumberOfEntries; i++)
                {
                    var node = GetNode(i);
                    Slice current;
                    var currentScope = GetNodeKey(tx, i, out current);

                    if (SliceComparer.CompareInline(prev, current) >= 0)
                    {
                        DebugStuff.RenderAndShowTree(tx, root);
                        throw new InvalidOperationException("The page " + PageNumber + " is not sorted");
                    }

                    if (node->Flags == (TreeNodeFlags.PageRef))
                    {
                        if (pages.Add(node->PageNumber) == false)
                        {
                            DebugStuff.RenderAndShowTree(tx, root);
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

        public List<long> GetAllOverflowPages()
        {
            var results = new List<long>(NumberOfEntries);
            for (int i = 0; i < NumberOfEntries; i++)
            {
                var nodeOffset = KeysOffsets[i];
                var nodeHeader = (TreeNodeHeader*)(Base + nodeOffset);

                if (nodeHeader->Flags == TreeNodeFlags.PageRef)
                    results.Add(nodeHeader->PageNumber);
            }

            return results;
        }
    }
}
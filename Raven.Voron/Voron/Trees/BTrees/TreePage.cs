using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Text;
using Voron.Debugging;
using Voron.Impl;
using Voron.Impl.FileHeaders;
using Voron.Impl.Paging;

namespace Voron.Trees
{
    using Sparrow;
    using Sparrow.Platform;
	using System.Runtime.CompilerServices;
    using Voron.Util;

	public unsafe class TreePage
    {
        private readonly byte* _base;
	    private readonly int _pageSize;
	    private readonly string _source;
	    private readonly TreePageHeader* _header;

	    public string Source
	    {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get { return _source; }
	    }

	    public int LastMatch;
	    public int LastSearchPosition;
	    public bool Dirty;

	    public TreePage(byte* b, string source, int pageSize)
        {
            _base = b;
	        _source = source;
	        _pageSize = pageSize;
	        _header = (TreePageHeader*)b;
        }

        
        public long PageNumber 
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get { return _header->PageNumber; }
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            set { _header->PageNumber = value; } 
        }

	    public TreePageFlags TreeFlags 
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get { return _header->TreeFlags; }
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            set { _header->TreeFlags = value; }
        }

        public ushort Lower 
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get { return _header->Lower; }
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            set { _header->Lower = value; } 
        }

        public ushort Upper 
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get { return _header->Upper; }
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            set { _header->Upper = value; } 
        }

        public int OverflowSize 
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get { return _header->OverflowSize; }
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            set { _header->OverflowSize = value; } 
        }

		public int PageSize 
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get { return _pageSize; } 
        }

        public ushort* KeysOffsets
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get { return (ushort*)(_base + Constants.PageHeaderSize); }
        }


        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public TreeNodeHeader* Search(Slice key)
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
                        var pageKey = new Slice(SliceOptions.Key);

                        if (numberOfEntries == 1)
                        {
                            var node = GetNode(0);

                            SetNodeKey(node, ref pageKey);
                            LastMatch = SliceComparer.CompareInline(key, pageKey);
                            LastSearchPosition = LastMatch > 0 ? 1 : 0;
                            return LastSearchPosition == 0 ? node : null;
                        }

                        int low = IsLeaf ? 0 : 1;
                        int high = numberOfEntries - 1;
                        int position = 0;

                        while (low <= high)
                        {
                            position = (low + high) >> 1;

                            var node = (TreeNodeHeader*)(_base + KeysOffsets[position]);

                            SetNodeKey(node, ref pageKey);

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

            var nodeOffset = KeysOffsets[n];
            var nodeHeader = (TreeNodeHeader*)(_base + nodeOffset);

            return nodeHeader;
        }

	    public bool IsLeaf
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
			get { return (_header->TreeFlags & TreePageFlags.Leaf) == TreePageFlags.Leaf; }
        }

        public bool IsBranch
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get { return (_header->TreeFlags & TreePageFlags.Branch) == TreePageFlags.Branch; }
        }

		public bool IsOverflow
		{
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get { return (_header->Flags & PageFlags.Overflow) == PageFlags.Overflow; }
		}

		public bool IsFixedSize
		{
			[MethodImpl(MethodImplOptions.AggressiveInlining)]
            get { return (_header->TreeFlags & TreePageFlags.FixedSize) == TreePageFlags.FixedSize; }
		}

        public ushort FixedSize_NumberOfEntries
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get { return _header->FixedSize_NumberOfEntries; }
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            set { _header->FixedSize_NumberOfEntries = value; }
        }

		public ushort FixedSize_StartPosition
		{
			[MethodImpl(MethodImplOptions.AggressiveInlining)]
			get { return _header->FixedSize_StartPosition; }
			[MethodImpl(MethodImplOptions.AggressiveInlining)]
			set { _header->FixedSize_StartPosition = value; }
		}

        public ushort FixedSize_ValueSize
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get { return _header->FixedSize_ValueSize; }
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            set { _header->FixedSize_ValueSize = value; }
        }

        public ushort NumberOfEntries
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get
            {
                // Because we store the keys offset from the end of the head to lower
                // we can calculate the number of entries by getting the size and dividing
                // in 2, since that is the size of the offsets we use

                return (ushort)((_header->Lower - Constants.PageHeaderSize) >> 1);
            }
        }

        public void RemoveNode(int index)
        {
            Debug.Assert(index >= 0 || index < NumberOfEntries);

            for (int i = index + 1; i < NumberOfEntries; i++)
            {
                KeysOffsets[i - 1] = KeysOffsets[i];
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
            Debug.Assert(IsBranch == false || index != 0 || key.KeyLength == 0);// branch page's first item must be the implicit ref
	        if (HasSpaceFor(key, len) == false)
		        throw new InvalidOperationException(string.Format("The page is full and cannot add an entry, this is probably a bug. Key: {0}, data length: {1}, size left: {2}", key, len, SizeLeft));

            // move higher pointers up one slot
            for (int i = NumberOfEntries; i > index; i--)
            {
                KeysOffsets[i] = KeysOffsets[i - 1];
            }

            var nodeSize = SizeOf.NodeEntry(PageMaxSpace, key, len);
            var node = AllocateNewNode(index, nodeSize, previousNodeVersion);

	        node->KeySize = key.Size;

            if (key.Options == SliceOptions.Key && key.Size > 0)
                key.CopyTo((byte*)node + Constants.NodeHeaderSize);

	        node->Flags = flags;

	        return node;
        }

        /// <summary>
        /// Internal method that is used when splitting pages
        /// No need to do any work here, we are always adding at the end
        /// </summary>
        internal void CopyNodeDataToEndOfPage(TreeNodeHeader* other, Slice key)
        {
			var index = NumberOfEntries;

			Debug.Assert(HasSpaceFor(SizeOf.NodeEntryWithAnotherKey(other, key) + Constants.NodeOffsetSize));

			var nodeSize = SizeOf.NodeEntryWithAnotherKey(other, key);

			Debug.Assert(IsBranch == false || index != 0 || key.KeyLength == 0);// branch page's first item must be the implicit ref

	        var nodeVersion = other->Version; // every time new node is allocated the version is increased, but in this case we do not want to increase it
			if (nodeVersion > 0)
				nodeVersion -= 1;

            var newNode = AllocateNewNode(index, nodeSize, nodeVersion);

			newNode->KeySize = key.Size;
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

            var newNodeOffset = (ushort)(_header->Upper - nodeSize);
            Debug.Assert(newNodeOffset >= _header->Lower + Constants.NodeOffsetSize);
            KeysOffsets[index] = newNodeOffset;
            _header->Upper = newNodeOffset;
            _header->Lower += (ushort)Constants.NodeOffsetSize;

			var node = (TreeNodeHeader*)(_base + newNodeOffset);
            node->Flags = 0;
			node->Version = ++previousNodeVersion;
            return node;
        }

        public int SizeLeft
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get { return _header->Upper - _header->Lower; }
        }

        public int SizeUsed
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
			get { return _pageSize - SizeLeft; }
        }

        public byte* Base
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get { return _base; }
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

		        var slice = CreateNewEmptyKey();

				for (int j = 0; j < i; j++)
				{
					var node = GetNode(j);
					SetNodeKey(node, ref slice);
                    copy.CopyNodeDataToEndOfPage(node, slice);
				}

                Memory.Copy(_base + Constants.PageHeaderSize,
									 copy._base + Constants.PageHeaderSize,
                                     _pageSize - Constants.PageHeaderSize);

		        Upper = copy.Upper;
				Lower = copy.Lower;
	        }

            if (LastSearchPosition > i)
                LastSearchPosition = i;
        }

        public int NodePositionFor(Slice key)
        {
            Search(key);
            return LastSearchPosition;
        }

        public override string ToString()
        {
            if ((TreeFlags & TreePageFlags.FixedSize)==TreePageFlags.FixedSize)
                return "#" + PageNumber + " (count: " + FixedSize_NumberOfEntries + ") " + TreeFlags;
            return "#" + PageNumber + " (count: " + NumberOfEntries + ") " + TreeFlags;
        }

        public string Dump()
        {
            var sb = new StringBuilder();

            for (var i = 0; i < NumberOfEntries; i++)
            {
                sb.Append(GetNodeKey(i)).Append(", ");
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
                Memory.Copy(tempPage.Base, Base, _pageSize);

			    var numberOfEntries = NumberOfEntries;

                Upper = (ushort)_pageSize;

			    for (int i = 0; i < numberOfEntries; i++)
			    {
					var node = tempPage.GetNode(i);
				    var size = node->GetNodeSize() - Constants.NodeOffsetSize;
				    size += size & 1;
                    Memory.Copy(Base + Upper - size, (byte*)node, size);
                    Upper -= (ushort)size;
				    KeysOffsets[i] = Upper;
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
	        return SizeOf.NodeEntry(PageMaxSpace, key, len) + Constants.NodeOffsetSize;
        }

	    public int PageMaxSpace
	    {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get { return _pageSize - Constants.PageHeaderSize; }
	    }

	    public PageFlags Flags
	    {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get { return _header->Flags; }
	    
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            set { _header->Flags = value; }
	    }

	    public string this[int i]
        {
            get { return GetNodeKey(i).ToString(); }
        }


        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void SetNodeKey(TreeNodeHeader* node, ref Slice slice)
        {
            Slice.SetInline(slice, node);
        }

		public Slice GetNodeKey(int nodeNumber)
		{
			var node = GetNode(nodeNumber);

			return GetNodeKey(node);
		}

        public Slice GetNodeKey(TreeNodeHeader* node)
		{
            var keySize = node->KeySize;
            var key = new byte[keySize];

            fixed (byte* ptr = key)
                Memory.CopyInline(ptr, (byte*)node + Constants.NodeHeaderSize, keySize);

            return new Slice(key);
		}

	    public string DebugView()
	    {
		    var sb = new StringBuilder();
		    for (int i = 0; i < NumberOfEntries; i++)
		    {
				sb.Append(i)
					.Append(": ")
					.Append(GetNodeKey(i))
					.Append(" - ")
					.Append(KeysOffsets[i])
					.AppendLine();
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

            var prev = GetNodeKey(0);
            var pages = new HashSet<long>();
            for (int i = 1; i < NumberOfEntries; i++)
            {
                var node = GetNode(i);
	            var current = GetNodeKey(i);

                if (prev.Compare(current) >= 0)
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

                prev = current;
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
            var size = 0;
            for (int i = 0; i < NumberOfEntries; i++)
            {
                var node = GetNode(i);
                var nodeSize = node->GetNodeSize();
                size += nodeSize + (nodeSize & 1);
            }

            Debug.Assert(size <= _pageSize);
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

		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		public Slice CreateNewEmptyKey()
		{
            return new Slice(SliceOptions.Key);
		}
    }
}
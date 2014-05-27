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
    public unsafe class Page
    {
	    public const byte PrefixCount = 8;
        private readonly byte* _base;
        private readonly PageHeader* _header;

	    public readonly string Source;
	    private readonly ushort _pageSize;

	    public int LastMatch;
        public int LastSearchPosition;
        public bool Dirty;

        public Page(byte* b, string source, ushort pageSize)
        {
            _base = b;
            _header = (PageHeader*)b;
	        Source = source;
	        _pageSize = pageSize;
        }

        public long PageNumber { get { return _header->PageNumber; } set { _header->PageNumber = value; } }

        public PageFlags Flags { get { return _header->Flags; } set { _header->Flags = value; } }

        public ushort Lower { get { return _header->Lower; } set { _header->Lower = value; } }

        public ushort Upper { get { return _header->Upper; } set { _header->Upper = value; } }

        public int OverflowSize { get { return _header->OverflowSize; } set { _header->OverflowSize = value; } }

		private ushort* PrefixOffsets { get { return _header->PrefixOffsets; } }

		private byte NextPrefixId { get { return _header->NextPrefixId; } set { _header->NextPrefixId = value; }}

        public ushort* KeysOffsets
        {
            get { return (ushort*)(_base + Constants.PageHeaderSize); }
        }

		public NodeHeader* Search(Slice key, SliceComparer cmp)
		{
			if (NumberOfEntries == 0)
			{
				LastSearchPosition = 0;
				LastMatch = 1;
				return null;
			}

			if (key.Options == SliceOptions.BeforeAllKeys)
			{
				LastSearchPosition = 0;
				LastMatch = 1;
				return GetNode(0);
			}

			if (key.Options == SliceOptions.AfterAllKeys)
			{
				LastMatch = -1;
				LastSearchPosition = NumberOfEntries - 1;
				return GetNode(LastSearchPosition);
			}

			Slice pageKey = null;
			if (NumberOfEntries == 1)
			{
				pageKey = GetFullNodeKey(0);
				LastMatch = key.Compare(pageKey, cmp);
				LastSearchPosition = LastMatch > 0 ? 1 : 0;
				return LastSearchPosition == 0 ? GetNode(0) : null;
			}

			int low = IsLeaf ? 0 : 1;
			int high = NumberOfEntries - 1;
			int position = 0;

			while (low <= high)
			{
				position = (low + high) >> 1;

				pageKey = GetFullNodeKey(position);

				LastMatch = key.Compare(pageKey, cmp);
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

			if (position >= NumberOfEntries)
				return null;
			return GetNode(position);
		}

        public NodeHeader* GetNode(int n)
        {
            Debug.Assert(n >= 0 && n < NumberOfEntries);

            var nodeOffset = KeysOffsets[n];
            var nodeHeader = (NodeHeader*)(_base + nodeOffset);

            return nodeHeader;
        }

	    private PrefixNode GetPrefixNode(byte n)
	    {
			Debug.Assert(n < PrefixCount, "Requested prefix number was: " + n);
			Debug.Assert(n <= (NextPrefixId - 1), "Requested prefix number was: " + n + ", while the max available prefix id is " + (NextPrefixId - 1));

		    var prefixOffset = PrefixOffsets[n];

		    if (prefixOffset == 0) // allocated but not written yet
			    return null;

		    return new PrefixNode(_base + prefixOffset);
	    }

	    public bool IsLeaf
        {
            get { return _header->Flags==(PageFlags.Leaf); }
        }

        public bool IsBranch
        {
            get { return _header->Flags==(PageFlags.Branch); }
        }

		public bool IsOverflow
		{
			get { return _header->Flags==(PageFlags.Overflow); }
		}

        public ushort NumberOfEntries
        {
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

            for (int i = index+1; i < NumberOfEntries; i++)
            {
                KeysOffsets[i-1] = KeysOffsets[i];
            }

            Lower -= (ushort)Constants.NodeOffsetSize;
        }

		public byte* AddPageRefNode(int index, PrefixedSlice key, long pageNumber)
		{
			var node = CreateNode(index, key, NodeFlags.PageRef, -1, 0);
			node->PageNumber = pageNumber;

			return null; // nothing to write into page ref node
		}

		public byte* AddDataNode(int index, PrefixedSlice key, int dataSize, ushort previousNodeVersion)
		{
			Debug.Assert(dataSize >= 0);
			Debug.Assert(key.Options == SliceOptions.Key);

			var node = CreateNode(index, key, NodeFlags.Data, dataSize, previousNodeVersion);
			node->DataSize = dataSize;

			return (byte*)node + Constants.NodeHeaderSize + key.Size;
		}

		public byte* AddMultiValueNode(int index, PrefixedSlice key, int dataSize, ushort previousNodeVersion)
		{
			Debug.Assert(dataSize == sizeof(TreeRootHeader));
			Debug.Assert(key.Options == SliceOptions.Key);

			var node = CreateNode(index, key, NodeFlags.MultiValuePageRef, dataSize, previousNodeVersion);
			node->DataSize = dataSize;

			return (byte*)node + Constants.NodeHeaderSize + key.Size;
		}

		public void ChangeImplicitRefPageNode(long implicitRefPageNumber)
		{
			const int implicitRefIndex = 0;

			var node = GetNode(implicitRefIndex);

			node->KeySize = 0;
			node->Flags = NodeFlags.PageRef;
			node->Version = 1;
			node->PageNumber = implicitRefPageNumber;
		}

        private NodeHeader* CreateNode(int index, PrefixedSlice key, NodeFlags flags, int len, ushort previousNodeVersion)
        {
            Debug.Assert(index <= NumberOfEntries && index >= 0);
            Debug.Assert(IsBranch == false || index != 0 || key.Size == 0);// branch page's first item must be the implicit ref
            if (HasSpaceFor(key, len) == false)
                throw new InvalidOperationException("The page is full and cannot add an entry, this is probably a bug");

	        if (key.NewPrefix != null)
		        WritePrefix(key.NewPrefix, key.PrefixId);

            // move higher pointers up one slot
            for (int i = NumberOfEntries; i > index; i--)
            {
                KeysOffsets[i] = KeysOffsets[i - 1];
            }

            var nodeSize = SizeOf.NodeEntry(PageMaxSpace, key, len);
            var node = AllocateNewNode(index, nodeSize, previousNodeVersion);

	        node->KeySize = key.Size;

            if (key.Options == SliceOptions.Key) // TODO arek - check this if statement
                key.CopyTo((byte*)node + Constants.NodeHeaderSize);

	        node->Flags = flags;

	        return node;
        }

        /// <summary>
        /// Internal method that is used when splitting pages
        /// No need to do any work here, we are always adding at the end
        /// </summary>
        internal void CopyNodeDataToEndOfPage(NodeHeader* other, PrefixedSlice key)
        {
			// TODO arek - go though all callers of this method and try to change the api to avoid such constiction: rightPage.CopyNodeDataToEndOfPage(node, rightPage.ConvertToPrefixedKey(_page.GetFullNodeKey(node), rightPage.NumberOfEntries));

			var index = NumberOfEntries;

	        var nodeKey = key;
            Debug.Assert(HasSpaceFor(SizeOf.NodeEntryWithAnotherKey(other, nodeKey) + Constants.NodeOffsetSize + SizeOf.NewPrefix(key)));
            
            var nodeSize = SizeOf.NodeEntryWithAnotherKey(other, nodeKey);

			// TODO arek other->keySize == 0? beforeallkeys or after all keys?
			if (other->KeySize == 0/* TODO arek && key == null*/) // when copy first item from branch which is implicit ref
			{
				nodeSize += nodeKey.Size;
			}

            Debug.Assert(IsBranch == false || index != 0 || nodeKey.Size == 0);// branch page's first item must be the implicit ref

	        var nodeVersion = other->Version; // every time new node is allocated the version is increased, but in this case we do not want to increase it
			if (nodeVersion > 0)
				nodeVersion -= 1;

	        if (nodeKey.NewPrefix != null)
		        WritePrefix(nodeKey.NewPrefix, nodeKey.PrefixId);

            var newNode = AllocateNewNode(index, nodeSize, nodeVersion);

			newNode->KeySize = nodeKey.Size;
            newNode->Flags = other->Flags;
            nodeKey.CopyTo((byte*)newNode + Constants.NodeHeaderSize);

            if (IsBranch || other->Flags==(NodeFlags.PageRef))
            {
                newNode->PageNumber = other->PageNumber;
                newNode->Flags = NodeFlags.PageRef;
                return;
            }
            newNode->DataSize = other->DataSize;
            NativeMethods.memcpy((byte*)newNode + Constants.NodeHeaderSize + nodeKey.Size,
                                 (byte*)other + Constants.NodeHeaderSize + other->KeySize,
                                 other->DataSize);
        }

	    public PrefixedSlice ConvertToPrefixedKey(Slice key, int nodeIndex)
	    {
		    PrefixedSlice prefixedSlice;

		    if (TryUseExistingPrefix(key, out prefixedSlice)) 
				return prefixedSlice;

		    if (TryCreateNewPrefix(key, nodeIndex, out prefixedSlice))
			    return prefixedSlice;

		    if (key.Size == 0)
			    return PrefixedSlice.Empty;

			return new PrefixedSlice(key);
	    }

	    private bool TryUseExistingPrefix(Slice key, out PrefixedSlice prefixedSlice)
	    {
		    for (byte prefixId = 0; prefixId < NextPrefixId; prefixId++)
		    {
			    var prefix = GetPrefixNode(prefixId);

			    var length = key.FindPrefixSize(prefix.Value);
			    if (length == 0) 
					continue;

				// TODO arek  we need better prefix detection here, not only the first match

			    prefixedSlice = new PrefixedSlice(prefixId, (ushort) length, key);
			    return true;
		    }

		    prefixedSlice = null;
		    return false;
	    }

		private bool TryCreateNewPrefix(Slice key, int nodeIndex, out PrefixedSlice prefixedSlice)
		{
			if (NextPrefixId >= PrefixCount || NumberOfEntries == 0)
			{
				prefixedSlice = null;
				return false;
			}

			Slice left;
			Slice right;

			if (nodeIndex > 0 && nodeIndex < NumberOfEntries) // middle
			{
				left = GetFullNodeKey(nodeIndex - 1);
				right = GetFullNodeKey(nodeIndex);
			}
			else if (nodeIndex == 0) // first
			{
				left = null;
				right = GetFullNodeKey(0);
			}
			else if (nodeIndex == NumberOfEntries) // last
			{
				left = GetFullNodeKey(nodeIndex - 1);
				right = null;
			}
			else
			{
				throw new InvalidOperationException();
			}

			ushort leftLength = 0;
			ushort rightLength = 0;

			if (left != null && left.Size > 0) // not before all keys
				leftLength = (ushort)key.FindPrefixSize(left);

			if (right != null)
				rightLength = (ushort)key.FindPrefixSize(right);

			// TODO arek - shouldn't we specify the min prefix length

			if (leftLength > 0 && leftLength > rightLength)
			{
				prefixedSlice = new PrefixedSlice(NextPrefixId, leftLength, key)
				{
					NewPrefix = new Slice(left, leftLength)
				};

				return true;
			}

			if (rightLength > 0 && rightLength > leftLength)
			{
				prefixedSlice = new PrefixedSlice(NextPrefixId, rightLength, key)
				{
					NewPrefix = new Slice(right, rightLength)
				};

				return true;
			}

			prefixedSlice = null;
			return false;
		}

        private NodeHeader* AllocateNewNode(int index, int nodeSize, ushort previousNodeVersion)
        {
	        int newSize = previousNodeVersion + 1;
	        if (newSize > ushort.MaxValue)
				previousNodeVersion = 0;

            var newNodeOffset = (ushort)(_header->Upper - nodeSize);
            Debug.Assert(newNodeOffset >= _header->Lower + Constants.NodeOffsetSize);
            KeysOffsets[index] = newNodeOffset;
            _header->Upper = newNodeOffset;
            _header->Lower += (ushort)Constants.NodeOffsetSize;

			var node = (NodeHeader*)(_base + newNodeOffset);
            node->Flags = 0;
			node->Version = ++previousNodeVersion;
            return node;
        }

		private void WritePrefix(Slice prefix, int prefixId)
		{
			var prefixNodeSize = Constants.PrefixNodeHeaderSize + prefix.Size;
			var prefixNodeOffset = (ushort)(Upper - prefixNodeSize);
			Upper = prefixNodeOffset;

			Debug.Assert(NextPrefixId == prefixId);

			if (PrefixOffsets[prefixId] != 0)
				throw new InvalidOperationException(string.Format("Cannot write a prefix '{0}' at the following offset position: {1} because it's already taken by another prefix. The offset for the prefix {1} is {2}. ", prefix, prefixId, PrefixOffsets[prefixId]));

			PrefixOffsets[prefixId] = prefixNodeOffset;

			var prefixNodeHeader = (PrefixNodeHeader*)(_base + prefixNodeOffset);

			prefixNodeHeader->PrefixLength = prefix.Size;

			prefix.CopyTo((byte*)prefixNodeHeader + Constants.PrefixNodeHeaderSize);

			NextPrefixId++;
		}

        public int SizeLeft
        {
            get { return _header->Upper - _header->Lower; }
        }

        public int SizeUsed
        {
			get { return _pageSize - SizeLeft; }
        }

        public byte* Base
        {
            get { return _base; }
        }

        public int LastSearchPositionOrLastEntry
        {

            get
            {
                return LastSearchPosition >= NumberOfEntries
                         ? NumberOfEntries - 1 // after the last entry, but we want to update the last entry
                         : LastSearchPosition;
            }
        }

        public void Truncate(Transaction tx, int i)
        {
            if (i >= NumberOfEntries)
                return;

            // when truncating, we copy the values to a tmp page
            // this has the effect of compacting the page data and avoiding
            // internal page fragmentation
	        TemporaryPage tmp;
	        using (tx.Environment.GetTemporaryPage(tx, out tmp))
	        {
		        var copy = tmp.TempPage;
				copy.Flags = Flags;

		        copy.ClearPrefixInfo();

				for (int j = 0; j < i; j++)
				{
					var node = GetNode(j);
					copy.CopyNodeDataToEndOfPage(node, copy.ConvertToPrefixedKey(GetFullNodeKey(node), copy.NumberOfEntries));
				}

				NativeMethods.memcpy(_base + Constants.PageHeaderSize,
									 copy._base + Constants.PageHeaderSize,
									 _pageSize - Constants.PageHeaderSize);

				ClearPrefixInfo();
				NextPrefixId = copy.NextPrefixId;

				for (var prefixId = 0; prefixId < NextPrefixId; prefixId++)
		        {
					PrefixOffsets[prefixId] = copy.PrefixOffsets[prefixId];
		        }

				Upper = copy.Upper;
				Lower = copy.Lower;
	        }

            if (LastSearchPosition > i)
                LastSearchPosition = i;
        }

	    public void ClearPrefixInfo()
	    {
		    NativeMethods.memset((byte*) PrefixOffsets, 0, sizeof (ushort)*PrefixCount);
			NextPrefixId = 0;
	    }

	    public int NodePositionFor(Slice key, SliceComparer cmp)
        {
            Search(key, cmp);
            return LastSearchPosition;
        }

        public override string ToString()
        {
            return "#" + PageNumber + " (count: " + NumberOfEntries + ") " + Flags;
        }

        public string Dump()
        {
            var sb = new StringBuilder();
            var slice = new Slice(SliceOptions.Key);
            for (var i = 0; i < NumberOfEntries; i++)
            {
                var n = GetNode(i);
                slice.Set(n);
                sb.Append(slice).Append(", ");
            }
            return sb.ToString();
        }

        public bool HasSpaceFor(Transaction tx, int len)
        {
            if (len <= SizeLeft)
                return true;
            if (len > CalcSizeLeft())
                return false;

            Defrag(tx);

            Debug.Assert(len <= SizeLeft);

            return true;
        }

	    private void Defrag(Transaction tx)
	    {
		    TemporaryPage tmp;
		    using (tx.Environment.GetTemporaryPage(tx, out tmp))
		    {
			    var tempPage = tmp.TempPage;
			    NativeMethods.memcpy(tempPage.Base, Base, _pageSize);

			    var numberOfEntries = NumberOfEntries;

			    Upper = _pageSize;

			    for (int i = 0; i < numberOfEntries; i++)
			    {
					var node = tempPage.GetNode(i);
				    var size = node->GetNodeSize() - Constants.NodeOffsetSize;
				    size += size & 1;
				    NativeMethods.memcpy(Base + Upper - size, (byte*) node, size);
				    Upper -= (ushort) size;
				    KeysOffsets[i] = Upper;
			    }

			    for (byte i = 0; i < NextPrefixId; i++)
			    {
				    var prefixNode = tempPage.GetPrefixNode(i);

					if (prefixNode == null)
						continue;

				    var prefixNodeSize = prefixNode.Size;

					NativeMethods.memcpy(Base + Upper - prefixNodeSize, prefixNode.Base, prefixNodeSize);
				    Upper -= (ushort) prefixNodeSize;
				    PrefixOffsets[i] = Upper;
			    }
		    }
	    }

	    private bool HasSpaceFor(int len)
        {
            return len <= SizeLeft;
        }

        public bool HasSpaceFor(Transaction tx, PrefixedSlice key, int len)
        {
            var requiredSpace = GetRequiredSpace(key, len);
            return HasSpaceFor(tx, requiredSpace);
        }

        private bool HasSpaceFor(PrefixedSlice key, int len)
        {
            return HasSpaceFor(GetRequiredSpace(key, len));
        }

        public int GetRequiredSpace(PrefixedSlice key, int len)
        {
	        return SizeOf.NodeEntry(PageMaxSpace, key, len) + Constants.NodeOffsetSize + SizeOf.NewPrefix(key);
        }

	    public int PageMaxSpace
	    {
		    get
		    {
			    return _pageSize - Constants.PageHeaderSize;
		    }
	    }

	    public string this[int i]
        {
            get { return GetFullNodeKey(i).ToString(); }
        }

	    public Slice GetFullNodeKey(NodeHeader* node)
	    {
		    if (node->KeySize == 0)
			    return Slice.Empty;

			var prefixedSlice = new PrefixedSlice(node);

		    if (prefixedSlice.PrefixUsage > 0)
		    {
				Debug.Assert(prefixedSlice.PrefixId < PrefixCount);

				var prefixNode = GetPrefixNode(prefixedSlice.PrefixId);

				var key = new byte[prefixedSlice.PrefixUsage + prefixedSlice.NonPrefixedDataSize];

				prefixNode.Value.CopyTo(0, key, 0, prefixedSlice.PrefixUsage);

				fixed (byte* ptr1 = key)
					NativeMethods.memcpy(ptr1 + prefixedSlice.PrefixUsage, prefixedSlice.NonPrefixedData, prefixedSlice.NonPrefixedDataSize);

				return new Slice(key);
		    }

			return new Slice(prefixedSlice.NonPrefixedData, prefixedSlice.NonPrefixedDataSize);
	    }

		public Slice GetFullNodeKey(int nodeNumber)
		{
			var node = GetNode(nodeNumber);

			return GetFullNodeKey(node);
		}

	    public string DebugView()
	    {
		    var sb = new StringBuilder();
		    for (int i = 0; i < NumberOfEntries; i++)
		    {
				// TODO arek
				//sb.Append(i)
				//	.Append(": ")
				//	.Append(new Slice((NodeHeader*)( _base + KeysOffsets[i])))
				//	.Append(" - ")
				//	.Append(KeysOffsets[i])
				//	.AppendLine();
		    }
		    return sb.ToString();
	    }

        [Conditional("VALIDATE")]
        public void DebugValidate(Transaction tx, SliceComparer comparer, long root)
        {
            if (NumberOfEntries == 0)
                return;

            var prev = GetFullNodeKey(0);
            var pages = new HashSet<long>();
            for (int i = 1; i < NumberOfEntries; i++)
            {
                var node = GetNode(i);
	            var current = GetFullNodeKey(i);

                if (prev.Compare(current, comparer) >= 0)
                {
                    DebugStuff.RenderAndShow(tx, root, 1);
                    throw new InvalidOperationException("The page " + PageNumber + " is not sorted");
                }

                if (node->Flags==(NodeFlags.PageRef))
                {
                    if (pages.Add(node->PageNumber) == false)
                    {
                        DebugStuff.RenderAndShow(tx, root, 1);
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
            var sizeUsed  = CalcSizeUsed();
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

			for (byte i = 0; i < NextPrefixId; i++)
			{
				var prefixNode = GetPrefixNode(i);

				if (prefixNode == null) // allocated but not written yet
					continue;

				size += prefixNode.Size;
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

        public void EnsureHasSpaceFor(Transaction tx, PrefixedSlice key, int len)
        {
            if (HasSpaceFor(tx, key, len) == false)
                throw new InvalidOperationException("Could not ensure that we have enough space, this is probably a bug");
        }
    }
}
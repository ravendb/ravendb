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

		public ushort PageSize { get { return _pageSize; } }

        public ushort* KeysOffsets
        {
            get { return (ushort*)(_base + Constants.PageHeaderSize); }
        }

		public NodeHeader* Search(MemorySlice key)
		{
			key.PrepareForSearching();

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

			MemorySlice pageKey = null;
			if (NumberOfEntries == 1)
			{
				pageKey = GetNodeKey(0);
				LastMatch = key.Compare(pageKey);
				LastSearchPosition = LastMatch > 0 ? 1 : 0;
				return LastSearchPosition == 0 ? GetNode(0) : null;
			}

			int low = IsLeaf ? 0 : 1;
			int high = NumberOfEntries - 1;
			int position = 0;

			while (low <= high)
			{
				position = (low + high) >> 1;

				pageKey = GetNodeKey(position);

				LastMatch = key.Compare(pageKey);
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

		    return new PrefixNode(_base + prefixOffset, PageNumber);
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
		        WritePrefix(key.NewPrefix, key.Header.PrefixId);

            // move higher pointers up one slot
            for (int i = NumberOfEntries; i > index; i--)
            {
                KeysOffsets[i] = KeysOffsets[i - 1];
            }

            var nodeSize = SizeOf.NodeEntry(PageMaxSpace, key, len);
            var node = AllocateNewNode(index, nodeSize, previousNodeVersion);

	        node->KeySize = key.Size;

            if (key.Options == SliceOptions.Key)
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
			var index = NumberOfEntries;

			Debug.Assert(HasSpaceFor(SizeOf.NodeEntryWithAnotherKey(other, key) + Constants.NodeOffsetSize + SizeOf.NewPrefix(key)));

			var nodeSize = SizeOf.NodeEntryWithAnotherKey(other, key);

			Debug.Assert(IsBranch == false || index != 0 || key.Size == 0);// branch page's first item must be the implicit ref

	        var nodeVersion = other->Version; // every time new node is allocated the version is increased, but in this case we do not want to increase it
			if (nodeVersion > 0)
				nodeVersion -= 1;

			if (key.NewPrefix != null)
				WritePrefix(key.NewPrefix, key.Header.PrefixId);

            var newNode = AllocateNewNode(index, nodeSize, nodeVersion);

			newNode->KeySize = key.Size;
            newNode->Flags = other->Flags;

			if(key.Options == SliceOptions.Key)
				key.CopyTo((byte*)newNode + Constants.NodeHeaderSize);

            if (IsBranch || other->Flags==(NodeFlags.PageRef))
            {
                newNode->PageNumber = other->PageNumber;
                newNode->Flags = NodeFlags.PageRef;
                return;
            }
            newNode->DataSize = other->DataSize;
			NativeMethods.memcpy((byte*)newNode + Constants.NodeHeaderSize + key.Size,
                                 (byte*)other + Constants.NodeHeaderSize + other->KeySize,
                                 other->DataSize);
        }

	    public PrefixedSlice ConvertToPrefixedKey(MemorySlice key, int nodeIndex)
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

	    private bool TryUseExistingPrefix(MemorySlice key, out PrefixedSlice prefixedSlice)
	    {
		    Tuple<byte, ushort> bestMatch = null;

			for (byte prefixId = 0; prefixId < NextPrefixId; prefixId++)
			{
				var prefix = GetPrefixNode(prefixId);

				var length = key.FindPrefixSize(prefix.Value);
				if (length == 0)
					continue;

				if (length == prefix.PrefixLength) // full prefix usage
				{
					prefixedSlice = new PrefixedSlice(prefixId, length, key.Skip(length));
					return true;
				}

				// keep on looking for a better prefix

				if (bestMatch == null)
				{
					bestMatch = new Tuple<byte, ushort>(prefixId, length);
					continue;
				}

				if (length > bestMatch.Item2)
					bestMatch = new Tuple<byte, ushort>(prefixId, length);
			}

		    if (bestMatch != null && bestMatch.Item2 > MinPrefixLength(key))
		    {
			    prefixedSlice = new PrefixedSlice(bestMatch.Item1, bestMatch.Item2, key.Skip(bestMatch.Item2));
			    return true;
		    }

			prefixedSlice = null;
			return false;
	    }

		private bool TryCreateNewPrefix(MemorySlice key, int nodeIndex, out PrefixedSlice prefixedSlice)
		{
			if (NextPrefixId >= PrefixCount || NumberOfEntries == 0)
			{
				prefixedSlice = null;
				return false;
			}

			PrefixedSlice left;
			PrefixedSlice right;

			if (nodeIndex > 0 && nodeIndex < NumberOfEntries) // middle
			{
				left = GetNodeKey(nodeIndex - 1);
				right = GetNodeKey(nodeIndex);
			}
			else if (nodeIndex == 0) // first
			{
				left = null;
				right = GetNodeKey(0);
			}
			else if (nodeIndex == NumberOfEntries) // last
			{
				left = GetNodeKey(nodeIndex - 1);
				right = null;
			}
			else
				throw new NotSupportedException("Invalid node index prefix: " + nodeIndex + ". Number of entries: " + NumberOfEntries);

			ushort leftLength = 0;
			ushort rightLength = 0;

			if (left != null && left.Size > 0) // not before all keys
				leftLength = key.FindPrefixSize(left);

			if (right != null)
				rightLength = key.FindPrefixSize(right);

			var minPrefixLength = MinPrefixLength(key);

			if (left != null && leftLength > minPrefixLength && leftLength > rightLength)
			{
				prefixedSlice = new PrefixedSlice(NextPrefixId, leftLength, key.Skip(leftLength))
				{
					NewPrefix = new Slice(left.ToSlice(), leftLength)
				};

				return true;
			}

			if (right != null && rightLength > minPrefixLength && rightLength > leftLength)
			{
				prefixedSlice = new PrefixedSlice(NextPrefixId, rightLength, key.Skip(rightLength))
				{
					NewPrefix = new Slice(right.ToSlice(), rightLength)
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
			prefixNodeSize += prefixNodeSize & 1;

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
					copy.CopyNodeDataToEndOfPage(node, copy.ConvertToPrefixedKey(GetNodeKey(node), copy.NumberOfEntries));
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

        public int NodePositionFor(MemorySlice key)
        {
            Search(key);
            return LastSearchPosition;
        }

        public override string ToString()
        {
            return "#" + PageNumber + " (count: " + NumberOfEntries + ") " + Flags;
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
				    prefixNodeSize += prefixNodeSize & 1;

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
            get { return GetNodeKey(i).ToString(); }
        }

	    public PrefixedSlice GetNodeKey(NodeHeader* node)
	    {
			if (node->KeySize == 0)
				return PrefixedSlice.Empty;

			var slice = new PrefixedSlice(node);

			if (slice.Header.PrefixUsage == 0)
			{
				Debug.Assert(slice.Header.PrefixId == PrefixedSlice.NonPrefixedId);

				return slice;
			}

			Debug.Assert(slice.Header.PrefixId < PrefixCount);

			var prefix = GetPrefixNode(slice.Header.PrefixId);

			slice.SetPrefix(prefix);

			return slice;
	    }

		public PrefixedSlice GetNodeKey(int nodeNumber)
		{
			var node = GetNode(nodeNumber);

			return GetNodeKey(node);
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
        public void DebugValidate(Transaction tx, long root)
        {
            if (NumberOfEntries == 0)
                return;

            var prev = GetNodeKey(0);
            var pages = new HashSet<long>();
            for (int i = 1; i < NumberOfEntries; i++)
            {
                var node = GetNode(i);
	            var current = GetNodeKey(i);

                if (prev.Compare(current) >= 0)
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

				size += prefixNode.Size + (prefixNode.Size & 1);
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

		private static ushort MinPrefixLength(MemorySlice key)
		{
			return (ushort) Math.Max(key.KeyLength * 0.2, 2);
		}
    }
}
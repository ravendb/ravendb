using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Text;
using Voron.Debugging;
using Voron.Impl;
using Voron.Impl.FileHeaders;
using Voron.Impl.Paging;
using Voron.Util.Conversion;

namespace Voron.Trees
{
    public unsafe class Page
    {
        private readonly byte* _base;
        private readonly PageHeader* _header;

	    public readonly string Source;

        public int LastMatch;
        public int LastSearchPosition;
        public bool Dirty;

        public Page(byte* b, string source)
        {
            _base = b;
            _header = (PageHeader*)b;
	        Source = source;
        }

        public long PageNumber { get { return _header->PageNumber; } set { _header->PageNumber = value; } }

        public PageFlags Flags { get { return _header->Flags; } set { _header->Flags = value; } }

        public ushort Lower { get { return _header->Lower; } set { _header->Lower = value; } }

        public ushort Upper { get { return _header->Upper; } set { _header->Upper = value; } }

        public int OverflowSize { get { return _header->OverflowSize; } set { _header->OverflowSize = value; } }

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

			int low = IsLeaf ? 0 : 1;
			int high = NumberOfEntries - 1;
			int position = 0;

			var pageKey = new Slice(SliceOptions.Key);
			bool matched = false;
			NodeHeader* node = null;
			while (low <= high)
			{
				position = (low + high) >> 1;

				node = GetNode(position);
				pageKey.Set(node);

				LastMatch = key.Compare(pageKey, cmp);
				matched = true;
				if (LastMatch == 0)
					break;

				if (LastMatch > 0)
					low = position + 1;
				else
					high = position - 1;
			}

			if (matched == false)
			{
				LastMatch = key.Compare(pageKey, cmp);
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

		public byte* AddPageRefNode(int index, Slice key, long pageNumber)
		{
			var node = CreateNode(index, key, NodeFlags.PageRef, -1, 0);
			node->PageNumber = pageNumber;

			return null; // nothing to write into page ref node
		}

		public byte* AddDataNode(int index, Slice key, int dataSize, ushort previousNodeVersion)
		{
			Debug.Assert(dataSize >= 0);
			Debug.Assert(key.Options == SliceOptions.Key);

			var node = CreateNode(index, key, NodeFlags.Data, dataSize, previousNodeVersion);
			node->DataSize = dataSize;

			return (byte*)node + Constants.NodeHeaderSize + key.Size;
		}

		public byte* AddMultiValueNode(int index, Slice key, int dataSize, ushort previousNodeVersion)
		{
			Debug.Assert(dataSize == sizeof(TreeRootHeader));
			Debug.Assert(key.Options == SliceOptions.Key);

			var node = CreateNode(index, key, NodeFlags.MultiValuePageRef, dataSize, previousNodeVersion);
			node->DataSize = dataSize;

			return (byte*)node + Constants.NodeHeaderSize + key.Size;
		}

        private NodeHeader* CreateNode(int index, Slice key, NodeFlags flags, int len, ushort previousNodeVersion)
        {
            Debug.Assert(index <= NumberOfEntries && index >= 0);
            Debug.Assert(IsBranch == false || index != 0 || key.Size == 0);// branch page's first item must be the implicit ref
            if (HasSpaceFor(key, len) == false)
                throw new InvalidOperationException("The page is full and cannot add an entry, this is probably a bug");

            // move higher pointers up one slot
            for (int i = NumberOfEntries; i > index; i--)
            {
                KeysOffsets[i] = KeysOffsets[i - 1];
            }
            var nodeSize = SizeOf.NodeEntry(AbstractPager.PageMaxSpace, key, len);
            var node = AllocateNewNode(index, key, nodeSize, previousNodeVersion);

            if (key.Options == SliceOptions.Key)
                key.CopyTo((byte*)node + Constants.NodeHeaderSize);

	        node->Flags = flags;

	        return node;
        }

        /// <summary>
        /// Internal method that is used when splitting pages
        /// No need to do any work here, we are always adding at the end
        /// </summary>
        internal void CopyNodeDataToEndOfPage(NodeHeader* other, Slice key = null)
        {
			var nodeKey = key ?? new Slice(other);
            Debug.Assert(HasSpaceFor(SizeOf.NodeEntryWithAnotherKey(other, nodeKey) + Constants.NodeOffsetSize));
            
            var index = NumberOfEntries;

            var nodeSize = SizeOf.NodeEntryWithAnotherKey(other, nodeKey);


			if (other->KeySize == 0 && key == null) // when copy first item from branch which is implicit ref
			{
				nodeSize += nodeKey.Size;
			}

            Debug.Assert(IsBranch == false || index != 0 || nodeKey.Size == 0);// branch page's first item must be the implicit ref

	        var nodeVersion = other->Version; // every time new node is allocated the version is increased, but in this case we do not want to increase it
			if (nodeVersion > 0)
				nodeVersion -= 1;

            var newNode = AllocateNewNode(index, nodeKey, nodeSize, nodeVersion);
            newNode->Flags = other->Flags;
            nodeKey.CopyTo((byte*)newNode + Constants.NodeHeaderSize);

            if (IsBranch || other->Flags==(NodeFlags.PageRef))
            {
                newNode->PageNumber = other->PageNumber;
                newNode->Flags = NodeFlags.PageRef;
                return;
            }
            newNode->DataSize = other->DataSize;
            NativeMethods.memcpy((byte*)newNode + Constants.NodeHeaderSize + other->KeySize,
                                 (byte*)other + Constants.NodeHeaderSize + other->KeySize,
                                 other->DataSize);
        }


        private NodeHeader* AllocateNewNode(int index, Slice key, int nodeSize, ushort previousNodeVersion)
        {
			if (previousNodeVersion + 1 > ushort.MaxValue)
				previousNodeVersion = 0;

            var newNodeOffset = (ushort)(_header->Upper - nodeSize);
            Debug.Assert(newNodeOffset >= _header->Lower + Constants.NodeOffsetSize);
            KeysOffsets[index] = newNodeOffset;
            _header->Upper = newNodeOffset;
            _header->Lower += (ushort)Constants.NodeOffsetSize;

            var node = (NodeHeader*)(_base + newNodeOffset);
            node->KeySize = key.Size;
            node->Flags = 0;
			node->Version = ++previousNodeVersion;
            return node;
        }


        public int SizeLeft
        {
            get { return _header->Upper - _header->Lower; }
        }

        public int SizeUsed
        {
			get { return _header->Lower + AbstractPager.PageMaxSpace - _header->Upper; }
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
            var copy = tx.Environment.TemporaryPage.TempPage;
            copy.Flags = Flags;
            for (int j = 0; j < i; j++)
            {
                copy.CopyNodeDataToEndOfPage(GetNode(j));
            }
            NativeMethods.memcpy(_base + Constants.PageHeaderSize,
                                 copy._base + Constants.PageHeaderSize,
								 AbstractPager.PageSize - Constants.PageHeaderSize);

            Upper = copy.Upper;
            Lower = copy.Lower;

            if (LastSearchPosition > i)
                LastSearchPosition = i;
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
            var tmp = tx.Environment.TemporaryPage.TempPage;
            NativeMethods.memcpy(tmp.Base, Base, AbstractPager.PageSize);

            var numberOfEntries = NumberOfEntries;

            Upper = AbstractPager.PageSize;

            for (int i = 0; i < numberOfEntries; i++)
            {
                var node = tmp.GetNode(i);
                var size = node->GetNodeSize() - Constants.NodeOffsetSize;
                size += size & 1;
                NativeMethods.memcpy(Base + Upper - size, (byte*) node, size);
                Upper -= (ushort)size;
                KeysOffsets[i] = Upper;
            }
        }

        private bool HasSpaceFor(int len)
        {
            return len <= SizeLeft;
        }

        public bool HasSpaceFor(Transaction tx, Slice key, int len)
        {
            var requiredSpace = GetRequiredSpace(key, len);
            return HasSpaceFor(tx, requiredSpace);
        }

        private bool HasSpaceFor(Slice key, int len)
        {
            return HasSpaceFor(GetRequiredSpace(key, len));
        }

        public int GetRequiredSpace(Slice key, int len)
        {
			return SizeOf.NodeEntry(AbstractPager.PageMaxSpace, key, len) + Constants.NodeOffsetSize;
        }

        public string this[int i]
        {
            get { return new Slice(GetNode(i)).ToString(); }
        }

		public Slice GetNodeKey(int nodeNumber)
		{
			var node = GetNode(nodeNumber);
			var keySize = node->KeySize;
			var key = new byte[keySize];

			fixed (byte* ptr = key)
				NativeMethods.memcpy(ptr, (byte*)node + Constants.NodeHeaderSize, keySize);

			return new Slice(key);
		}

	    public string DebugView()
	    {
		    var sb = new StringBuilder();
		    for (int i = 0; i < NumberOfEntries; i++)
		    {
			    sb.Append(i)
				    .Append(": ")
				    .Append(new Slice((NodeHeader*)( _base + KeysOffsets[i])))
				    .Append(" - ")
				    .Append(KeysOffsets[i])
				    .AppendLine();
		    }
		    return sb.ToString();
	    }

        [Conditional("VALIDATE")]
        public void DebugValidate(Transaction tx, SliceComparer comparer, long root)
        {
            if (NumberOfEntries == 0)
                return;

            var prev = new Slice(GetNode(0));
            var pages = new HashSet<long>();
            for (int i = 1; i < NumberOfEntries; i++)
            {
                var node = GetNode(i);
                var current = new Slice(node);

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
            Debug.Assert(SizeUsed >= size);
            return size;
        }

        public int CalcSizeLeft()
        {
            var sl = AbstractPager.PageMaxSpace - CalcSizeUsed();
            Debug.Assert(sl >= 0);
            return sl;
        }

        public void EnsureHasSpaceFor(Transaction tx, Slice key, int len)
        {
            if (HasSpaceFor(tx, key, len) == false)
                throw new InvalidOperationException("Could not ensure that we have enough space, this is probably a bug");
        }
    }
}
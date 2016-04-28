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

    public unsafe class Page
    {
        public const byte PrefixCount = 8;
        public const sbyte KeysPrefixingDisabled = -127;
        private readonly byte* _base;
        private readonly PageHeader* _header;
        private readonly PrefixInfoSection* _prefixSection;

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
            _prefixSection = (PrefixInfoSection*)(_base + _pageSize - Constants.PrefixInfoSectionSize);
        }

        
        public long PageNumber 
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get { return _header->PageNumber; }
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            set { _header->PageNumber = value; } 
        }

        public PageFlags Flags 
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get { return _header->Flags; }
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            set { _header->Flags = value; }
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

        public ushort PageSize 
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
        private NodeHeader* Search(Slice key)
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

                        ushort* keys = KeysOffsets;
                        while (low <= high)
                        {
                            position = (low + high) >> 1;

                            var node = (NodeHeader*)(_base + keys[position]);

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

        public List<long> GetAllOverflowPages()
        {
            var results = new List<long>(NumberOfEntries);
            for ( int i = 0; i < NumberOfEntries; i++ )
            {
                var nodeOffset = KeysOffsets[i];

                // We will only select the nodes that have a valid Page pointer.
                var nodeHeader = (NodeHeader*)(_base + nodeOffset);
                if ( nodeHeader->Flags == NodeFlags.PageRef)
                    results.Add(nodeHeader->PageNumber);
            }

            return results;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private NodeHeader* SearchPrefixed(MemorySlice key)
        {
            key.PrepareForSearching();

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
                        var pageKey = CreateNewEmptyKey();

                        if (numberOfEntries == 1)
                        {
                            var node = GetNode(0);

                            SetNodeKey(node, ref pageKey);
                            LastMatch = key.Compare(pageKey);
                            LastSearchPosition = LastMatch > 0 ? 1 : 0;
                            return LastSearchPosition == 0 ? node : null;
                        }

                        int low = IsLeaf ? 0 : 1;
                        int high = numberOfEntries - 1;
                        int position = 0;

                        ushort* keys = KeysOffsets;
                        while (low <= high)
                        {
                            position = (low + high) >> 1;

                            var node = (NodeHeader*)(_base + keys[position]);

                            SetNodeKey(node, ref pageKey);

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


        [MethodImpl(MethodImplOptions.NoInlining)]
        public NodeHeader* Search(MemorySlice key)
        {
            if (KeysPrefixed)
            {
                return SearchPrefixed(key);
            }
            else
            {
                return Search((Slice)key);
            }                
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public NodeHeader* GetNode(int n)
        {
            Debug.Assert(n >= 0 && n < NumberOfEntries);

            var nodeOffset = KeysOffsets[n];
            var nodeHeader = (NodeHeader*)(_base + nodeOffset);

            return nodeHeader;
        }

        [Conditional("DEBUG")]
        private void AssertPrefixNode(byte prefixId)
        {
            Debug.Assert(prefixId < PrefixCount, "Requested prefix number was: " + prefixId);
            Debug.Assert(prefixId <= (_prefixSection->NextPrefixId - 1), "Requested prefix number was: " + prefixId + ", while the max available prefix id is " + (_prefixSection->NextPrefixId - 1));
        }

        public bool IsLeaf
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get { return (_header->Flags & PageFlags.Leaf) == PageFlags.Leaf; }
        }

        public bool IsBranch
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get { return (_header->Flags & PageFlags.Branch) == PageFlags.Branch; }
        }

        public bool IsOverflow
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get { return (_header->Flags & PageFlags.Overflow) == PageFlags.Overflow; }
        }

        public bool KeysPrefixed
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get { return (_header->Flags & PageFlags.KeysPrefixed) == PageFlags.KeysPrefixed; }
        }

        public bool IsFixedSize
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get { return (_header->Flags & PageFlags.FixedSize) == PageFlags.FixedSize; }
        }

        public bool HasPrefixes
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get { return _prefixSection->NextPrefixId > 0; }
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

        public byte* AddPageRefNode(int index, MemorySlice key, long pageNumber)
        {
            var node = CreateNode(index, key, NodeFlags.PageRef, -1, 0);
            node->PageNumber = pageNumber;

            return null; // nothing to write into page ref node
        }

        public byte* AddDataNode(int index, MemorySlice key, int dataSize, ushort previousNodeVersion)
        {
            Debug.Assert(dataSize >= 0);
            Debug.Assert(key.Options == SliceOptions.Key);

            var node = CreateNode(index, key, NodeFlags.Data, dataSize, previousNodeVersion);
            node->DataSize = dataSize;

            return (byte*)node + Constants.NodeHeaderSize + key.Size;
        }

        public byte* AddMultiValueNode(int index, MemorySlice key, int dataSize, ushort previousNodeVersion)
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

        private NodeHeader* CreateNode(int index, MemorySlice key, NodeFlags flags, int len, ushort previousNodeVersion)
        {
            Debug.Assert(index <= NumberOfEntries && index >= 0);
            Debug.Assert(IsBranch == false || index != 0 || key.KeyLength == 0);// branch page's first item must be the implicit ref
            if (HasSpaceFor(key, len) == false)
                throw new InvalidOperationException(string.Format("The page is full and cannot add an entry, this is probably a bug. Key: {0}, data length: {1}, size left: {2}", key, len, SizeLeft));

            var prefixedKey = key as PrefixedSlice;
            if (prefixedKey != null && prefixedKey.NewPrefix != null)
                WritePrefix(prefixedKey.NewPrefix, prefixedKey.Header.PrefixId);

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
        internal void CopyNodeDataToEndOfPage(NodeHeader* other, MemorySlice key)
        {
            var index = NumberOfEntries;

            Debug.Assert(HasSpaceFor(SizeOf.NodeEntryWithAnotherKey(other, key) + Constants.NodeOffsetSize + SizeOf.NewPrefix(key)));

            var nodeSize = SizeOf.NodeEntryWithAnotherKey(other, key);

            Debug.Assert(IsBranch == false || index != 0 || key.KeyLength == 0);// branch page's first item must be the implicit ref

            var nodeVersion = other->Version; // every time new node is allocated the version is increased, but in this case we do not want to increase it
            if (nodeVersion > 0)
                nodeVersion -= 1;

            var prefixedKey = key as PrefixedSlice;
            if (prefixedKey != null && prefixedKey.NewPrefix != null)
                WritePrefix(prefixedKey.NewPrefix, prefixedKey.Header.PrefixId);

            var newNode = AllocateNewNode(index, nodeSize, nodeVersion);

            newNode->KeySize = key.Size;
            newNode->Flags = other->Flags;

            if (key.Options == SliceOptions.Key && key.Size > 0)
                key.CopyTo((byte*)newNode + Constants.NodeHeaderSize);

            if (IsBranch || other->Flags == (NodeFlags.PageRef))
            {
                newNode->PageNumber = other->PageNumber;
                newNode->Flags = NodeFlags.PageRef;
                return;
            }
            newNode->DataSize = other->DataSize;
            Memory.Copy((byte*)newNode + Constants.NodeHeaderSize + key.Size,
                                 (byte*)other + Constants.NodeHeaderSize + other->KeySize,
                                 other->DataSize);
        }

        public MemorySlice PrepareKeyToInsert(MemorySlice key, int nodeIndex)
        {
            if (KeysPrefixed == false)
                return key;

            if (key.KeyLength == 0)
                return PrefixedSlice.Empty;

            PrefixedSlice prefixedSlice;

            if (TryUseExistingPrefix(key, out prefixedSlice))
                return prefixedSlice;

            if (TryCreateNewPrefix(key, nodeIndex, out prefixedSlice))
                return prefixedSlice;

            return new PrefixedSlice(key);
        }

        private class BestPrefixMatch
        {
            public byte PrefixId;
            public ushort PrefixUsage;
            public PrefixNode PrefixNode;
        }

        public PrefixNode[] GetPrefixes()
        {
            var prefixes = new PrefixNode[_prefixSection->NextPrefixId];

            for (byte prefixId = 0; prefixId < _prefixSection->NextPrefixId; prefixId++)
            {
                var prefix = new PrefixNode();
                prefix.Set(_base + _prefixSection->PrefixOffsets[prefixId], PageNumber);
                prefixes[prefixId] = prefix;
            }

            return prefixes;
        }

        private bool TryUseExistingPrefix(MemorySlice key, out PrefixedSlice prefixedSlice)
        {
            if (_prefixSection->NextPrefixId < 1)
            {
                prefixedSlice = null;
                return false;
            }

            BestPrefixMatch bestMatch = null;

            for (byte prefixId = 0; prefixId < _prefixSection->NextPrefixId; prefixId++)
            {
                AssertPrefixNode(prefixId);

                var prefix = new PrefixNode();

                prefix.Set(_base + _prefixSection->PrefixOffsets[prefixId], PageNumber);

                var length = key.FindPrefixSize(new Slice(prefix.ValuePtr, prefix.PrefixLength));
                if (length == 0)
                    continue;

                if (length == prefix.PrefixLength) // full prefix usage
                {
                    prefixedSlice = new PrefixedSlice(prefixId, length, key.Skip(length))
                    {
                        Prefix = prefix
                    };
                    return true;
                }

                // keep on looking for a better prefix

                if (bestMatch == null)
                {
                    bestMatch = new BestPrefixMatch
                    {
                        PrefixId = prefixId,
                        PrefixUsage = length,
                        PrefixNode = prefix
                    };
                }
                else if (length > bestMatch.PrefixUsage)
                {
                    bestMatch.PrefixId = prefixId;
                    bestMatch.PrefixUsage = length;
                    bestMatch.PrefixNode = prefix;
                }
            }

            if (bestMatch != null && bestMatch.PrefixUsage > MinPrefixLength(key))
            {
                prefixedSlice = new PrefixedSlice(bestMatch.PrefixId, bestMatch.PrefixUsage, key.Skip(bestMatch.PrefixUsage))
                {
                    Prefix = bestMatch.PrefixNode
                };
                return true;
            }

            prefixedSlice = null;
            return false;
        }

        private bool TryCreateNewPrefix(MemorySlice key, int nodeIndex, out PrefixedSlice prefixedSlice)
        {
            if (_prefixSection->NextPrefixId >= PrefixCount || NumberOfEntries == 0)
            {
                prefixedSlice = null;
                return false;
            }

            MemorySlice left;
            MemorySlice right;

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
                prefixedSlice = new PrefixedSlice(_prefixSection->NextPrefixId, leftLength, key.Skip(leftLength))
                {
                    NewPrefix = new Slice(left.ToSlice(), leftLength)
                };

                return true;
            }

            if (right != null && rightLength > minPrefixLength && rightLength > leftLength)
            {
                prefixedSlice = new PrefixedSlice(_prefixSection->NextPrefixId, rightLength, key.Skip(rightLength))
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

        public void WritePrefix(Slice prefix, int prefixId)
        {
            var prefixNodeSize = Constants.PrefixNodeHeaderSize + prefix.Size;
            prefixNodeSize += prefixNodeSize & 1;

            var prefixNodeOffset = (ushort)(Upper - prefixNodeSize);
            Upper = prefixNodeOffset;

            Debug.Assert(_prefixSection->NextPrefixId == prefixId);

            if (_prefixSection->PrefixOffsets[prefixId] != 0)
                throw new InvalidOperationException(string.Format("Cannot write a prefix '{0}' at the following offset position: {1} because it's already taken by another prefix. The offset for the prefix {1} is {2}. ", prefix, prefixId, _prefixSection->PrefixOffsets[prefixId]));

            _prefixSection->PrefixOffsets[prefixId] = prefixNodeOffset;

            var prefixNodeHeader = (PrefixNodeHeader*)(_base + prefixNodeOffset);

            prefixNodeHeader->PrefixLength = prefix.Size;

            prefix.CopyTo((byte*)prefixNodeHeader + Constants.PrefixNodeHeaderSize);

            _prefixSection->NextPrefixId++;
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
                var copy = tmp.GetTempPage(KeysPrefixed);
                copy.Flags = Flags;

                copy.ClearPrefixInfo();

                var slice = CreateNewEmptyKey();

                if (KeysPrefixed && HasPrefixes)
                {
                    var prefixes = GetPrefixes();

                    for (int prefixId = 0; prefixId < prefixes.Length; prefixId++)
                    {
                        var prefix = prefixes[prefixId];

                        copy.WritePrefix(new Slice(prefix.ValuePtr, prefix.PrefixLength), prefixId);
                    }
                }

                for (int j = 0; j < i; j++)
                {
                    var node = GetNode(j);
                    SetNodeKey(node, ref slice);
                    copy.CopyNodeDataToEndOfPage(node, copy.PrepareKeyToInsert(slice, copy.NumberOfEntries));
                }

                Memory.Copy(_base + Constants.PageHeaderSize,
                                     copy._base + Constants.PageHeaderSize,
                                     _pageSize - Constants.PageHeaderSize);

                if (KeysPrefixed)
                {
                    ClearPrefixInfo();
                    _prefixSection->NextPrefixId = copy._prefixSection->NextPrefixId;

                    for (var prefixId = 0; prefixId < _prefixSection->NextPrefixId; prefixId++)
                    {
                        _prefixSection->PrefixOffsets[prefixId] = copy._prefixSection->PrefixOffsets[prefixId];
                    }
                }

                Upper = copy.Upper;
                Lower = copy.Lower;
            }

            if (LastSearchPosition > i)
                LastSearchPosition = i;
        }

        public void ClearPrefixInfo()
        {
            if (KeysPrefixed == false)
                return;

            UnmanagedMemory.Set((byte*)_prefixSection->PrefixOffsets, 0, sizeof(ushort) * PrefixCount);
            _prefixSection->NextPrefixId = 0;
        }

        public int NodePositionFor(MemorySlice key)
        {
            Search(key);
            return LastSearchPosition;
        }

        public override string ToString()
        {
            if ((Flags & PageFlags.FixedSize)==PageFlags.FixedSize)
                return "#" + PageNumber + " (count: " + FixedSize_NumberOfEntries + ") " + Flags;
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
                var tempPage = tmp.GetTempPage(KeysPrefixed);
                Memory.Copy(tempPage.Base, Base, _pageSize);

                var numberOfEntries = NumberOfEntries;

                Upper = KeysPrefixed ? (ushort)(_pageSize - Constants.PrefixInfoSectionSize) : _pageSize;

                for (int i = 0; i < numberOfEntries; i++)
                {
                    var node = tempPage.GetNode(i);
                    var size = node->GetNodeSize() - Constants.NodeOffsetSize;
                    size += size & 1;
                    Memory.Copy(Base + Upper - size, (byte*)node, size);
                    Upper -= (ushort)size;
                    KeysOffsets[i] = Upper;
                }

                if (KeysPrefixed == false)
                    return;
                
                var prefixNode = new PrefixNode();

                for (byte i = 0; i < _prefixSection->NextPrefixId; i++)
                {
                    tempPage.AssertPrefixNode(i);

                    prefixNode.Set(tempPage._base + tempPage._prefixSection->PrefixOffsets[i], tempPage.PageNumber);

                    var prefixNodeSize = Constants.PrefixNodeHeaderSize + prefixNode.PrefixLength;
                    prefixNodeSize += prefixNodeSize & 1;

                    Memory.Copy(Base + Upper - prefixNodeSize, prefixNode.Base, prefixNodeSize);
                    Upper -= (ushort)prefixNodeSize;
                    _prefixSection->PrefixOffsets[i] = Upper;
                }
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private bool HasSpaceFor(int len)
        {
            return len <= SizeLeft;
        }

        public bool HasSpaceFor(Transaction tx, MemorySlice key, int len)
        {
            var requiredSpace = GetRequiredSpace(key, len);
            return HasSpaceFor(tx, requiredSpace);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private bool HasSpaceFor(MemorySlice key, int len)
        {
            return HasSpaceFor(GetRequiredSpace(key, len));
        }

         [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int GetRequiredSpace(MemorySlice key, int len)
        {
            return SizeOf.NodeEntry(PageMaxSpace, key, len) + Constants.NodeOffsetSize + SizeOf.NewPrefix(key);
        }

        public int PageMaxSpace
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get { return _pageSize - Constants.PageHeaderSize; }
        }

        public string this[int i]
        {
            get { return GetNodeKey(i).ToString(); }
        }


        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void SetNodeKey(NodeHeader* node, ref Slice slice)
        {
            Slice.SetInline(slice, node);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void SetNodeKey(NodeHeader* node, ref PrefixedSlice slice)
        {
            if (node->KeySize == 0)
            {
                slice = PrefixedSlice.Empty;
                return;
            }

            if (slice != null && slice != PrefixedSlice.Empty)
            {
                slice.Set(node);
            }
            else
            {
                slice = new PrefixedSlice(node);
            }

            if (slice.Header.PrefixId == PrefixedSlice.NonPrefixedId)
            {
                Debug.Assert(slice.Header.PrefixUsage == 0);

                return;
            }

            Debug.Assert(slice.Header.PrefixId < PrefixCount);

            if (slice.Prefix == null)
                slice.Prefix = new PrefixNode();

            AssertPrefixNode(slice.Header.PrefixId);

            slice.Prefix.Set(_base + _prefixSection->PrefixOffsets[slice.Header.PrefixId], PageNumber);
        }

        // REVIEW: Removed forced inlining for now until we can see if we improve without needing it. 
        // [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void SetNodeKey(NodeHeader* node, ref MemorySlice sliceInstance)
        {
            if (KeysPrefixed)
            {
                var slice = (PrefixedSlice)sliceInstance;
                SetNodeKey(node, ref slice);
                sliceInstance = slice;
            }                
            else
            {
                Slice slice = (Slice)sliceInstance;
                SetNodeKey(node, ref slice);
                sliceInstance = slice;
            }
        }

        public MemorySlice GetNodeKey(int nodeNumber)
        {
            var node = GetNode(nodeNumber);

            return GetNodeKey(node);
        }

        public MemorySlice GetNodeKey(NodeHeader* node)
        {
            if (KeysPrefixed == false)
            {
                var keySize = node->KeySize;
                var key = new byte[keySize];

                fixed (byte* ptr = key)
                    Memory.CopyInline(ptr, (byte*)node + Constants.NodeHeaderSize, keySize);

                return new Slice(key);
            }

            if (node->KeySize == 0)
                return new PrefixedSlice(Slice.Empty);

            var prefixHeader = (PrefixedSliceHeader*)((byte*)node + Constants.NodeHeaderSize);

            var nonPrefixedSize = prefixHeader->NonPrefixedDataSize;
            var nonPrefixedData = new byte[nonPrefixedSize];

            fixed (byte* ptr = nonPrefixedData)
                Memory.CopyInline(ptr, (byte*)prefixHeader + Constants.PrefixedSliceHeaderSize, nonPrefixedSize);

            var prefixedSlice = new PrefixedSlice(prefixHeader->PrefixId, prefixHeader->PrefixUsage, new Slice(nonPrefixedData));

            if (prefixHeader->PrefixId == PrefixedSlice.NonPrefixedId)
                return prefixedSlice;

            AssertPrefixNode(prefixedSlice.Header.PrefixId);

            var prefixNodePtr = (PrefixNodeHeader*)(_base + _prefixSection->PrefixOffsets[prefixedSlice.Header.PrefixId]);

            var prefixLength = prefixNodePtr->PrefixLength;
            var prefixData = new byte[prefixLength];

            fixed (byte* ptr = prefixData)
                Memory.CopyInline(ptr, (byte*)prefixNodePtr + Constants.PrefixNodeHeaderSize, prefixLength);

            prefixedSlice.Prefix = new PrefixNode(new PrefixNodeHeader { PrefixLength = prefixLength }, prefixData, PageNumber);

            return prefixedSlice;
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
                    DebugStuff.RenderAndShow(tx, root);
                    throw new InvalidOperationException("The page " + PageNumber + " is not sorted");
                }

                if (node->Flags == (NodeFlags.PageRef))
                {
                    if (pages.Add(node->PageNumber) == false)
                    {
                        DebugStuff.RenderAndShow(tx, root);
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

            if (KeysPrefixed)
            {
                PrefixNode prefixNode = null;

                for (byte i = 0; i < _prefixSection->NextPrefixId; i++)
                {
                    if (prefixNode == null)
                        prefixNode = new PrefixNode();

                    AssertPrefixNode(i);

                    prefixNode.Set(_base + _prefixSection->PrefixOffsets[i], PageNumber);

                    var prefixNodeSize = Constants.PrefixNodeHeaderSize + prefixNode.PrefixLength;
                    size += prefixNodeSize + (prefixNodeSize & 1);
                }

                size += Constants.PrefixInfoSectionSize;
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

        public void EnsureHasSpaceFor(Transaction tx, MemorySlice key, int len)
        {
            if (HasSpaceFor(tx, key, len) == false)
                throw new InvalidOperationException("Could not ensure that we have enough space, this is probably a bug");
        }

        private static int MinPrefixLength(MemorySlice key)
        {
            return Math.Max(key.KeyLength / 5, 2);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public MemorySlice CreateNewEmptyKey()
        {
            if (KeysPrefixed)
                return new PrefixedSlice(SliceOptions.Key);

            return new Slice(SliceOptions.Key);
        }
    }
}

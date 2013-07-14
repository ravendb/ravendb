using System;
using System.Diagnostics;
using System.IO;
using System.Text;

namespace Nevar
{
	public unsafe class Page
	{
		private readonly byte* _base;
		private readonly PageHeader* _header;
		public ushort LastSearchPosition;

		public Page(byte* b)
		{
			_base = b;
			_header = (PageHeader*)b;
		}

		public int PageNumber { get { return _header->PageNumber; } set { _header->PageNumber = value; } }

		public PageFlags Flags { get { return _header->Flags; } set { _header->Flags = value; } }

		public ushort Lower { get { return _header->Lower; } set { _header->Lower = value; } }

		public ushort Upper { get { return _header->Upper; } set { _header->Upper = value; } }

		public int NumberOfPages { get { return _header->NumberOfPages; } set { _header->NumberOfPages = value; } }

		public ushort* KeysOffsets
		{
			get { return (ushort*)(_base + Constants.PageHeaderSize); }
		}

		public NodeHeader* Search(Slice key, SliceComparer cmp, out int match)
		{
			int low = IsLeaf ? 0 : 1; // leaf pages entries start at 0, but branch entries 0th entry is the implicit left page
			int high = NumberOfEntries - 1;
			int position = 0;
			match = 0;

			var pageKey = new Slice(SliceOptions.Key);

			NodeHeader* node = null;
			while (low <= high)
			{
				position = (low + high) >> 1;

				node = GetNode(position);
				pageKey.Set(node);

				match = key.Compare(pageKey, cmp);
				if (match == 0)
					break;

				if (match > 0)
					low = position + 1;
				else
					high = position - 1;
			}


			if (match > 0) // found entry less than key
				position++; // move to the smallest entry larger than the key


			Debug.Assert(position < ushort.MaxValue);
			LastSearchPosition = (ushort)position;

			if (position >= NumberOfEntries)
				return null;
			return node;
		}

		public NodeHeader* GetNode(int n)
		{
			Debug.Assert(n >= 0 && n <= NumberOfEntries);

			var nodeOffset = KeysOffsets[n];
			var nodeHeader = (NodeHeader*)(_base + nodeOffset);

			return nodeHeader;
		}


		public bool IsLeaf
		{
			get { return _header->Flags.HasFlag(PageFlags.Leaf); }
		}

		public bool IsBranch
		{
			get { return _header->Flags.HasFlag(PageFlags.Branch); }
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

		internal void AddNode(ushort index, Slice key, Stream value = null, int pageNumber = -1)
		{
			if (HasSpaceFor(key, value) == false)
				throw new InvalidOperationException("The page is full and cannot add an entry, this is probably a bug");

			// move higher pointers up one slot
			for (int i = NumberOfEntries; i > index; i--)
			{
				KeysOffsets[i] = KeysOffsets[i - 1];
			}
			var nodeSize = Util.GetNodeSize(key, value);
			var node = AllocateNewNode(index, key, nodeSize);

			if (key.Options == SliceOptions.Key)
				key.CopyTo((byte*)node + Constants.NodeHeaderSize);

			if (IsBranch)
			{
				if (pageNumber == -1)
					throw new ArgumentException("Page numbers must be positive", "pageNumber");
				node->PageNumber = pageNumber;
				node->Flags = NodeFlags.PageRef;
				return;
			}

			Debug.Assert(key.Options == SliceOptions.Key);
			Debug.Assert(value != null);
			var dataPos = (byte*)node + Constants.NodeHeaderSize + key.Size;
			node->DataSize = (int)value.Length;
			using (var ums = new UnmanagedMemoryStream(dataPos, value.Length, value.Length, FileAccess.ReadWrite))
			{
				value.CopyTo(ums);
			}
		}

		/// <summary>
		/// Internal method that is used when splitting pages
		/// No need to do any work here, we are always adding at the end
		/// </summary>
		internal void CopyNodeData(NodeHeader* other)
		{
			Debug.Assert(Util.NodeEntrySize(other) <= SizeLeft);

			var index = NumberOfEntries;

			var nodeSize = Constants.NodeHeaderSize + other->KeySize;
			var isBranch = other->Flags.HasFlag(NodeFlags.PageRef);
			if (isBranch == false)
				nodeSize += other->DataSize;
			var key = new Slice(other);
			var newNode = AllocateNewNode(index, key, nodeSize);
			key.CopyTo((byte*) newNode + Constants.NodeHeaderSize);

			if (IsBranch)
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

		private NodeHeader* AllocateNewNode(ushort index, Slice key, int nodeSize)
		{
			var newNodeOffset = (ushort)(_header->Upper - nodeSize);
			Debug.Assert(newNodeOffset >= _header->Lower + Constants.NodeOffsetSize);
			KeysOffsets[index] = newNodeOffset;
			_header->Upper = newNodeOffset;
			_header->Lower += (ushort)Constants.NodeOffsetSize;

			var node = (NodeHeader*)(_base + newNodeOffset);
			node->KeySize = key.Size;
			node->Flags = NodeFlags.None;
			return node;
		}


		public int SizeLeft
		{
			get { return _header->Upper - _header->Lower; }
		}

		public void Truncate(ushort i)
		{
			if (i >= NumberOfEntries)
				return;

			Upper = KeysOffsets[i];
			Lower = (ushort)(Constants.PageHeaderSize + Constants.NodeOffsetSize * i);

			if (LastSearchPosition > i)
				LastSearchPosition = i;
		}

		public ushort NodePositionFor(Slice key, SliceComparer cmp)
		{
			int match;
			Search(key, cmp, out match);
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
				var pageRef = n->Flags.HasFlag(NodeFlags.PageRef);
				sb.AppendFormat("Node: {0,-5}", i).Append("\tKey: ").Append(slice);
				if (pageRef == false)
				{
					sb.Append("\tSize: ").Append(n->GetNodeSize());
				}
				else
				{
					sb.Append("\tPage: ").Append(n->PageNumber);
				}
				 sb.AppendLine();
			}
			return sb.ToString();
		}

		public bool HasSpaceFor(Slice key, Stream value)
		{
			var requiredSpace = Util.GetNodeSize(key, value) + Constants.NodeOffsetSize;
			return requiredSpace < SizeLeft;
		}
	}
}
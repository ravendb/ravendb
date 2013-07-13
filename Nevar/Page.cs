using System;
using System.Diagnostics;
using System.IO;
using System.Runtime.InteropServices;

namespace Nevar
{
	[StructLayout(LayoutKind.Explicit, Pack = 1)]
	public unsafe struct Page
	{
		[FieldOffset(0)]
		public byte* Base;
		[FieldOffset(0)]
		public PageHeader* Header;

		[FieldOffset(8)]
		public ushort LastSearchPosition; // note that this is _not_ persisted!

		public ushort* KeysOffsets
		{
			get { return (ushort*)(Base + Constants.PageHeaderSize); }
		}

		public NodeHeader* Search(Slice key, SliceComparer cmp, out bool exactMatch)
		{
			int low = IsLeaf ? 0 : 1; // leaf pages entries start at 0, but branch entries 0th entry is the implicit left page
			int high = NumberOfEntries - 1;
			int position = 0;
			int cmpResult = 0;

			var pageKey = new Slice();

			NodeHeader* node = null;
			while (low <= high)
			{
				position = (low + high) >> 1;

				node = GetNode(position);
				pageKey.Set((byte*)node + Constants.NodeHeaderSize, node->KeySize);

				cmpResult = key.Compare(pageKey, cmp);
				if (cmpResult == 0)
					break;

				if (cmpResult > 0)
					low = position + 1;
				else
					high = position - 1;
			}


			if (cmpResult > 0) // found entry less than key
				position++; // move to the smallest entry larger than the key


			exactMatch = cmpResult == 0;
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
			return (NodeHeader*)(Base + nodeOffset);
		}

		public bool IsLeaf
		{
			get { return Header->Flags.HasFlag(PageFlags.Leaf); }
		}

		public bool IsBranch
		{
			get { return Header->Flags.HasFlag(PageFlags.Branch); }
		}

		public ushort NumberOfEntries
		{
			get
			{
				// Because we store the keys offset from the end of the head to lower
				// we can calculate the number of entries by getting the size and dividing
				// in 2, since that is the size of the offsets we use

				return (ushort)((Header->Lower - Constants.PageHeaderSize) >> 1);
			}
		}

		internal void AddNode(ushort index, Slice key, Stream value, int pageNumber)
		{
			var nodeSize = Constants.NodeHeaderSize;

			if (key.Options == SliceOptions.Key)
				nodeSize += key.Size;

			nodeSize += nodeSize & 1; // align on 2 boundary

			if (nodeSize + sizeof(ushort) > SizeLeft)
				throw new InvalidOperationException("The page is full and cannot add an entry with size: " + nodeSize +
													", this is probably a bug");

			// move higher pointers up one slot
			for (int i = NumberOfEntries; i > index; i--)
			{
				KeysOffsets[i] = KeysOffsets[i - 1];
			}

			var newNodeOffset = (ushort)(Header->Upper - nodeSize);
			Debug.Assert(newNodeOffset >= Header->Lower + sizeof(ushort));
			KeysOffsets[index] = newNodeOffset;
			Header->Upper = newNodeOffset;
			Header->Lower += sizeof(ushort);

			var node = (NodeHeader*)(Base + newNodeOffset);
			node->KeySize = key.Size;
			node->Flags = NodeFlags.None;

			if (key.Options == SliceOptions.Key)
				key.CopyTo((byte*)node + Constants.NodeHeaderSize);

			if (IsBranch)
			{
				node->PageNumber = pageNumber;
				return;
			}

			node->DataSize = (int)value.Length;
			Debug.Assert(key.Options == SliceOptions.Key);

			using (var ums = new UnmanagedMemoryStream((byte*)node + Constants.NodeHeaderSize + key.Size, value.Length, value.Length, FileAccess.ReadWrite))
			{
				value.CopyTo(ums);
			}
		}

		public int SizeLeft
		{
			get { return Header->Upper - Header->Lower; }
		}
	}

	[StructLayout(LayoutKind.Explicit, Pack = 1)]
	public unsafe struct PageHeader
	{
		[FieldOffset(0)]
		public int PageNumber;
		[FieldOffset(4)]
		public PageFlags Flags;

		[FieldOffset(5)]
		public ushort Lower;
		[FieldOffset(7)]
		public ushort Upper;

		[FieldOffset(5)]
		public int NumberOfPages;
	}

	[StructLayout(LayoutKind.Explicit, Pack = 1)]
	public struct NodeHeader
	{
		[FieldOffset(0)]
		public int DataSize;
		[FieldOffset(0)]
		public int PageNumber;

		[FieldOffset(4)]
		public NodeFlags Flags;

		[FieldOffset(6)]
		public ushort KeySize;
	}

	[Flags]
	public enum NodeFlags : byte
	{
		None = 0,
		Overflow = 1,
	}
}
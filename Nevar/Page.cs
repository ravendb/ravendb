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
			return (NodeHeader*)(_base + nodeOffset);
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

		internal void AddNode(ushort index, Slice key, Stream value = null, int pageNumber = -1, NodeHeader* other = null)
		{
			var leafNodeSize = Util.GetLeafNodeSize(key, value, other);
			if (leafNodeSize > SizeLeft)
				throw new InvalidOperationException("The page is full and cannot add an entry with min size of: " + leafNodeSize +
													", this is probably a bug");

			// move higher pointers up one slot
			for (int i = NumberOfEntries; i > index; i--)
			{
				KeysOffsets[i] = KeysOffsets[i - 1];
			}
			var nodeSize = Util.GetRequiredSpace(key, value, other);
			var newNodeOffset = (ushort)(_header->Upper - nodeSize);
			Debug.Assert(newNodeOffset >= _header->Lower + Constants.NodeOffsetSize);
			KeysOffsets[index] = newNodeOffset;
			_header->Upper = newNodeOffset;
			_header->Lower += (ushort)Constants.NodeOffsetSize;

			var node = (NodeHeader*)(_base + newNodeOffset);
			node->KeySize = key.Size;
			node->Flags = NodeFlags.None;

			if (key.Options == SliceOptions.Key)
				key.CopyTo((byte*)node + Constants.NodeHeaderSize);

			if (IsBranch)
			{
				node->PageNumber = pageNumber;
				return;
			}

			Debug.Assert(key.Options == SliceOptions.Key);
			var dataPos = (byte*)node + Constants.NodeHeaderSize + key.Size;
			if (value != null)
			{
				node->DataSize = (int)value.Length;
				using (var ums = new UnmanagedMemoryStream(dataPos, value.Length, value.Length, FileAccess.ReadWrite))
				{
					value.CopyTo(ums);
				}
			}
			else if (other != null)
			{
				node->DataSize = other->DataSize;
				NativeMethods.memcpy(dataPos, ((byte*)other) + Constants.NodeHeaderSize + other->KeySize, other->DataSize);
			}
			else
			{
				throw new ArgumentException("Adding a node to a leaf node requires either a value or a node header to clone from");
			}
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
			if (Search(key, cmp, out match) == null)
				return (ushort) (NumberOfEntries - 1);
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
				sb.AppendFormat("Node: {0,-5}", i).Append("\tKey: ").Append(slice)
				  .Append("\tSize: ")
				  .Append(n->GetNodeSize())
				  .AppendLine();
			}
			return sb.ToString();
		}
	}
}
using System;
using System.Collections.Generic;
using Voron.Trees;

namespace Voron.Impl
{
	internal unsafe class SizeOf
	{
		/// <summary>
		/// Calculate the size of a leaf node.
		/// The size depends on the environment's page size; if a data item
		/// is too large it will be put onto an overflow page and the node
		/// size will only include the key and not the data. Sizes are always
		/// rounded up to an even number of bytes, to guarantee 2-byte alignment
		/// </summary>
		public static int LeafEntry(int pageMaxSpace, MemorySlice key, int len)
		{
			var nodeSize = Constants.NodeHeaderSize;

			if (key.Options == SliceOptions.Key)
				nodeSize += key.Size;
			if (len != 0)
			{
				nodeSize += len;

				if (nodeSize > pageMaxSpace)
					nodeSize -= len - Constants.PageNumberSize;
			}
			// else - page ref node, take no additional space

			nodeSize += nodeSize & 1;

			return nodeSize;
		}


		public static int BranchEntry(MemorySlice key)
		{
			var sz = Constants.NodeHeaderSize + key.Size;
			sz += sz & 1;
			return sz;
		}

		public static int NodeEntry(int pageMaxSpace, MemorySlice key, int len)
		{
			if (len < 0)
				return BranchEntry(key);
			return LeafEntry(pageMaxSpace, key, len);
		}

		public static int NodeEntry(NodeHeader* other)
		{
			var sz = other->KeySize + Constants.NodeHeaderSize;
			if (other->Flags == NodeFlags.Data || other->Flags == NodeFlags.MultiValuePageRef)
				sz += other->DataSize;

			sz += sz & 1;

			return sz;
		}

        public static int NodeEntryWithAnotherKey(NodeHeader* other, MemorySlice key)
        {
            var keySize = key == null ? other->KeySize : key.Size;
            var sz = keySize + Constants.NodeHeaderSize;
            if (other->Flags == NodeFlags.Data || other->Flags == NodeFlags.MultiValuePageRef)
                sz += other->DataSize;

            sz += sz & 1;

            return sz;
        }

		public static int NewPrefix(MemorySlice key)
		{
			var prefixedKey = key as PrefixedSlice;

			if (prefixedKey != null && prefixedKey.NewPrefix != null) // also need to take into account the size of a new prefix that will be written to the page
			{
				var size = Constants.PrefixNodeHeaderSize + prefixedKey.NewPrefix.Size;
				size += size & 1;
				
				return size;
			}

			return 0;
		}

		private static readonly Dictionary<Type, int> _primitiveSizes = new Dictionary<Type, int>
		{
			{typeof (sbyte), sizeof (sbyte)},
			{typeof (byte), sizeof (byte)},
			{typeof (short), sizeof (short)},
			{typeof (ushort), sizeof (ushort)},
			{typeof (int), sizeof (int)},
			{typeof (uint), sizeof (uint)},
			{typeof (long), sizeof (long)},
			{typeof (ulong), sizeof (ulong)},
			{typeof (char), sizeof (char)},
			{typeof (float), sizeof (float)},
			{typeof (double), sizeof (double)},
			{typeof (decimal), sizeof (decimal)},
			{typeof (bool), sizeof (bool)},
		};

		public static int Primitive(Type type)
		{
			return _primitiveSizes[type];
		}
	}
}
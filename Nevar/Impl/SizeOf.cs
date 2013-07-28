using System.IO;
using Nevar.Trees;

namespace Nevar.Impl
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
		public static int LeafEntry(Slice key, int len)
		{
			var nodeSize = Constants.NodeHeaderSize;

			if (key.Options == SliceOptions.Key)
				nodeSize += key.Size;
			if (len != 0)
			{
				nodeSize += len;

				if (nodeSize > Constants.PageMaxSpace)
					nodeSize -= len - Constants.PageNumberSize;
			}
			// else - page ref node, take no additional space
			
			nodeSize += nodeSize & 1;

			return nodeSize;
		}


		public static int BranchEntry(Slice key)
		{
			var sz = Constants.NodeHeaderSize + key.Size;
			sz += sz & 1;
			return sz;
		}

		public static int NodeEntry(Slice key, int len)
		{
			if (len < 0)
				return BranchEntry(key);
			return LeafEntry(key, len);
		}

		public static int NodeEntry(NodeHeader* other)
		{
			var sz = other->KeySize + Constants.NodeHeaderSize;
			if (other->Flags.HasFlag(NodeFlags.Data))
				sz += other->DataSize;

			sz += sz & 1;

			return sz;
		}
	}
}
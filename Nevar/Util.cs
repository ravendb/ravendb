using System.IO;

namespace Nevar
{
	internal unsafe class Util
	{
		/// <summary>
		/// Calculate the size of a leaf node.
		/// The size depends on the environment's page size; if a data item
		/// is too large it will be put onto an overflow page and the node
		/// size will only include the key and not the data. Sizes are always
		/// rounded up to an even number of bytes, to guarantee 2-byte alignment
		/// </summary>
		public static int GetLeafNodeSize(Slice key, Stream value, NodeHeader* other = null)
		{
			var nodeSize = Constants.NodeHeaderSize;

			if (key.Options == SliceOptions.Key)
				nodeSize += key.Size;
			if (value != null)
			{
				nodeSize += (int) value.Length;

				if (nodeSize > Constants.PageMaxSpace)
					nodeSize -= (int) value.Length - Constants.PageNumberSize;
			}
			else if (other != null)
			{
				// we already know it can fit in a page (it comes from a page), no need to check again
				nodeSize += other->DataSize;
			}
			// else - page ref node, take no additional space
			
			nodeSize += nodeSize & 1;

			return nodeSize + Constants.NodeOffsetSize;
		}

		public static int GetRequiredSpace(Slice key, Stream value, NodeHeader* other)
		{
			var nodeSize = Constants.NodeHeaderSize + key.Size;
			if (value != null)
			{
				nodeSize += (int)value.Length;
			}
			else if (other != null)
			{
				nodeSize += other->DataSize;
			}
			return nodeSize;
		}


		/// <summary>
		///  Calculate the size of a branch node.
		/// The size should depend on the environment's page size but since
		/// we currently don't support spilling large keys onto overflow
		/// pages, it's simply the size of the #MDB_node header plus the
		/// size of the key. Sizes are always rounded up to an even number
		/// of bytes, to guarantee 2-byte alignment of the #MDB_node headers.
		/// </summary>
		public static int GetBranchSize(Slice key)
		{
			return key.Size + Constants.NodeOffsetSize;
		}
	}
}
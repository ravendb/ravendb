using System.IO;

namespace Nevar
{
	internal class Util
	{
		public static int GetMinNodeSize(Slice key)
		{
			var nodeSize = Constants.NodeHeaderSize;

			if (key.Options == SliceOptions.Key)
				nodeSize += key.Size;

			nodeSize += nodeSize & 1; // align on 2 boundary
			return nodeSize + Constants.NodeOffsetSize;
		}

		/// <summary>
		/// Calculate the size of a leaf node.
		/// The size depends on the environment's page size; if a data item
		/// is too large it will be put onto an overflow page and the node
		/// size will only include the key and not the data. Sizes are always
		/// rounded up to an even number of bytes, to guarantee 2-byte alignment
		/// </summary>
		public static int GetLeafNodeSize(Slice key, Stream value)
		{
			var nodeSize = Constants.NodeHeaderSize;

			if (key.Options == SliceOptions.Key)
				nodeSize += key.Size;
			nodeSize += (int) value.Length;

			if (nodeSize > Constants.PageMaxSpace)
				nodeSize -= (int) value.Length - Constants.PageNumberSize;

			nodeSize += nodeSize & 1;

			return nodeSize + Constants.NodeOffsetSize;
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
namespace Raven.Storage.Managed.Data
{
	public class StreamPosition
	{
		public long? Position;
		public TreeNode Node;

		public StreamPosition(TreeNode node)
		{
			Node = node;
		}

		public StreamPosition(long position, TreeNode node)
		{
			Position = position;
			Node = node;
		}
	}
}
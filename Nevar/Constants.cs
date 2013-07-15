using System.Runtime.InteropServices;

namespace Nevar
{
	public unsafe class Constants
	{
		public const int PageSize = 128;

		/// <summary>
		/// If there are less than 2 keys in a page, we no longer have a tree
		/// This impacts the MakKeySize available
		/// </summary>
		public const int MinKeysInPage = 2;

		public static readonly int PageHeaderSize = sizeof(PageHeader);

		public static readonly int NodeHeaderSize = sizeof(NodeHeader);

		public static readonly int MaxKeySize = (PageSize - PageHeaderSize) / MinKeysInPage;

		public static int PageMaxSpace = PageSize - PageHeaderSize;

		public static int PageNumberSize = sizeof(ushort);

		public static int NodeOffsetSize = sizeof(ushort);
	}
}
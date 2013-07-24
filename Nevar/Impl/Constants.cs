using Nevar.Trees;

namespace Nevar.Impl
{
	public unsafe class Constants
	{
        public const ulong MagicMarker = 0xB16BAADC0DEF0015; 

		public const int PageSize = 4096;

		/// <summary>
		/// If there are less than 2 keys in a page, we no longer have a tree
		/// This impacts the MakKeySize available
		/// </summary>
		public const int MinKeysInPage = 2;

		public static readonly int PageHeaderSize = sizeof(PageHeader);

		public static readonly int NodeHeaderSize = sizeof(NodeHeader);

		public static readonly int MaxNodeSize = (PageSize - PageHeaderSize) / MinKeysInPage;

		public static int PageMaxSpace = PageSize - PageHeaderSize;

		public static int PageMinSpace = (int)(PageMaxSpace * 0.33);

		public static int PageNumberSize = sizeof(long);

		public static int NodeOffsetSize = sizeof(ushort);
	}
}
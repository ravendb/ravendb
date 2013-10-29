using Voron.Trees;

namespace Voron.Impl
{
	public unsafe class Constants
	{
        public const ulong MagicMarker = 0xB16BAADC0DEF0015;

        public const ulong TransactionHeaderMarker = 0x1A4C92AD90ABC123; 

		/// <summary>
		/// If there are less than 2 keys in a page, we no longer have a tree
		/// This impacts the MakKeySize available
		/// </summary>
		public const int MinKeysInPage = 2;

		public static readonly int PageHeaderSize = sizeof(PageHeader);

		public static readonly int NodeHeaderSize = sizeof(NodeHeader);

		public static int PageNumberSize = sizeof(long);

		public static int NodeOffsetSize = sizeof(ushort);
	    public const int CurrentVersion = 0x00010009;

		public const string RootTreeName = "Root";
		public const string FreeSpaceTreeName = "Free Space";
	}
}
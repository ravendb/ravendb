using System.Runtime.InteropServices;

namespace Nevar
{
	internal unsafe class Constants
	{
		public const int PageSize = 4096;

		public const int MinKeysInPage = 2;

		public static readonly int PageHeaderSize = sizeof(PageHeader);

		public static readonly int NodeHeaderSize = sizeof(NodeHeader);

		public static readonly int MaxKeySize = (PageSize - PageHeaderSize) / MinKeysInPage;

	}
}
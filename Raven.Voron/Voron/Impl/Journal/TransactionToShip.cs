using System;
using System.IO;
using Voron.Impl.Paging;

namespace Voron.Impl.Journal
{
	public unsafe class TransactionToShip
	{
	    public TransactionHeader Header { get; private set; }

		public uint PreviousTransactionCrc { get; set; }

		public byte*[] CompressedPages { get; set; }
	    public byte[] CopiedPages { get; set; }

	    public TransactionToShip(TransactionHeader header)
		{
			Header = header;
		}

	    public void CopyPages()
	    {
	        CopiedPages = new byte[CompressedPages.Length*AbstractPager.PageSize];
	        fixed (byte* p = CopiedPages)
	        {
	            for (int i = 0; i < CopiedPages.Length; i++)
	            {
	                NativeMethods.memcpy(p + (i*AbstractPager.PageSize), CompressedPages[i], AbstractPager.PageSize);
	            }
	        }
	    }
	}
}
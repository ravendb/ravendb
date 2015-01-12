using System;
using System.Diagnostics;
using System.IO;
using Voron.Impl.Paging;
using Voron.Util;

namespace Voron.Impl.Journal
{
	public unsafe class TransactionToShip
	{
		private byte[] _copiedPages;
		public TransactionHeader Header { get; private set; }

		public byte[] PagesSnapshot
		{
			get
			{
				if(_copiedPages == null)
					CreatePagesSnapshot();
				
				return _copiedPages;
			}
		}
		public byte*[] CompressedPages { get; set; }

		public TransactionToShip(TransactionHeader header)
		{
			Header = header;
		}

		public void CreatePagesSnapshot()
	    {
			_copiedPages = new byte[CompressedPages.Length * AbstractPager.PageSize];
	        fixed (byte* p = PagesSnapshot)
	        {
				for (int i = 0; i < CompressedPages.Length; i++)
	            {
                    MemoryUtils.Copy(p + (i * AbstractPager.PageSize), CompressedPages[i], AbstractPager.PageSize);
	            }
	        }
	    }
	}
}
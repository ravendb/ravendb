using Sparrow;
using System;
using System.Diagnostics;
using System.IO;
using Voron.Impl.Paging;
using Voron.Util;

namespace Voron.Impl.Journal
{
    public unsafe class TransactionToShip
    {
        private readonly int _pageSize;
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
        public IntPtr[] CompressedPages { get; set; }
        public int PageSize { get { return _pageSize; } }

        public TransactionToShip(TransactionHeader header, int pageSize)
        {
            _pageSize = pageSize;
            Header = header;
        }

        public void CreatePagesSnapshot()
        {
            _copiedPages = new byte[CompressedPages.Length * _pageSize];
            fixed (byte* p = PagesSnapshot)
            {
                for (int i = 0; i < CompressedPages.Length; i++)
                {
                    Memory.Copy(p + (i * _pageSize), (byte*)CompressedPages[i].ToPointer(), _pageSize);
                }
            }
        }
    }
}

using System;
using System.Runtime.InteropServices;
using Voron.Trees;

namespace Voron.Impl.Paging
{
    public unsafe class TemporaryPage : IDisposable
    {
        private readonly byte[] _tempPageBuffer = new byte[AbstractPager.PageSize];
        private GCHandle _tempPageHandle;
        private IntPtr _tempPage;

		public TemporaryPage()
		{
			_tempPageHandle = GCHandle.Alloc(_tempPageBuffer, GCHandleType.Pinned);
			_tempPage = _tempPageHandle.AddrOfPinnedObject();
		}

		public void Dispose()
		{
			if (_tempPageHandle.IsAllocated)
			{
				_tempPageHandle.Free();
			}
		}

        public byte[] TempPageBuffer
        {
            get { return _tempPageBuffer; }
        }

		public byte* TempPagePointer
		{
			get { return (byte*)_tempPage.ToPointer(); }
		}

        public Page TempPage
        {
            get
            {
                return new Page((byte*)_tempPage.ToPointer(), "temp")
                {
                    Upper = AbstractPager.PageSize,
                    Lower = (ushort)Constants.PageHeaderSize,
                    Flags = 0,
                };
            }
        }

    }
}
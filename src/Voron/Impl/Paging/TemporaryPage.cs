using System;
using System.Runtime.InteropServices;
using Voron.Data.BTrees;

namespace Voron.Impl.Paging
{
    public unsafe class TemporaryPage : IDisposable
    {
	    private readonly StorageEnvironmentOptions _options;
	    private readonly byte[] _tempPageBuffer;
        private readonly GCHandle _tempPageHandle;
        private readonly IntPtr _tempPage;

		public TemporaryPage(StorageEnvironmentOptions options)
		{
			_options = options;
			_tempPageBuffer = new byte[options.PageSize];
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

        public TreePage GetTempPage()
        {
			return new TreePage((byte*)_tempPage.ToPointer(), "temp", _options.PageSize)
	        {
				Upper = (ushort)_options.PageSize,
		        Lower = (ushort) Constants.TreePageHeaderSize,
		        TreeFlags = TreePageFlags.None,
	        };
        }
	    public IDisposable ReturnTemporaryPageToPool { get; set; }
    }
}
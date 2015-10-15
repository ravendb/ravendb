using System;
using System.Runtime.InteropServices;
using Voron.Trees;

namespace Voron.Impl.Paging
{
    public unsafe class TemporaryPage : IDisposable
    {
	    private readonly StorageEnvironmentOptions _options;
	    private readonly byte[] _tempPageBuffer;
        private GCHandle _tempPageHandle;
        private IntPtr _tempPage;

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
		        Lower = (ushort) Constants.PageHeaderSize,
		        TreeFlags = TreePageFlags.None,
	        };
        }
	    public IDisposable ReturnTemporaryPageToPool { get; set; }
    }
}
using System;
using System.Runtime.InteropServices;
using Voron.Global;
using Voron.Data.BTrees;

namespace Voron.Impl.Paging
{
    public unsafe class TemporaryPage : IDisposable
    {
        private readonly byte[] _tempPageBuffer;
        private GCHandle _tempPageHandle;
        private readonly IntPtr _tempPage;
        internal readonly int PageSize;

        public TemporaryPage(StorageEnvironmentOptions options, int? pageSize = null)
        {
            PageSize = pageSize ?? options.PageSize;
            _tempPageBuffer = new byte[PageSize];
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

        public byte[] TempPageBuffer => _tempPageBuffer;

        public byte* TempPagePointer => (byte*)_tempPage.ToPointer();

        public TreePage GetTempPage()
        {
            return new TreePage((byte*)_tempPage.ToPointer(), PageSize)
            {
                Upper = (ushort)PageSize,
                Lower = (ushort) Constants.TreePageHeaderSize,
                TreeFlags = TreePageFlags.None,
            };
        }
        public IDisposable ReturnTemporaryPageToPool { get; set; }
    }
}
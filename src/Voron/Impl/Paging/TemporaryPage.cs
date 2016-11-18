using System;
using System.Diagnostics;
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
            var upper = (ushort)PageSize;

            if (upper < PageSize)
            {
                // we have overflown Upper which is ushort 
                // it means the page size is 64KB
                // we have special handling for this in AllocateNewNode

                Debug.Assert(PageSize == Constants.Storage.MaxPageSize);

                upper = ushort.MaxValue;
            }

            return new TreePage((byte*)_tempPage.ToPointer(), PageSize)
            {
                Upper = upper,
                Lower = (ushort) Constants.TreePageHeaderSize,
                TreeFlags = TreePageFlags.None,
            };
        }
        public IDisposable ReturnTemporaryPageToPool { get; set; }
    }
}
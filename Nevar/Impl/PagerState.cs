using System;
using System.IO.MemoryMappedFiles;
using System.Runtime.InteropServices;
using System.Threading;

namespace Nevar.Impl
{
    public unsafe class PagerState
    {
        private int _refs;

        public MemoryMappedViewAccessor Accessor;

        public MemoryMappedFile File;

        public byte* Base;

        public IntPtr Ptr;

        public void Release()
        {
            if (Interlocked.Decrement(ref _refs) == 0)
                return;

            Base = null;
            if (Accessor != null)
            {
                Accessor.SafeMemoryMappedViewHandle.ReleasePointer();
                Accessor.Dispose();
            }
            if (File != null)
                File.Dispose();

            if(Ptr != IntPtr.Zero)
                Marshal.FreeHGlobal(Ptr);
        }

        public void AddRef()
        {
            Interlocked.Increment(ref _refs);
        }
    }
}
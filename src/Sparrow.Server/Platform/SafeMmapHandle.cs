using System;
using System.Runtime.InteropServices;

namespace Sparrow.Server.Platform
{
    public class SafeMmapHandle : SafeHandle
    {
        public PalFlags.FailCodes FailCode;
        public int ErrorNo;

        public SafeMmapHandle() : base(IntPtr.Zero, true)
        {
        }

        protected override bool ReleaseHandle()
        {
            FailCode = Pal.rvn_mmap_dispose_handle(handle, out ErrorNo);

            handle = IntPtr.Zero;
            return FailCode == PalFlags.FailCodes.Success;
        }

        public override bool IsInvalid => handle == IntPtr.Zero;
    }
}
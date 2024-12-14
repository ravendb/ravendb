using System;
using System.Runtime.InteropServices;

namespace Sparrow.Server.Platform
{
    public sealed class SafeJournalHandle() : SafeHandle(IntPtr.Zero, true)
    {
        public PalFlags.FailCodes FailCode; 
        public int ErrorNo;

        protected override bool ReleaseHandle()
        {
            FailCode = Pal.rvn_close_journal(handle, out ErrorNo);
            handle = IntPtr.Zero;
            return FailCode == PalFlags.FailCodes.Success;
        }

        public override bool IsInvalid => handle == IntPtr.Zero;
    }
}

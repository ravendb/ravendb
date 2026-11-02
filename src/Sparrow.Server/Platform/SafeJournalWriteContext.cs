using System;
using System.Runtime.InteropServices;

namespace Sparrow.Server.Platform
{
    public sealed class SafeJournalWriteContext() : SafeHandle(IntPtr.Zero, true)
    {
        public PalFlags.FailCodes FailCode;
        public int ErrorNo;

        public static SafeJournalWriteContext Create()
        {
            var rc = Pal.rvn_create_journal_write_context(out var context, out var errorCode);
            if (rc != PalFlags.FailCodes.Success)
                PalHelper.ThrowLastError(rc, errorCode, "Attempted to create a journal write context");

            return context;
        }

        protected override bool ReleaseHandle()
        {
            FailCode = Pal.rvn_free_journal_write_context(handle, out ErrorNo);
            handle = IntPtr.Zero;
            return FailCode == PalFlags.FailCodes.Success;
        }

        public override bool IsInvalid => handle == IntPtr.Zero;
    }
}

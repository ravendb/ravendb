namespace Voron.Impl.Paging;

public unsafe partial class Pager2
{
    public class Functions
    {
        public delegate* <Pager2, OpenFileOptions, State> Init;
        public delegate* <Pager2, State, ref PagerTransactionState, long, byte*> AcquirePagePointer;
        public delegate* <Pager2, State, ref PagerTransactionState, long, byte*> AcquireRawPagePointer;
        public delegate* <Pager2, long, int, State, ref PagerTransactionState, byte*> AcquirePagePointerForNewPage;
        public delegate* <Pager2, long, ref State, long, void> AllocateMorePages;
        public delegate* <Pager2, State, void> Sync;
        public delegate* <byte*, ulong, void> ProtectPageRange;
        public delegate* <byte*, ulong, void> UnprotectPageRange;
        public delegate* <Pager2, State, ref PagerTransactionState, long, int, bool> EnsureMapped;
        public delegate* <Pager2, byte*, long, bool> RecoverFromMemoryLockFailure;
        public delegate* <Pager2, ref State, ref PagerTransactionState, long, long, int, byte*, void> DirectWrite;
    }
}

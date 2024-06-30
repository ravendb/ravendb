namespace Voron.Impl.Paging;

public unsafe partial class Pager2
{
    public class Functions
    {
        public delegate* <Pager2, State, ref PagerTransactionState, long, byte*> AcquirePagePointer;
        public delegate* <Pager2, State, ref PagerTransactionState, long, byte*> AcquireRawPagePointer;
        public delegate* <Pager2, long, int, State, ref PagerTransactionState, byte*> AcquirePagePointerForNewPage;
        public delegate* <byte*, ulong, void> ProtectPageRange;
        public delegate* <byte*, ulong, void> UnprotectPageRange;
        public delegate* <Pager2, State, ref PagerTransactionState, long, int, bool> EnsureMapped;
    }
}

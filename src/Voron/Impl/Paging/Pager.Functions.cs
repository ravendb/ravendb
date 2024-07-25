namespace Voron.Impl.Paging;

public unsafe partial class Pager
{
    public class Functions
    {
        public delegate* <Pager, State, ref PagerTransactionState, long, byte*> AcquirePagePointer;
        public delegate* <Pager, State, ref PagerTransactionState, long, byte*> AcquireRawPagePointer;
        public delegate* <Pager, long, int, State, ref PagerTransactionState, byte*> AcquirePagePointerForNewPage;
        public delegate* <Pager, State, ref PagerTransactionState, long, int, bool> EnsureMapped;
    }
}

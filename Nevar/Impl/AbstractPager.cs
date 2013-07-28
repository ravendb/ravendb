using Nevar.Trees;

namespace Nevar.Impl
{
    public abstract class AbstractPager : IVirtualPager
    {
        protected AbstractPager()
        {
            MaxNodeSize = (PageSize - Constants.PageHeaderSize) / Constants.MinKeysInPage;
            PageMaxSpace = PageSize - Constants.PageHeaderSize;
            PageMinSpace = (int)(PageMaxSpace * 0.33);
        }

        public int PageMaxSpace { get; private set; }
        public int MaxNodeSize { get; private set; }
        public int PageMinSpace { get; private set; }
        public int PageSize
        {
            get { return 4096; }
        }

        
        public abstract  long NumberOfAllocatedPages { get; }
    
        public abstract Page Get(long n);

        public abstract void Flush();

        public abstract PagerState TransactionBegan();

        public abstract void TransactionCompleted(PagerState state);

        public abstract void Dispose();
    }
}
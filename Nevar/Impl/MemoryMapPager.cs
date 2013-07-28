using Nevar.Trees;

namespace Nevar.Impl
{
    public class MemoryMapPager : AbstractPager
    {
        public override Page Get(long n)
        {
            throw new System.NotImplementedException();
        }

        public override void Flush()
        {
            throw new System.NotImplementedException();
        }

        public override PagerState TransactionBegan()
        {
            throw new System.NotImplementedException();
        }

        public override void TransactionCompleted(PagerState state)
        {
            throw new System.NotImplementedException();
        }

        public override void Dispose()
        {
            throw new System.NotImplementedException();
        }

        public override long NumberOfAllocatedPages
        {
            get { throw new System.NotImplementedException(); }
        }
    }
}
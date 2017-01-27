using Voron.Impl;

namespace Voron.Data.Tables
{
    public class NewPageAllocator
    {
        private readonly LowLevelTransaction _llt;

        public NewPageAllocator(LowLevelTransaction llt)
        {
            _llt = llt;
        }

        public Page AllocatePage(int numberOfPages)
        {
            return _llt.AllocatePage(numberOfPages);
        }

        public void FreePage(long pageNumber)
        {
            _llt.FreePage(pageNumber);
        }
    }
}
using Voron.Impl;

namespace Voron.Data.Tables
{
    /// <summary>
    /// This class is responsible for allocating pages for tables for the 
    /// indexes. This is important so we'll have high degree of locality 
    /// for those indexes, instead of having to search throughout the data
    /// file.
    /// 
    /// This is done by storing the information by preallocating
    /// data in batches of 128 pages at a time (1MB for 8KB pages) and
    /// trying to allocate the value near to the parent page as we can get.
    /// 
    /// Note that overflow pages are always allocated externally (they shouldn't
    /// happen in indexes, and complicate the code significantly).
    /// </summary>
    public class NewPageAllocator
    {
        private readonly LowLevelTransaction _llt;

        public NewPageAllocator(LowLevelTransaction llt)
        {
            _llt = llt;
        }

        public Page AllocatePage()
        {
            return _llt.AllocatePage(1);
        }

        public void FreePage(long pageNumber)
        {
            _llt.FreePage(pageNumber);
        }
    }
}
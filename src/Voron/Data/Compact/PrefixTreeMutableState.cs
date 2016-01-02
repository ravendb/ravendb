using Voron.Data.BTrees;

namespace Voron.Data.Compact
{
    public unsafe class PrefixTreeMutableState
    {
        public long RootPageNumber;
        public long PageCount;

        public void RecordNewPage(TreePage p, int num)
        {
            PageCount += num;
        }

        public void RecordFreedPage(TreePage p, int num)
        {
            PageCount -= num;
        }
    }
}

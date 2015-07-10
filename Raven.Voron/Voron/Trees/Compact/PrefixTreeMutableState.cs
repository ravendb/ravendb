using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Voron.Trees.Compact
{
    public unsafe class PrefixTreeMutableState
    {
        public long RootPageNumber;
        public long PageCount;

        public void RecordNewPage(Page p, int num)
        {
            PageCount += num;
        }

        public void RecordFreedPage(Page p, int num)
        {
            PageCount -= num;
        }
    }
}

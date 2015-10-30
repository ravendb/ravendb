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

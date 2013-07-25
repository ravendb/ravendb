using System.Collections.Generic;

namespace Nevar.Debugging
{
    public class EnvironmentStats
    {
        public long FreePages;
        public long FreePagesOverhead;
        public long RootPages;
        public long HeaderPages;
        public List<FreedTransaction> FreedTransactions = new List<FreedTransaction>();

        public class FreedTransaction
        {
            public long Id;
            public List<long> Pages = new List<long>();
        }

    }
}
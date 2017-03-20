using System;
using System.Collections.Generic;

namespace Voron.Impl.Paging
{
    public class TempPagerTransaction : IPagerLevelTransactionState
    {
        public void Dispose()
        {
            OnDispose?.Invoke(this);
        }

        public Dictionary<AbstractPager, TransactionState> PagerTransactionState32Bits
        {
            get; set;
        }

        public event Action<IPagerLevelTransactionState> OnDispose;
        public void EnsurePagerStateReference(PagerState state)
        {
        }

        public StorageEnvironment Environment => null;
    }


}
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

        public Dictionary<AbstractPager, Windows32BitMemoryMapPager.TransactionState> Windows32BitPagerTransactionState
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
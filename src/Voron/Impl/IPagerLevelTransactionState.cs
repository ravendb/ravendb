using System;
using System.Collections.Generic;
using Voron.Impl.Paging;

namespace Voron.Impl
{
    public interface IPagerLevelTransactionState : IDisposable
    {
        Dictionary<AbstractPager, TransactionState> PagerTransactionState32Bits { get; set; }
        event Action<IPagerLevelTransactionState> OnDispose;
        void EnsurePagerStateReference(PagerState state);
        StorageEnvironment Environment { get; }
    }

    public static class PagerLevelTransacionState
    {
        public static long GetTotal32BitsMappedSize(this IPagerLevelTransactionState self)
        {
            var dic = self?.PagerTransactionState32Bits;
            if (dic == null)
                return 0;
            var result = 0L;
            // ReSharper disable once LoopCanBeConvertedToQuery
            foreach (var state in dic)
            {
                result += state.Value.TotalLoadedSize;
            }
            return result;
        }
    }
}
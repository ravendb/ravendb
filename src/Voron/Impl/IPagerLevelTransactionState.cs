using System;
using System.Collections.Generic;
using Sparrow;
using Voron.Impl.Paging;

namespace Voron.Impl
{
    public interface IPagerLevelTransactionState : IDisposable
    {
        Dictionary<AbstractPager, TransactionState> PagerTransactionState32Bits { get; set; }
        Dictionary<AbstractPager, CryptoTransactionState> CryptoPagerTransactionState { get; set; }
        Size AdditionalMemoryUsageSize { get; }

        event Action<IPagerLevelTransactionState> OnDispose;
        event Action<IPagerLevelTransactionState> BeforeCommitFinalization;
        void EnsurePagerStateReference(ref PagerState state);
        StorageEnvironment Environment { get; }
        bool IsWriteTransaction { get; }
    }

    public static class PagerLevelTransactionState
    {
        public static Size GetTotal32BitsMappedSize(this IPagerLevelTransactionState self)
        {
            var dic = self?.PagerTransactionState32Bits;
            if (dic == null)
                return new Size(0, SizeUnit.Bytes);
            var result = 0L;
            // ReSharper disable once LoopCanBeConvertedToQuery
            foreach (var state in dic)
            {
                result += state.Value.TotalLoadedSize;
            }
            return new Size(result, SizeUnit.Bytes);
        }
    }
}

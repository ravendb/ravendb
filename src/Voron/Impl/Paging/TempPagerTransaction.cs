using System;
using System.Collections.Generic;
using Sparrow;

namespace Voron.Impl.Paging
{
    public sealed class TempPagerTransaction : IPagerLevelTransactionState
    {
        private readonly bool _isWriteTransaction;

        public TempPagerTransaction(bool isWriteTransaction = false)
        {
            _isWriteTransaction = isWriteTransaction;
        }

        public void Dispose()
        {
            BeforeCommitFinalization?.Invoke(this);
            OnDispose?.Invoke(this);
        }

        bool IPagerLevelTransactionState.IsWriteTransaction => _isWriteTransaction;

        public Dictionary<AbstractPager, TransactionState> PagerTransactionState32Bits
        {
            get; set;
        }

        public Dictionary<AbstractPager, CryptoTransactionState> CryptoPagerTransactionState
        {
            get; set;
        }

        public Size AdditionalMemoryUsageSize
        {
            get
            {
                var cryptoTransactionStates = CryptoPagerTransactionState;
                if (cryptoTransactionStates == null)
                {
                    return new Size(0, SizeUnit.Bytes);
                }

                var total = 0L;
                foreach (var state in cryptoTransactionStates.Values)
                {
                    total += state.TotalCryptoBufferSize;
                }

                return new Size(total, SizeUnit.Bytes);
                ;
            }
        }

        public event Action<IPagerLevelTransactionState> OnDispose;
        public event Action<IPagerLevelTransactionState> BeforeCommitFinalization;

        public void EnsurePagerStateReference(ref PagerState state)
        {
        }

        public StorageEnvironment Environment => null;
    }


}

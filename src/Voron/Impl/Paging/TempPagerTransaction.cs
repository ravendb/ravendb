using System;
using System.Collections.Generic;
using Sparrow.Json;

namespace Voron.Impl.Paging
{
    public class TempPagerTransaction : IPagerLevelTransactionState
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

        public event Action<IPagerLevelTransactionState> OnDispose;
        public event Action<IPagerLevelTransactionState> BeforeCommitFinalization;

        public void EnsurePagerStateReference(PagerState state)
        {
        }

        public StorageEnvironment Environment => null;
    }


}
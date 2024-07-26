#nullable enable

using System.Collections.Generic;
using Sparrow;

namespace Voron.Impl.Paging;

public partial class Pager

{
    public struct PagerTransactionState
    {
        public Dictionary<Pager, TxStateFor32Bits>? For32Bits;
        public Dictionary<Pager, CryptoTransactionState>? ForCrypto;
        public bool IsWriteTransaction;
        
        /// <summary>
        /// These are events because we may have a single transaction deal
        /// with multiple pagers 
        /// </summary>
        public event TxStateDelegate OnDispose;
        public event TxStateDelegate OnBeforeCommitFinalization;
        
        public delegate void TxStateDelegate(StorageEnvironment environment, ref State dataPagerState, ref PagerTransactionState txState);

        public void InvokeBeforeCommitFinalization(StorageEnvironment environment, ref State dataPagerState, ref PagerTransactionState txState) => OnBeforeCommitFinalization?.Invoke(environment, ref dataPagerState,  ref txState);
        public void InvokeDispose(StorageEnvironment environment, ref State dataPagerState,ref PagerTransactionState txState) => OnDispose?.Invoke(environment, ref dataPagerState, ref txState);

        public Size GetTotal32BitsMappedSize()
        {
            if (For32Bits == null)
                return new Size(0, SizeUnit.Bytes);
            var result = 0L;
            // ReSharper disable once LoopCanBeConvertedToQuery
            foreach (var state in For32Bits)
            {
                result += state.Value.TotalLoadedSize;
            }
            return new Size(result, SizeUnit.Bytes);
        }
        
        
        public Size AdditionalMemoryUsageSize
        {
            get
            {
                
                var cryptoTransactionStates = ForCrypto;
                if( cryptoTransactionStates== null)
                    return Size.Zero;


                long total = 0;
                foreach (var state in cryptoTransactionStates.Values)
                {
                    total += state.TotalCryptoBufferSize;
                }
                return new Size(total, SizeUnit.Bytes);
            }
        }
    }
}

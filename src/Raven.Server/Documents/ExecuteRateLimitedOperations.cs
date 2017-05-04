using System;
using System.Collections.Generic;
using System.Threading;
using Raven.Client.Util.RateLimiting;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents
{
    public class ExecuteRateLimitedOperations<T> : TransactionOperationsMerger.MergedTransactionCommand
    {
        private readonly Queue<T> _documentIds;
        private readonly Action<T, DocumentsOperationContext> _action;
        private readonly RateGate _rateGate;
        private readonly OperationCancelToken _token;
        private readonly int? _batchSize;
        private CancellationToken _cancellationToken;

        internal ExecuteRateLimitedOperations(Queue<T> documentIds, Action<T, DocumentsOperationContext> action, RateGate rateGate,
            OperationCancelToken token, int? batchSize = null)
        {
            _documentIds = documentIds;
            _action = action;
            _rateGate = rateGate;
            _token = token;
            _batchSize = batchSize;
            _cancellationToken = token.Token;
        }

        public bool NeedWait { get; private set; }

        public int Processed { get; private set; }

        public override int Execute(DocumentsOperationContext context)
        {
            while (_documentIds.Count > 0)
            {
                _cancellationToken.ThrowIfCancellationRequested();

                _token.Delay();

                if (_rateGate != null && _rateGate.WaitToProceed(0) == false)
                {
                    NeedWait = true;
                    break;
                }

                var id = _documentIds.Dequeue();

                _action(id, context);

                Processed++;

                if (_batchSize != null && Processed >= _batchSize)
                    break;
            }

            return Processed;
        }

        
    }
}
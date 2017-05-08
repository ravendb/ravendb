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
        private readonly Func<T, TransactionOperationsMerger.MergedTransactionCommand> _commandToExecute;
        private readonly RateGate _rateGate;
        private readonly OperationCancelToken _token;
        private readonly int? _batchSize;
        private CancellationToken _cancellationToken;

        internal ExecuteRateLimitedOperations(Queue<T> documentIds, Func<T, TransactionOperationsMerger.MergedTransactionCommand> commandToExecute, RateGate rateGate,
            OperationCancelToken token, int? batchSize = null)
        {
            _documentIds = documentIds;
            _commandToExecute = commandToExecute;
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

                var command = _commandToExecute(id);

                var count = command?.Execute(context) ?? 0;

                Processed += count;

                if (_batchSize != null && Processed >= _batchSize)
                    break;
            }

            return Processed;
        }
    }
}
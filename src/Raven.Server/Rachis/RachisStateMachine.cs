using System;
using System.Diagnostics;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;

namespace Raven.Server.Rachis
{
    public abstract class RachisStateMachine : IDisposable
    {
        protected TransactionContextPool ContextPoolForReadOnlyOperations;
        private RachisConsensus _parent;
        public void Initialize(RachisConsensus parent)
        {
            _parent = parent;
            ContextPoolForReadOnlyOperations = _parent.ContextPool;
        }

        public void Apply(TransactionOperationContext context, long uptoInclusive)
        {
            Debug.Assert(context.Transaction != null);

            var lastAppliedIndex = _parent.GetLastCommitIndex(context);
            for (var index = lastAppliedIndex+1; index <= uptoInclusive; index++)
            {
                var cmd = _parent.GetEntry(context, index);
                if (cmd == null)
                    throw new InvalidOperationException("Expected to apply entry " + index + " but it isn't stored");

                Apply(context, cmd);
            }
            _parent.SetLastCommitIndex(context, uptoInclusive);
        }

        protected abstract void Apply(TransactionOperationContext context, BlittableJsonReaderObject cmd);

        public void Dispose()
        {
            
        }
    }
}
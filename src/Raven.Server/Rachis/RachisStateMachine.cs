using System;
using System.Diagnostics;
using System.IO;
using System.Threading.Tasks;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Voron;
using Voron.Data;
// ReSharper disable InconsistentNaming

namespace Raven.Server.Rachis
{
    public abstract class RachisStateMachine : IDisposable
    {
        protected TransactionContextPool ContextPoolForReadOnlyOperations;
        protected RachisConsensus _parent;

        public virtual void Initialize(RachisConsensus parent, TransactionOperationContext context)
        {
            _parent = parent;            
            ContextPoolForReadOnlyOperations = _parent.ContextPool;
        }

        public void Apply(TransactionOperationContext context, long uptoInclusive, Leader leader)
        {
            Debug.Assert(context.Transaction != null);

            var lastAppliedIndex = _parent.GetLastCommitIndex(context);
            for (var index = lastAppliedIndex+1; index <= uptoInclusive; index++)
            {
                RachisEntryFlags flags;
                var cmd = _parent.GetEntry(context, index,out flags);
                if (cmd == null || flags == RachisEntryFlags.Invalid)
                    throw new InvalidOperationException("Expected to apply entry " + index + " but it isn't stored");

                if(flags != RachisEntryFlags.StateMachineCommand)
                    continue;

                Apply(context, cmd, index, leader);
            }
            var term = _parent.GetTermForKnownExisting(context, uptoInclusive);

            _parent.SetLastCommitIndex(context, uptoInclusive, term);
        }

        protected abstract void Apply(TransactionOperationContext context, BlittableJsonReaderObject cmd, long index, Leader leader);

        public void Dispose()
        {
            
        }


        public abstract bool ShouldSnapshot(Slice slice, RootObjectType type);

        public abstract Task<Stream> ConnectToPeer(string url, string apiKey);

        public virtual void OnSnapshotInstalled(TransactionOperationContext context, long lastIncludedIndex)
        {
            
        }
    }
}
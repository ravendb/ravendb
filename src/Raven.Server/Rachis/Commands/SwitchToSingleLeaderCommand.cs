using System;
using Raven.Server.Documents.TransactionMerger.Commands;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Rachis.Commands
{
    public sealed class SwitchToSingleLeaderCommand : MergedTransactionCommand<ClusterOperationContext, ClusterTransaction>
    {
        private readonly RachisConsensus _engine;

        public SwitchToSingleLeaderCommand(RachisConsensus engine)
        {
            _engine = engine;
        }

        protected override long ExecuteCmd(ClusterOperationContext context)
        {
            _engine.SwitchToSingleLeader(context);
            return 1;
        }

        public override IReplayableCommandDto<ClusterOperationContext, ClusterTransaction, MergedTransactionCommand<ClusterOperationContext, ClusterTransaction>> ToDto(ClusterOperationContext context)
        {
            throw new NotImplementedException();
        }
    }
}

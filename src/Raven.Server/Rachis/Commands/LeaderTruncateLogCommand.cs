using System;
using JetBrains.Annotations;
using Raven.Server.Documents.TransactionMerger.Commands;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;

namespace Raven.Server.Rachis.Commands;

public sealed class LeaderTruncateLogCommand : MergedTransactionCommand<ClusterOperationContext, ClusterTransaction>
{
    private readonly RachisConsensus _engine;
    private readonly long _lowestIndexInEntireCluster;

    public LeaderTruncateLogCommand([NotNull] RachisConsensus engine, long lowestIndexInEntireCluster)
    {
        _engine = engine ?? throw new ArgumentNullException(nameof(engine));
        _lowestIndexInEntireCluster = lowestIndexInEntireCluster;
    }

    protected override long ExecuteCmd(ClusterOperationContext context)
    {
        _engine.TruncateLogBefore(context, _lowestIndexInEntireCluster);

        return 1;
    }

    public override IReplayableCommandDto<ClusterOperationContext, ClusterTransaction, MergedTransactionCommand<ClusterOperationContext, ClusterTransaction>> ToDto(ClusterOperationContext context)
    {
        throw new NotImplementedException();
    }

}

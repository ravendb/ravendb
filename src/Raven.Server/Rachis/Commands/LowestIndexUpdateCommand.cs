using System;
using System.Collections.Generic;
using System.Diagnostics.Eventing.Reader;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Jint;
using Raven.Server.Documents.TransactionMerger.Commands;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Rachis.Commands;

public sealed class LowestIndexUpdateCommand : MergedTransactionCommand<ClusterOperationContext, ClusterTransaction>
{
    private readonly RachisConsensus _engine;

    private long _lowestIndexInEntireCluster;


    public LowestIndexUpdateCommand([NotNull] RachisConsensus engine, long lowestIndexInEntireCluster)
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


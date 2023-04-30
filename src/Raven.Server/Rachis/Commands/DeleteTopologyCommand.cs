using System;
using System.Collections.Generic;
using JetBrains.Annotations;
using Raven.Client.Http;
using Raven.Server.Documents.TransactionMerger.Commands;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;

namespace Raven.Server.Rachis.Commands;

public class DeleteTopologyCommand : MergedTransactionCommand<ClusterOperationContext, ClusterTransaction>
{
    private readonly RachisConsensus _engine;

    public DeleteTopologyCommand(RachisConsensus engine)
    {
        _engine = engine;
    }

    protected override long ExecuteCmd(ClusterOperationContext context)
    {
        var topology = _engine.GetTopology(context);
        var newTopology = new ClusterTopology(
            topology.TopologyId,
            new Dictionary<string, string>(),
            new Dictionary<string, string>(),
            new Dictionary<string, string>(),
            topology.LastNodeId,
            -1
        );
        _engine.SetTopology(context, newTopology);

        return 1;
    }

    public override IReplayableCommandDto<ClusterOperationContext, ClusterTransaction, MergedTransactionCommand<ClusterOperationContext, ClusterTransaction>> ToDto(ClusterOperationContext context)
    {
        throw new NotImplementedException();
    }
}

using System;
using JetBrains.Annotations;
using Raven.Client.ServerWide;
using Raven.Server.Documents.TransactionMerger.Commands;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;

namespace Raven.Server.Rachis.Commands;

public class SwitchToCandidateStateCommand : MergedTransactionCommand<ClusterOperationContext, ClusterTransaction>
{
    private readonly RachisConsensus _engine;
    private readonly long _currentTerm;
    private readonly string _tag;
    private readonly string _reason;

    public SwitchToCandidateStateCommand([NotNull] RachisConsensus engine, long currentTerm, [NotNull] string tag, string reason)
    {
        _engine = engine ?? throw new ArgumentNullException(nameof(engine));
        _currentTerm = currentTerm;
        _tag = tag ?? throw new ArgumentNullException(nameof(tag));
        _reason = reason;
    }

    protected override long ExecuteCmd(ClusterOperationContext context)
    {
        var clusterTopology = _engine.GetTopology(context);
        if (clusterTopology.TopologyId == null ||
            clusterTopology.AllNodes.ContainsKey(_tag) == false)
        {
            if (_engine.Log.IsInfoEnabled)
            {
                _engine.Log.Info($"We are not a part of the cluster so moving to passive (candidate because: {_reason})");
            }

            _engine.SetNewStateInTx(context, RachisState.Passive, null, _currentTerm, "We are not a part of the cluster so moving to passive");
            return 1;
        }

        if (clusterTopology.Members.ContainsKey(_tag) == false)
        {
            if (_engine.Log.IsInfoEnabled)
            {
                _engine.Log.Info($"Candidate because: {_reason}, but while we are part of the cluster, we aren't a member, so we can't be a candidate.");
            }
            // we aren't a member, nothing that we can do here
            return 1;
        }

        if (clusterTopology.AllNodes.Count == 1 &&
            clusterTopology.Members.Count == 1)
        {
            if (_engine.Log.IsInfoEnabled)
            {
                _engine.Log.Info("Trying to switch to candidate when I'm the only node in the cluster, turning into a leader, instead");
            }

            _engine.SwitchToSingleLeader(context);
            return 1;
        }

        return 1;
    }

    public override IReplayableCommandDto<ClusterOperationContext, ClusterTransaction, MergedTransactionCommand<ClusterOperationContext, ClusterTransaction>> ToDto(JsonOperationContext context)
    {
        throw new System.NotImplementedException();
    }
}

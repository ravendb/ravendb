using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using JetBrains.Annotations;
using Raven.Client.Http;
using Raven.Server.Documents.TransactionMerger.Commands;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;

namespace Raven.Server.Rachis.Commands;

public class LeaderModifyTopologyCommand : MergedTransactionCommand<ClusterOperationContext, ClusterTransaction>
{
    private readonly RachisConsensus _engine;
    private readonly Leader _leader;
    private readonly Leader.TopologyModification _modification;
    private readonly string _nodeTag;
    private readonly string _nodeUrl;
    private readonly bool _validateNotInTopology;

    public long Index { get; private set; }

    public LeaderModifyTopologyCommand([NotNull] RachisConsensus engine, Leader leader, Leader.TopologyModification modification, string nodeTag, string nodeUrl, bool validateNotInTopology)
    {
        _engine = engine ?? throw new ArgumentNullException(nameof(engine));
        _leader = leader;
        _modification = modification;
        _nodeTag = nodeTag;
        _nodeUrl = nodeUrl;
        _validateNotInTopology = validateNotInTopology;
    }

    public static void AssertTopology(ClusterTopology clusterTopology, bool validateNotInTopology, string nodeTag, string nodeUrl)
    {
        if (validateNotInTopology && (nodeTag != null && clusterTopology.Contains(nodeTag) || clusterTopology.TryGetNodeTagByUrl(nodeUrl).HasUrl))
        {
            throw new InvalidOperationException($"Was requested to modify the topology for node={nodeTag} " +
                                                "with validation that it is not contained by the topology but current topology contains it.");
        }
    }

    protected override long ExecuteCmd(ClusterOperationContext context)
    {
        var nodeTag = _nodeTag;
        var nodeUrl = _nodeUrl;
        var clusterTopology = _engine.GetTopology(context);

        //We need to validate that the node doesn't exists before we generate the nodeTag
        AssertTopology(clusterTopology, _validateNotInTopology, nodeTag, nodeUrl);

        if (nodeTag == null)
        {
            nodeTag = Leader.GenerateNodeTag(clusterTopology);
        }

        var newVotes = new Dictionary<string, string>(clusterTopology.Members);
        newVotes.Remove(nodeTag);
        var newPromotables = new Dictionary<string, string>(clusterTopology.Promotables);
        newPromotables.Remove(nodeTag);
        var newNonVotes = new Dictionary<string, string>(clusterTopology.Watchers);
        newNonVotes.Remove(nodeTag);

        var highestNodeId = newVotes.Keys.Concat(newPromotables.Keys).Concat(newNonVotes.Keys).Concat(new[] { nodeTag }).Max();

        if (nodeTag == _engine.Tag)
            RachisTopologyChangeException.Throw("Cannot modify the topology of the leader node.");

        switch (_modification)
        {
            case Leader.TopologyModification.Voter:
                Debug.Assert(nodeUrl != null);
                newVotes[nodeTag] = nodeUrl;
                break;
            case Leader.TopologyModification.Promotable:
                Debug.Assert(nodeUrl != null);
                newPromotables[nodeTag] = nodeUrl;
                break;
            case Leader.TopologyModification.NonVoter:
                Debug.Assert(nodeUrl != null);
                newNonVotes[nodeTag] = nodeUrl;
                break;
            case Leader.TopologyModification.Remove:
                _leader.PeersVersion.TryRemove(nodeTag, out _);
                if (clusterTopology.Contains(nodeTag) == false)
                {
                    throw new InvalidOperationException($"Was requested to remove node={nodeTag} from the topology " +
                                                        "but it is not contained by the topology.");
                }
                break;
            default:
                throw new ArgumentOutOfRangeException(nameof(_modification), _modification, null);
        }

        clusterTopology = new ClusterTopology(
            clusterTopology.TopologyId,
            newVotes,
            newPromotables,
            newNonVotes,
            highestNodeId,
            _engine.GetLastEntryIndex(context) + 1
        );

        var topologyJson = _engine.SetTopology(context, clusterTopology);
        Index = _engine.InsertToLeaderLog(context, _leader.Term, topologyJson, RachisEntryFlags.Topology);

        if (_modification == Leader.TopologyModification.Remove)
        {
            _engine.GetStateMachine().EnsureNodeRemovalOnDeletion(context, _leader.Term, nodeTag);
        }

        return 1;
    }

    public override IReplayableCommandDto<ClusterOperationContext, ClusterTransaction, MergedTransactionCommand<ClusterOperationContext, ClusterTransaction>> ToDto(ClusterOperationContext context)
    {
        throw new NotImplementedException();
    }
}

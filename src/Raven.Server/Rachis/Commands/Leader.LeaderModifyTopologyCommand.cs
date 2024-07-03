using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Http;
using Raven.Server.Documents.TransactionMerger.Commands;
using Raven.Server.ServerWide.Context;
using Exception = System.Exception;

namespace Raven.Server.Rachis;

public partial class Leader
{
    public sealed class LeaderModifyTopologyCommand : MergedTransactionCommand<ClusterOperationContext, ClusterTransaction>
    {
        private readonly RachisConsensus _engine;
        private readonly Leader.TopologyModification _modification;
        private string _nodeTag;
        private readonly string _nodeUrl;
        public readonly Leader _leader;
        private readonly bool _validateNotInTopology;

        public LeaderModifyTopologyCommand([NotNull] RachisConsensus engine, [NotNull] Leader leader, Leader.TopologyModification modification, string nodeTag,
            string nodeUrl, bool validateNotInTopology)
        {
            _engine = engine ?? throw new ArgumentNullException(nameof(engine));
            _leader = leader ?? throw new ArgumentNullException(nameof(leader));
            _modification = modification;
            _nodeTag = nodeTag;
            _nodeUrl = nodeUrl;
            _validateNotInTopology = validateNotInTopology;
        }

        protected override long ExecuteCmd(ClusterOperationContext context)
        {
            var clusterTopology = _engine.GetTopology(context);

            //We need to validate that the node doesn't exists before we generate the nodeTag
            if (_validateNotInTopology && (_nodeTag != null && clusterTopology.Contains(_nodeTag) || clusterTopology.TryGetNodeTagByUrl(_nodeUrl).HasUrl))
            {
                throw new InvalidOperationException($"Was requested to modify the topology for node={_nodeTag} " +
                                                    "with validation that it is not contained by the topology but current topology contains it.");
            }

            if (_nodeTag == null)
            {
                _nodeTag = GenerateNodeTag(clusterTopology);
            }

            var newVotes = new Dictionary<string, string>(clusterTopology.Members);
            newVotes.Remove(_nodeTag);
            var newPromotables = new Dictionary<string, string>(clusterTopology.Promotables);
            newPromotables.Remove(_nodeTag);
            var newNonVotes = new Dictionary<string, string>(clusterTopology.Watchers);
            newNonVotes.Remove(_nodeTag);

            var highestNodeId = newVotes.Keys.Concat(newPromotables.Keys).Concat(newNonVotes.Keys).Concat(new[] { _nodeTag }).Max();

            if (_nodeTag == _engine.Tag)
                RachisTopologyChangeException.Throw("Cannot modify the topology of the leader node.");

            switch (_modification)
            {
                case Leader.TopologyModification.Voter:
                    Debug.Assert(_nodeUrl != null);
                    newVotes[_nodeTag] = _nodeUrl;
                    break;
                case Leader.TopologyModification.Promotable:
                    Debug.Assert(_nodeUrl != null);
                    newPromotables[_nodeTag] = _nodeUrl;
                    break;
                case Leader.TopologyModification.NonVoter:
                    Debug.Assert(_nodeUrl != null);
                    newNonVotes[_nodeTag] = _nodeUrl;
                    break;
                case Leader.TopologyModification.Remove:
                    _leader.PeersVersion.TryRemove(_nodeTag, out _);
                    if (clusterTopology.Contains(_nodeTag) == false)
                    {
                        throw new InvalidOperationException($"Was requested to remove node={_nodeTag} from the topology " +
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
            var index = _engine.InsertToLeaderLog(context, _leader.Term, topologyJson, RachisEntryFlags.Topology);

            if (_modification == Leader.TopologyModification.Remove)
            {
                _engine.GetStateMachine().EnsureNodeRemovalOnDeletion(context, _leader.Term, _nodeTag);
            }

            CompleteTopologyModificationAfterRachisCommit(index);

            return 1;
        }

        private static string GenerateNodeTag(ClusterTopology clusterTopology)
        {
            if (clusterTopology.LastNodeId.Length == 0)
            {
                return "A";
            }

            if (clusterTopology.LastNodeId[clusterTopology.LastNodeId.Length - 1] + 1 > 'Z')
            {
                return clusterTopology.LastNodeId + "A";
            }

            var lastChar = (char)(clusterTopology.LastNodeId[clusterTopology.LastNodeId.Length - 1] + 1);
            return clusterTopology.LastNodeId.Substring(0, clusterTopology.LastNodeId.Length - 1) + lastChar;
        }

        private void CompleteTopologyModificationAfterRachisCommit(long index)
        {
            var tcs = new TaskCompletionSource<(long Index, object Result)>(TaskCreationOptions.RunContinuationsAsynchronously);
            _leader._entries[index] = new Leader.CommandState { TaskCompletionSource = tcs, CommandIndex = index };
            tcs.Task.ContinueWith(t =>
            {
                var current = Interlocked.Exchange(ref _leader._topologyModification, null);

                try
                {
                    t.GetAwaiter().GetResult(); // this task is already completed here
                    current?.TrySetResult(null);
                }
                catch (OperationCanceledException)
                {
                    current?.TrySetCanceled();
                }
                catch (Exception e)
                {
                    current?.TrySetException(e);
                }
            });
        }

        public override IReplayableCommandDto<ClusterOperationContext, ClusterTransaction, MergedTransactionCommand<ClusterOperationContext, ClusterTransaction>> ToDto(
            ClusterOperationContext context)
        {
            throw new NotImplementedException();
        }
    }

}

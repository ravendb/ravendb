using System;
using System.Collections.Generic;
using Raven.Client.Http;
using Raven.Client.ServerWide;
using Raven.Server.Documents.TransactionMerger.Commands;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Rachis.Commands
{
    public sealed class HardResetToPassiveCommand : MergedTransactionCommand<ClusterOperationContext, ClusterTransaction>
    {
        private readonly RachisConsensus _engine;
        private readonly string _tag;
        private readonly string _topologyId;
        public HardResetToPassiveCommand(RachisConsensus engine, string tag, string topologyId)
        {
            _engine = engine;
            _topologyId = topologyId;
            _tag = tag;
        }

        protected override long ExecuteCmd(ClusterOperationContext context)
        {
            _engine.UpdateNodeTag(context, RachisConsensus.InitialTag);
            var oldTopology = _engine.GetTopology(context);

            var topology = new ClusterTopology(
                _topologyId ?? oldTopology.TopologyId,
                new Dictionary<string, string>
                {
                    [_tag] = _engine.Url
                },
                new Dictionary<string, string>(),
                new Dictionary<string, string>(),
                _tag,
                _engine.GetLastEntryIndex(context) + 1
            );

            if (_topologyId != oldTopology.TopologyId)
                // if we are going to add this to a different cluster we must get a snapshot
                _engine.SetSnapshotRequest(context, true);

            RachisConsensus.SetTopology(_engine, context, topology);

            _engine.SetNewStateInTx(context, RachisState.Passive, null, _engine.CurrentTermIn(context), "Hard reset to passive by admin");

            return 1;
        }

        public override IReplayableCommandDto<ClusterOperationContext, ClusterTransaction, MergedTransactionCommand<ClusterOperationContext, ClusterTransaction>> ToDto(ClusterOperationContext context)
        {
            throw new NotImplementedException();
        }
    }


}

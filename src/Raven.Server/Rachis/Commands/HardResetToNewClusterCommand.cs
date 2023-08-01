using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Raven.Client.Http;
using Raven.Server.Documents.TransactionMerger.Commands;
using Raven.Server.ServerWide.Context;
using Voron.Impl;

namespace Raven.Server.Rachis.Commands
{
    public sealed class HardResetToNewClusterCommand : MergedTransactionCommand<ClusterOperationContext, ClusterTransaction>
    {
        private readonly RachisConsensus _engine;
        private readonly string _tag;
        private readonly string _topologyId;
        public bool Committed { get; private set; }

        public HardResetToNewClusterCommand(RachisConsensus engine, string tag, string topologyId)
        {
            _engine = engine;
            _topologyId = topologyId;
            _tag = tag;
            Committed = false;
        }

        protected override long ExecuteCmd(ClusterOperationContext context)
        {
            var topology = new ClusterTopology(
                _topologyId,
                new Dictionary<string, string>
                {
                    [_tag] = _engine.Url
                },
                new Dictionary<string, string>(),
                new Dictionary<string, string>(),
                _tag,
                _engine.GetLastEntryIndex(context) + 1
            );

            _engine.UpdateNodeTag(context, _tag);

            RachisConsensus.SetTopology(_engine, context, topology);

            _engine.SetSnapshotRequest(context, false);

            _engine.SwitchToSingleLeader(context);

            context.Transaction.InnerTransaction.LowLevelTransaction.OnDispose += tx =>
            {
                if (tx is LowLevelTransaction llt && llt.Committed)
                {
                    Committed = true;
                }
            };

            return 1;
        }

        public override IReplayableCommandDto<ClusterOperationContext, ClusterTransaction, MergedTransactionCommand<ClusterOperationContext, ClusterTransaction>> ToDto(ClusterOperationContext context)
        {
            throw new NotImplementedException();
        }
    }

}

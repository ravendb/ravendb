using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Raven.Client.ServerWide;
using Raven.Server.Documents.TransactionMerger.Commands;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Rachis.Commands
{
    public sealed class SetNewStateCommand : MergedTransactionCommand<ClusterOperationContext, ClusterTransaction>
    {
        private readonly RachisConsensus _engine;
        private readonly RachisState _rachisState;
        private readonly IDisposable _parent;
        private readonly long _expectedTerm;
        private readonly string _stateChangedReason;
        private readonly Action _beforeStateChangedEvent;
        private readonly bool _disposeAsync;

        public SetNewStateCommand(RachisConsensus engine,
            RachisState rachisState,
            IDisposable parent,
            long expectedTerm,
            string stateChangedReason,
            Action beforeStateChangedEvent = null,
            bool disposeAsync = true)
        {
            _engine = engine;
            _rachisState = rachisState;
            _parent = parent;
            _expectedTerm = expectedTerm;
            _stateChangedReason = stateChangedReason;
            _beforeStateChangedEvent = beforeStateChangedEvent;
            _disposeAsync = disposeAsync;
        }

        protected override long ExecuteCmd(ClusterOperationContext context)
        {
            _engine.SetNewStateInTx(context, _rachisState, _parent, _expectedTerm, _stateChangedReason, _beforeStateChangedEvent, _disposeAsync);
            return 1;
        }

        public override IReplayableCommandDto<ClusterOperationContext, ClusterTransaction, MergedTransactionCommand<ClusterOperationContext, ClusterTransaction>> ToDto(ClusterOperationContext context)
        {
            throw new NotImplementedException();
        }
    }

}

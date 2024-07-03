using System;
using JetBrains.Annotations;
using Raven.Server.Documents.TransactionMerger.Commands;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Rachis.Commands;

public sealed class CastVoteInTermCommand : MergedTransactionCommand<ClusterOperationContext, ClusterTransaction>
{
    private readonly RachisConsensus _engine;
    private readonly long _term;
    private readonly string _reason;

    public CastVoteInTermCommand([NotNull] RachisConsensus engine, long term, string reason)
    {
        _engine = engine ?? throw new ArgumentNullException(nameof(engine));
        _term = term;
        _reason = reason;
    }

    protected override long ExecuteCmd(ClusterOperationContext context)
    {
        // we check it here again because now we are under the tx lock, so we can't get into concurrency issues
        if (_term <= _engine.CurrentTermIn(context))
            return 1;

        _engine.CastVoteInTerm(context, _term, votedFor: null, reason: _reason);

        return 1;
    }

    public override IReplayableCommandDto<ClusterOperationContext, ClusterTransaction, MergedTransactionCommand<ClusterOperationContext, ClusterTransaction>> ToDto(ClusterOperationContext context)
    {
        throw new NotImplementedException();
    }

}

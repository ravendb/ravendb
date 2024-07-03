using System;
using JetBrains.Annotations;
using Raven.Server.Documents.TransactionMerger.Commands;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Rachis.Commands;

public sealed class ElectorCastVoteInTermCommand : MergedTransactionCommand<ClusterOperationContext, ClusterTransaction>
{
    private readonly RachisConsensus _engine;
    private readonly RequestVote _requestVote;

    public ElectorCastVoteInTermCommand([NotNull] RachisConsensus engine, [NotNull] RequestVote requestVote)
    {
        _engine = engine ?? throw new ArgumentNullException(nameof(engine));
        _requestVote = requestVote ?? throw new ArgumentNullException(nameof(requestVote));
    }

    protected override long ExecuteCmd(ClusterOperationContext context)
    {
        // double checking things under the transaction lock
        if (_requestVote.Term > _engine.CurrentTermIn(context) + 1)
        {
            _engine.CastVoteInTerm(context, _requestVote.Term - 1, null, "Noticed that the term in the cluster grew beyond what I was familiar with, increasing it");
        }

        return 1;
    }

    public override IReplayableCommandDto<ClusterOperationContext, ClusterTransaction, MergedTransactionCommand<ClusterOperationContext, ClusterTransaction>> ToDto(ClusterOperationContext context)
    {
        throw new NotImplementedException();
    }
}

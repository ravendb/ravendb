using System;
using JetBrains.Annotations;
using Raven.Server.Documents.TransactionMerger.Commands;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Rachis.Commands;

public sealed class CandidateCastVoteInTermCommand([NotNull] RachisConsensus engine, long electionTerm, string reason)
    : MergedTransactionCommand<ClusterOperationContext, ClusterTransaction>
{
    private readonly RachisConsensus _engine = engine ?? throw new ArgumentNullException(nameof(engine));

    protected override long ExecuteCmd(ClusterOperationContext context)
    {
        _engine.CastVoteInTerm(context, electionTerm, _engine.Tag, reason);
        return 1;
    }

    public override IReplayableCommandDto<ClusterOperationContext, ClusterTransaction, MergedTransactionCommand<ClusterOperationContext, ClusterTransaction>> ToDto(ClusterOperationContext context)
    {
        throw new NotImplementedException();
    }
}

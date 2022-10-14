using System;
using JetBrains.Annotations;
using Raven.Server.Documents.TransactionMerger.Commands;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;

namespace Raven.Server.Rachis.Commands;

internal class ElectorCastVoteInTermWithShouldGrantVoteCommand : MergedTransactionCommand<ClusterOperationContext, ClusterTransaction>
{
    private readonly RachisConsensus _engine;
    private readonly Elector _elector;
    private readonly RequestVote _requestVote;
    private readonly long _lastLogIndex;

    public Elector.HandleVoteResult VoteResult { get; private set; }

    public ElectorCastVoteInTermWithShouldGrantVoteCommand([NotNull] RachisConsensus engine, [NotNull] Elector elector, [NotNull] RequestVote requestVote, long lastLogIndex)
    {
        _engine = engine ?? throw new ArgumentNullException(nameof(engine));
        _elector = elector ?? throw new ArgumentNullException(nameof(elector));
        _requestVote = requestVote ?? throw new ArgumentNullException(nameof(requestVote));
        _lastLogIndex = lastLogIndex;
    }

    protected override long ExecuteCmd(ClusterOperationContext context)
    {
        VoteResult = _elector.ShouldGrantVote(context, _lastLogIndex, _requestVote);
        if (VoteResult.DeclineVote == false)
            _engine.CastVoteInTerm(context, _requestVote.Term, _requestVote.Source, "Casting vote as elector");

        return 1;
    }

    public override IReplayableCommandDto<ClusterOperationContext, ClusterTransaction, MergedTransactionCommand<ClusterOperationContext, ClusterTransaction>> ToDto(JsonOperationContext context)
    {
        throw new System.NotImplementedException();
    }
}

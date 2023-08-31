using System;
using JetBrains.Annotations;
using Raven.Server.Documents.TransactionMerger.Commands;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Rachis.Commands;

internal sealed class ElectorCastVoteInTermWithShouldGrantVoteCommand : MergedTransactionCommand<ClusterOperationContext, ClusterTransaction>
{
    private readonly RachisConsensus _engine;
    private readonly RequestVote _requestVote;
    private readonly long _lastLogIndex;

    public Elector.HandleVoteResult VoteResult { get; private set; }

    public ElectorCastVoteInTermWithShouldGrantVoteCommand([NotNull] RachisConsensus engine, [NotNull] RequestVote requestVote, long lastLogIndex)
    {
        _engine = engine ?? throw new ArgumentNullException(nameof(engine));
        _requestVote = requestVote ?? throw new ArgumentNullException(nameof(requestVote));
        _lastLogIndex = lastLogIndex;
    }

    protected override long ExecuteCmd(ClusterOperationContext context)
    {
        VoteResult = ShouldGrantVote(context, _lastLogIndex, _requestVote);
        if (VoteResult.DeclineVote == false)
            _engine.CastVoteInTerm(context, _requestVote.Term, _requestVote.Source, "Casting vote as elector");

        return 1;
    }

    private Elector.HandleVoteResult ShouldGrantVote(ClusterOperationContext context, long lastIndex, RequestVote rv)
    {
        var result = new Elector.HandleVoteResult();
        var lastLogIndexUnderWriteLock = _engine.GetLastEntryIndex(context);
        var lastLogTermUnderWriteLock = _engine.GetTermFor(context, lastLogIndexUnderWriteLock);
    
        if (lastLogIndexUnderWriteLock != lastIndex)
        {
            result.DeclineVote = true;
            result.DeclineReason = "Log was changed";
            return result;
        }
    
        if (lastLogTermUnderWriteLock > rv.LastLogTerm)
        {
            result.DeclineVote = true;
            result.DeclineReason = $"My last log term {lastLogTermUnderWriteLock}, is higher than yours {rv.LastLogTerm}.";
            return result;
        }
    
        if (lastLogIndexUnderWriteLock > rv.LastLogIndex)
        {
            result.DeclineVote = true;
            result.DeclineReason = $"Vote declined because my last log index {lastLogIndexUnderWriteLock} is more up to date than yours {rv.LastLogIndex}";
            return result;
        }
    
        var (whoGotMyVoteIn, votedTerm) = _engine.GetWhoGotMyVoteIn(context, rv.Term);
        result.VotedTerm = votedTerm;
    
        if (whoGotMyVoteIn != null && whoGotMyVoteIn != rv.Source)
        {
            result.DeclineVote = true;
            result.DeclineReason = $"Already voted in {rv.LastLogTerm}, for {whoGotMyVoteIn}";
            return result;
        }
    
        if (votedTerm >= rv.Term)
        {
            result.DeclineVote = true;
            result.DeclineReason = $"Already voted in {rv.LastLogTerm}, for another node in higher term: {votedTerm}";
            return result;
        }
    
        return result;
    }

    public override IReplayableCommandDto<ClusterOperationContext, ClusterTransaction, MergedTransactionCommand<ClusterOperationContext, ClusterTransaction>> ToDto(ClusterOperationContext context)
    {
        throw new NotImplementedException();
    }
}

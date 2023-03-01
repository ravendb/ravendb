using System;
using Jint;
using System.Drawing;
using JetBrains.Annotations;
using Raven.Server.Documents.TransactionMerger.Commands;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;

namespace Raven.Server.Rachis.Commands;

public class CandidateCastVoteInTermCommand : MergedTransactionCommand<ClusterOperationContext, ClusterTransaction>
{
    private readonly Candidate _candidate;
    private readonly RachisConsensus _engine;
    private readonly long _electionTerm;
    private readonly string _reason;

    public CandidateCastVoteInTermCommand([NotNull] Candidate candidate, [NotNull] RachisConsensus engine, long electionTerm, string reason)
    {
        _engine = engine ?? throw new ArgumentNullException(nameof(engine));
        _electionTerm = electionTerm;
        _reason = reason;
        _candidate = candidate;
    }

    protected override long ExecuteCmd(ClusterOperationContext context)
    {
        _engine.CastVoteInTerm(context, _electionTerm, _engine.Tag, _reason);
        _candidate.ElectionTerm = _candidate.RunRealElectionAtTerm = _electionTerm;
        return 1;
    }

    public override IReplayableCommandDto<ClusterOperationContext, ClusterTransaction, MergedTransactionCommand<ClusterOperationContext, ClusterTransaction>> ToDto(ClusterOperationContext context)
    {
        throw new NotImplementedException();
    }
}

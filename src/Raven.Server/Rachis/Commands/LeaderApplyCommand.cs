using System;
using System.Diagnostics;
using JetBrains.Annotations;
using Raven.Server.Documents.TransactionMerger.Commands;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Rachis.Commands;

public sealed class LeaderApplyCommand : MergedTransactionCommand<ClusterOperationContext, ClusterTransaction>
{
    private readonly RachisConsensus _engine;
    private readonly long _lastCommit;
    private readonly long _maxIndexOnQuorum;
    private readonly Leader _leader;

    public long LastAppliedCommit { get; private set; }

    public LeaderApplyCommand([NotNull] Leader leader, [NotNull] RachisConsensus engine, long lastCommit, long maxIndexOnQuorum)
    {
        _leader = leader ?? throw new ArgumentNullException(nameof(leader));
        _engine = engine ?? throw new ArgumentNullException(nameof(engine));
        _lastCommit = lastCommit;
        _maxIndexOnQuorum = maxIndexOnQuorum;
    }

    protected override long ExecuteCmd(ClusterOperationContext context)
    {
        var sw = Stopwatch.StartNew();

        _engine.TakeOffice(context);

        LastAppliedCommit = _engine.Apply(context, _maxIndexOnQuorum, _leader, sw);
        var elapsed = sw.Elapsed;
        if (RachisStateMachine.EnableDebugLongCommit && elapsed > TimeSpan.FromSeconds(5))
            Console.WriteLine($"Commiting from {_lastCommit} to {LastAppliedCommit} took {elapsed}");

        return 1;
    }

    public override IReplayableCommandDto<ClusterOperationContext, ClusterTransaction, MergedTransactionCommand<ClusterOperationContext, ClusterTransaction>> ToDto(ClusterOperationContext context)
    {
        throw new NotImplementedException();
    }

}

using System;
using Raven.Server.Documents.TransactionMerger.Commands;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;

namespace Raven.Server.Rachis.Commands;

public sealed class InsertToLeaderLogCommand : MergedTransactionCommand<ClusterOperationContext, ClusterTransaction>
{
    private readonly RachisConsensus _engine;
    private long _term;
    private BlittableJsonReaderObject _raftCommand;
    private RachisEntryFlags _flags;

    public long Index = -1;

    public InsertToLeaderLogCommand(RachisConsensus engine, long term, BlittableJsonReaderObject cmd, RachisEntryFlags flags)
    {
        _engine = engine;
        _term = term;
        _raftCommand = cmd;
        _flags = flags;
    }

    protected override long ExecuteCmd(ClusterOperationContext context)
    {
        Index = _engine.InsertToLeaderLog(context, _term, _raftCommand, _flags);
        return 1;
    }

    public override IReplayableCommandDto<ClusterOperationContext, ClusterTransaction, MergedTransactionCommand<ClusterOperationContext, ClusterTransaction>> ToDto(ClusterOperationContext context)
    {
        throw new NotImplementedException();
    }
}

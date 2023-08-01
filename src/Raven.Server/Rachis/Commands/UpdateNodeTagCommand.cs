using System;
using JetBrains.Annotations;
using Raven.Server.Documents.TransactionMerger.Commands;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;

namespace Raven.Server.Rachis.Commands;

public sealed class UpdateNodeTagCommand : MergedTransactionCommand<ClusterOperationContext, ClusterTransaction>
{
    private readonly RachisConsensus _engine;
    private readonly string _tag;
    private readonly RachisHello _initialMessage;

    public UpdateNodeTagCommand([NotNull] RachisConsensus engine, [NotNull] string tag, [NotNull] RachisHello initialMessage)
    {
        _engine = engine ?? throw new ArgumentNullException(nameof(engine));
        _tag = tag ?? throw new ArgumentNullException(nameof(tag));
        _initialMessage = initialMessage ?? throw new ArgumentNullException(nameof(initialMessage));
    }

    protected override long ExecuteCmd(ClusterOperationContext context)
    {
        if (_tag == RachisConsensus.InitialTag)// double checked locking under tx write lock
            _engine.UpdateNodeTag(context, _initialMessage.DebugDestinationIdentifier);

        return 1;
    }

    public override IReplayableCommandDto<ClusterOperationContext, ClusterTransaction, MergedTransactionCommand<ClusterOperationContext, ClusterTransaction>> ToDto(ClusterOperationContext context)
    {
        throw new NotImplementedException();
    }
}

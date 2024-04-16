using System;
using Raven.Client.ServerWide;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents;

public record BackgroundWorkParameters(DocumentsOperationContext Context, DateTime CurrentTime, DatabaseTopology DatabaseTopology, string NodeTag, long AmountToTake, long MaxItemsToProcess = int.MaxValue)
{
    public readonly DocumentsOperationContext Context = Context;
    public readonly DateTime CurrentTime = CurrentTime;
    public readonly DatabaseTopology DatabaseTopology = DatabaseTopology;
    public readonly string NodeTag = NodeTag;
    public readonly long AmountToTake = AmountToTake;
    public readonly long MaxItemsToProcess = MaxItemsToProcess;
}

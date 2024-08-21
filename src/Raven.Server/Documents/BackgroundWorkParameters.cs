using System;
using Raven.Client.ServerWide;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents;

public record BackgroundWorkParameters(DocumentsOperationContext Context, DateTime CurrentTime, DatabaseRecord DatabaseRecord, string NodeTag, long AmountToTake, long MaxItemsToProcess = int.MaxValue)
{
    public readonly DocumentsOperationContext Context = Context;
    public readonly DateTime CurrentTime = CurrentTime;
    public readonly DatabaseRecord DatabaseRecord = DatabaseRecord;
    public readonly string NodeTag = NodeTag;
    public readonly long AmountToTake = AmountToTake;
    public readonly long MaxItemsToProcess = MaxItemsToProcess;
}

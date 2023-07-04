using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Subscriptions.Processor;

public interface IDatabaseSubscriptionProcessor
{
    public SubscriptionPatchDocument Patch { get; set; }

    long GetLastItemEtag(DocumentsOperationContext context, string collection);
}

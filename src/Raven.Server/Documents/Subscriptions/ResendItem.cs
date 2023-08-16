using Raven.Server.ServerWide.Commands.Subscriptions;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.Subscriptions;

public sealed class ResendItem : IDynamicJson
{
    public string Id;
    public long Batch;
    public string ChangeVector;
    public SubscriptionType Type;
    public long SubscriptionId;

    public DynamicJsonValue ToJson()
    {
        return new DynamicJsonValue
        {
            [nameof(Id)] = Id,
            [nameof(Batch)] = Batch,
            [nameof(ChangeVector)] = ChangeVector,
            [nameof(Type)] = Type.ToString(),
            [nameof(SubscriptionId)] = SubscriptionId
        };
    }
}

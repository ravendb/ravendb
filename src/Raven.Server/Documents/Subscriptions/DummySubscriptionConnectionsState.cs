using System.Threading;
using Raven.Client.Documents.Subscriptions;
using Raven.Server.Documents.TcpHandlers;

namespace Raven.Server.Documents.Subscriptions;

public class DummySubscriptionConnectionsState : SubscriptionConnectionsState
{
    // for subscription test only
    public DummySubscriptionConnectionsState(string name, DocumentsStorage storage, SubscriptionState state) : base(name, -0x42, storage.DocumentDatabase.SubscriptionStorage)
    {
        _subscriptionName = "dummy";

        LastChangeVectorSent = state.ChangeVectorForNextBatchStartingPoint;
        PreviouslyRecordedChangeVector = LastChangeVectorSent;
        Query = state.Query;
    }

    public override void Initialize(SubscriptionConnection connection, bool afterSubscribe = false)
    {
    }
}

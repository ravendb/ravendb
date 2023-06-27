using System;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents.Subscriptions;
using Raven.Client.ServerWide.Tcp;
using Raven.Server.Documents.TcpHandlers;
using Sparrow;

namespace Raven.Server.Documents.Subscriptions;

public interface ISubscriptionConnection : IDisposable
{
    public const long NonExistentBatch = -1;
    public static readonly StringSegment TypeSegment = new StringSegment("Type");

    SubscriptionWorkerOptions Options { get; }

    TcpConnectionOptions TcpConnection { get; }

    SubscriptionState SubscriptionState { get; set; }

    SubscriptionConnection.ParsedSubscription Subscription { get; }

    CancellationTokenSource CancellationTokenSource { get; }

    Task SubscriptionConnectionTask { get; set; }

    long SubscriptionId { get; }

    TcpConnectionHeaderMessage.SupportedFeatures SupportedFeatures { get; }

    public const int WaitForChangedDocumentsTimeoutInMs = 3000;
}

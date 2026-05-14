// Expected diagnostic: RVN011 — "Use batch.OpenSession inside a subscription Run delegate"
// Inside a subscription worker Run delegate, sessions must be opened via the batch object
// (batch.OpenSession() / batch.OpenAsyncSession()), not via the document store. Using the
// store bypasses the batch's internal transaction tracking, which means the session will
// not participate in the batch acknowledgement and documents may be re-processed.
//
// Fix: replace store.OpenSession() with batch.OpenSession().
// Code fix available: Alt+Enter / Ctrl+. on the squiggle applies the fix automatically.
using System.Linq;
using Raven.Client.Documents;
using Raven.Client.Documents.Subscriptions;

namespace Raven.Analyzers.Playground.Examples;

public class RVN011_SubscriptionService
{
    private readonly IDocumentStore _store;

    public RVN011_SubscriptionService(IDocumentStore store)
    {
        _store = store;
    }

    public void BadSubscription(SubscriptionWorker<RVN011_Order> worker)
    {
        worker.Run(batch =>
        {
            // warning RVN011: 'OpenSession' is called on an IDocumentStore inside a
            //   subscription Run lambda. Use batch.OpenSession() instead.
            using var session = _store.OpenSession();
            foreach (var item in batch.Items)
                session.Store(new RVN011_ProcessedOrder { SourceId = item.Result.Id });
            session.SaveChanges();
        });
    }

    public void GoodSubscription(SubscriptionWorker<RVN011_Order> worker)
    {
        worker.Run(batch =>
        {
            // Correct: open the session from the batch
            using var session = batch.OpenSession();
            foreach (var item in batch.Items)
                session.Store(new RVN011_ProcessedOrder { SourceId = item.Result.Id });
            session.SaveChanges();
        });
    }
}

public class RVN011_Order { public string Id { get; set; } = default!; }
public class RVN011_ProcessedOrder { public string SourceId { get; set; } = default!; }

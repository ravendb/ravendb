using System;
using System.Collections.Generic;

namespace Raven.Server.Documents.Subscriptions.Sharding;

public sealed class ShardDocumentSubscriptionFetcher : DocumentSubscriptionFetcher
{
    public ShardDocumentSubscriptionFetcher(DocumentDatabase database, SubscriptionConnectionsState subscriptionConnectionsState, string collection) :
        base(database, subscriptionConnectionsState, collection)
    {
    }

    protected override IEnumerator<Document> FetchFromResend()
    {
        var records = new SortedDictionary<long, Document>();
        foreach (var document in base.FetchFromResend())
        {
            var current = Database.DocumentsStorage.Get(DocsContext, document.Id, fields: DocumentFields.Default, throwOnConflict: false);
            using (current)
            {
                if (current == null)
                {
                    document.Dispose();
                    // items from another shard or might be already deleted
                    continue;
                }

                records.Add(current.Etag, document);
                DocsContext.Transaction.ForgetAbout(current);
            }
        }

        foreach (var kvp in records)
            yield return kvp.Value;
    }
}

using System.Collections.Generic;

namespace Raven.Server.Documents.Subscriptions.Sharding;

public class ShardDocumentSubscriptionFetcher : DocumentSubscriptionFetcher
{
    public ShardDocumentSubscriptionFetcher(DocumentDatabase database, SubscriptionConnectionsState subscriptionConnectionsState, string collection) :
        base(database, subscriptionConnectionsState, collection)
    {
    }

    protected override IEnumerable<Document> FetchFromResend()
    {
        var records = new SortedDictionary<long, Document>();
        foreach (var document in base.FetchFromResend())
        {
            var current = Database.DocumentsStorage.GetDocumentOrTombstone(DocsContext, document.Id, throwOnConflict: false);
            using (current.Document)
            using (current.Tombstone)
            {
                if (current.Missing || current.Document == null)
                {
                    document.Dispose();
                    // items from another shard or might be already deleted
                    continue;
                }

                records.Add(current.Document.Etag, document);
            }
        }

        foreach (var kvp in records)
            yield return kvp.Value;
    }
}

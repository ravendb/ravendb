using Raven.Client.Documents.Indexes;

namespace Raven.Quill.Metrics;

// Orders the conversation read-model by recency so the list pages newest-active-first server-side.
// Raw map over the @-collection (like ConversationMetricsIndex over @conversations) — a typed
// AbstractIndexCreationTask<T> would map T's convention collection, not the forced @ one.
internal sealed class ConversationPreviewIndex : AbstractIndexCreationTask
{
    public override string IndexName => "ConversationPreviewsIndex";

    public override IndexDefinition CreateIndexDefinition()
    {
        return new IndexDefinition
        {
            Maps =
            {
                """
                from p in docs.@ConversationPreviews 
                select new { 
                    p.LastMessageAt,
                    p.Agent,
                }
                """
            }
        };
    }
}

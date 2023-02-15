namespace Raven.Server.Documents.Sharding.Queries;

public static class ShardedQueryResultDocumentExtensions
{
    public static ShardedQueryResultDocument EnsureDataHashInQueryResultMetadata(this Document doc)
    {
        if (doc is ShardedQueryResultDocument result == false)
            result = ShardedQueryResultDocument.From(doc);

        result.ResultDataHash = doc.DataHash;

        return result;
    }
}

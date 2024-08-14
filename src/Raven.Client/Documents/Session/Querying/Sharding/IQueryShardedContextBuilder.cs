using System.Collections.Generic;

namespace Raven.Client.Documents.Session.Querying.Sharding;

public interface IQueryShardedContextBuilder
{
    /// <summary>
    /// Restricts the query execution to the specific shard where the document with the given ID resides.
    /// This method ensures that the query is directed only to the shard that holds the document,
    /// optimizing query performance by eliminating the need to query all shards.
    /// </summary>
    /// <param name="id">The ID of the document used to determine the shard to query</param>
    IQueryShardedContextBuilder ByDocumentId(string id);

    /// <summary>
    /// Restricts the query execution to the specific shards where the documents with the given IDs reside.
    /// This method ensures that the query is directed only to the shards that hold the specified documents,
    /// optimizing query performance by avoiding unnecessary queries to other shards.
    /// </summary>
    /// <param name="ids">A set of document IDs used to determine the shards to query</param>
    IQueryShardedContextBuilder ByDocumentIds(IEnumerable<string> ids);

    /// <summary>
    /// Restricts the query execution to only the shards associated with the specified prefix, as defined in the database's <see cref="PrefixedShardingSetting"/>.
    /// This allows the query to be executed on a subset of shards, avoiding unnecessary queries to shards not relevant to the specified prefix.
    /// </summary>
    /// <param name="prefix">The document ID prefix used to determine which shards to query.
    /// This prefix corresponds to the 'PrefixSetting' configured in the database record,
    /// and only the shards associated with this prefix will be included in the query execution.</param>
    IQueryShardedContextBuilder ByPrefix(string prefix);

    /// <summary>
    /// Restricts the query execution to only the shards associated with the specified prefixes, as defined in the database's <see cref="PrefixedShardingSetting"/>.
    /// This allows the query to be executed on a subset of shards, avoiding unnecessary queries to shards not relevant to the specified prefixes.
    /// </summary>
    /// <param name="prefixes">A collection of document ID prefixes used to determine which shards to query.
    /// Each prefix corresponds to a 'PrefixSetting' configured in the database record, and only the shards 
    /// associated with these prefixes will be included in the query execution.</param>

    IQueryShardedContextBuilder ByPrefixes(IEnumerable<string> prefixes);

}

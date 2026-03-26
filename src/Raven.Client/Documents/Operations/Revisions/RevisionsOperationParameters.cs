using System;
using System.Collections.Generic;

namespace Raven.Client.Documents.Operations.Revisions;

/// <summary>
/// Base class for revisions operation parameters, providing the common resume-from properties
/// shared by <see cref="EnforceRevisionsConfigurationOperation.Parameters"/> and
/// <see cref="AdoptOrphanedRevisionsOperation.Parameters"/>.
/// </summary>
public class RevisionsOperationParameters
{
    /// <summary>
    /// Gets or sets the collections the operation applies to.
    /// If <c>null</c>, the operation applies to all collections.
    /// </summary>
    public string[] Collections { get; set; } = null;

    /// <summary>
    /// Optional continuation state from a previous run.
    /// Populate from the completed operation's <see cref="OperationResult.LastProcessedEtags"/>,
    /// <see cref="OperationResult.EtagBarriersUsed"/>, and <see cref="OperationResult.NodeTags"/>
    /// to resume where it left off.
    /// <para>
    /// <b>Important:</b> When resuming, the operation must target the same database node as the
    /// original run because etags are node-local. Use <c>store.Maintenance.ForNode(nodeTag)</c>
    /// (or <c>store.Operations.ForNode(nodeTag)</c>) to pin the request.
    /// </para>
    /// </summary>
    public RevisionsOperationContinuationParameters ContinuationParameters { get; set; }

    /// <summary>
    /// Resolves the per-database operation parameters for a single shard/database.
    /// </summary>
    /// <param name="databaseName">The name of the target database or shard.</param>
    /// <param name="generateNextEtag">
    /// Called lazily to produce a fresh etag barrier when <see cref="RevisionsOperationContinuationParameters.EtagBarriers"/>
    /// does not contain an entry for <paramref name="databaseName"/>.
    /// </param>
    /// <param name="currentNodeTag">
    /// The node tag of the server currently executing the operation. Used to validate that the
    /// resumed operation is running on the same node that produced the continuation etags.
    /// </param>
    internal (HashSet<string> Collections, long StartFromEtag, long EtagBarrier) Resolve(string databaseName, Func<long> generateNextEtag)
    {
        var collections = Collections?.Length > 0 ? new HashSet<string>(Collections, StringComparer.OrdinalIgnoreCase) : null;
        var etagBarrier = ContinuationParameters?.EtagBarriers?.TryGetValue(databaseName, out var currentBarrier) == true ? currentBarrier : generateNextEtag();
        var startFromEtag = ContinuationParameters?.StartFromEtags?.TryGetValue(databaseName, out var currentStartEtag) == true ? currentStartEtag : etagBarrier;

        return (collections, startFromEtag, etagBarrier);
    }

    internal void Validate(string database, string currentNodeTag)
    {
        var expectedNodeTag = GetNodeTag(database);
        if (expectedNodeTag == null)
        {
            if (ContinuationParameters == null)
                return; // new operation, no expectation about the node
        }

        if (string.Equals(expectedNodeTag, currentNodeTag, StringComparison.OrdinalIgnoreCase) == false)
        {
            throw new InvalidOperationException(
                $"This revisions operation was previously executed on node '{expectedNodeTag}' for database '{database}', " +
                $"but is now running on node '{currentNodeTag}'. Resuming on a different node produces incorrect results " +
                $"because etags are node-local. Use store.Operations.ForNode(\"{expectedNodeTag}\") or " +
                $"store.Maintenance.ForNode(\"{expectedNodeTag}\") to route the request to the correct node.");
        }

        if (ContinuationParameters?.StartFromEtags?.ContainsKey(database) is not true)
            throw new InvalidOperationException(
                $"Continuation parameters for database '{database}' are missing a starting etag.");

        if (ContinuationParameters?.EtagBarriers?.ContainsKey(database) is not true)
            throw new InvalidOperationException(
                $"Continuation parameters for database '{database}' are missing an etag barrier. " +
                $"Ensure that the operation is resumed with the same etag barrier as the original run.");
    }

    internal string GetNodeTag(string databaseName)
    {
        if (ContinuationParameters?.NodeTags?.TryGetValue(databaseName, out var node) == true)
            return node;

        return null;
    }
}

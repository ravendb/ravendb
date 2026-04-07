using System.Collections.Generic;

namespace Raven.Client.Documents.Operations.Revisions;

/// <summary>
/// Contains the state needed to resume an interrupted revisions operation (enforce, adopt, or revert)
/// from where a previous run left off.
/// <para>
/// Populate this from the <see cref="OperationResult.LastProcessedEtags"/>,
/// <see cref="OperationResult.EtagBarriersUsed"/>, and <see cref="OperationResult.NodeTags"/>
/// of a completed run.
/// </para>
/// <para>
/// <b>Important:</b> Because etags are node-local, a resumed operation <b>must</b> run on the same
/// database node as the original run. Use <c>store.Maintenance.ForNode(nodeTag)</c> (or
/// <c>store.Operations.ForNode(nodeTag)</c>) to pin the request. When <see cref="NodeTags"/> is
/// populated, the server validates that the operation is executing on the correct node and throws
/// an <see cref="System.InvalidOperationException"/> on mismatch.
/// </para>
/// </summary>
public class RevisionsOperationContinuationParameters
{
    /// <summary>
    /// Starting etags for resuming an interrupted operation, keyed by database name.
    /// For non-sharded databases the key is the database name (e.g. <c>"Northwind"</c>).
    /// For sharded databases each shard uses its own name (e.g. <c>"Northwind$0"</c>).
    /// Populate from <see cref="OperationResult.LastProcessedEtags"/> of the previous run.
    /// </summary>
    public Dictionary<string, long> StartFromEtags { get; set; }

    /// <summary>
    /// Etag barriers from the previous run, keyed by database name.
    /// Must match the <see cref="OperationResult.EtagBarriersUsed"/> from the previous run so that documents
    /// modified between the original barrier and <see cref="StartFromEtags"/> are not incorrectly skipped.
    /// </summary>
    public Dictionary<string, long> EtagBarriers { get; set; }

    /// <summary>
    /// Node tags from the previous run, keyed by database/shard name.
    /// Populate from <see cref="OperationResult.NodeTags"/> of the previous run.
    /// When set, the server validates that the resumed operation is running on the same node
    /// that produced the etags and throws if there is a mismatch.
    /// </summary>
    public Dictionary<string, string> NodeTags { get; set; }
}

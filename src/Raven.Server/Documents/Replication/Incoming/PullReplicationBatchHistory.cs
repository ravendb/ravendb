using System;
using System.Collections.Generic;
using Raven.Server.Documents.Replication.Outgoing;
using Raven.Server.Utils;

namespace Raven.Server.Documents.Replication.Incoming;

public class PullReplicationBatchHistory
{
    private readonly ReplicationLoader _replicationLoader;

    // SinkToHub: hub-side queue mapping hub local etag → confirmed sink source frontier
    // HubToSink: sink-side queue mapping sink local etag → confirmed hub source frontier

    private const int MaxBatchHistorySize = 128;
    private readonly Queue<(long Etag, string ChangeVector)> _batchHistory = [];


    public PullReplicationBatchHistory(ReplicationLoader replicationLoader)
    {
        _replicationLoader = replicationLoader;
    }

    public void Add(long etag, string lastBatchChangeVector)
    {
        if (_batchHistory.Count is MaxBatchHistorySize)
            _batchHistory.Dequeue();

        _batchHistory.Enqueue((etag, lastBatchChangeVector));
    }

    public string ComputeConfirmedChangeVector(string lastBatchChangeVector)
    {
        var confirmedEtag = GetConfirmedMinimalClusterWideReplicatedEtag();
        switch (confirmedEtag)
        {
            case null:
                return null; // not all siblings connected yet, wait
            case long.MaxValue:
            {
                _batchHistory.Clear(); // no siblings, clear history as it isn't needed anymore
                return lastBatchChangeVector; // single-node: trivially confirmed
            }
        }

        string changeVector = null;
        while (_batchHistory.TryPeek(out (long Etag, string ChangeVector) current) && current.Etag <= confirmedEtag.Value)
        {
            _batchHistory.Dequeue();
            changeVector = ChangeVectorUtils.MergeVectors(changeVector, current.ChangeVector);
        }

        return changeVector;
    }

    /// <summary>
    /// Returns the minimum etag that has been confirmed as replicated across all sibling nodes in the cluster,
    /// representing the cluster-wide replication frontier.
    /// <para>
    /// Return values:
    /// <list type="bullet">
    ///   <item><description><c>long.MaxValue</c> — single-node topology (no siblings). Data is trivially confirmed; callers should use the last batch change vector directly.</description></item>
    ///   <item><description><c>null</c> — not all siblings have established an active connection yet. Callers should wait before confirming.</description></item>
    ///   <item><description>Any other value — the minimum etag confirmed by every sibling. Safe to advance the cursor up to this point.</description></item>
    /// </list>
    /// </para>
    /// </summary>
    private long? GetConfirmedMinimalClusterWideReplicatedEtag()
    {
        var numberOfSiblings = _replicationLoader.NumberOfSiblingsInInternalReplication;
        if (numberOfSiblings == 0)
            return long.MaxValue; // single-node topology: trivially confirmed, confirm immediately

        long min = long.MaxValue;
        int count = 0;

        foreach (var handler in _replicationLoader.OutgoingHandlers)
        {
            if (handler is not OutgoingInternalReplicationHandler)
                continue;

            count++;
            min = Math.Min(min, handler.LastSentDocumentEtag);
        }

        return count == numberOfSiblings ? min : null; // null = not all siblings connected yet, wait
    }
}

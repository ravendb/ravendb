using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Http;
using Raven.Server.Documents.TransactionMerger.Commands;
using Raven.Server.Rachis.Remote;
using Raven.Server.ServerWide.Context;
using Voron.Impl;

namespace Raven.Server.Rachis.Commands;
public sealed class FollowerReadAndCommitSnapshotCommand : MergedTransactionCommand<ClusterOperationContext, ClusterTransaction>
{
    private readonly RachisConsensus _engine;
    private readonly Follower _follower;
    private readonly InstallSnapshot _snapshot;

    private readonly Stream _stream;
    SnapshotReadAndCommitType _type;

    private readonly CancellationToken _token;

    public Task OnFullSnapshotInstalledTask { get; private set; }

    public FollowerReadAndCommitSnapshotCommand([NotNull] RachisConsensus engine, Follower follower, [NotNull] InstallSnapshot snapshot,
        Stream stream,  SnapshotReadAndCommitType type,
        CancellationToken token)
    {
        _engine = engine ?? throw new ArgumentNullException(nameof(engine));
        _follower = follower;
        _snapshot = snapshot ?? throw new ArgumentNullException(nameof(snapshot));
        _stream = stream;
        _type = type;
        _token = token;
    }

    protected override long ExecuteCmd(ClusterOperationContext context)
    {
        if(_type == SnapshotReadAndCommitType.Apply)
            OnFullSnapshotInstalledTask = ApplySnapshot(context, _snapshot, _stream, _token);
        else if(_type == SnapshotReadAndCommitType.ValidateLastIncludedIndex)
            ValidateSnapshotLastIncludedIndex(context, _snapshot);

        // snapshot always has the latest topology
        if (_snapshot.Topology == null)
        {
            const string message = "Expected to get topology on snapshot";
            if (_engine.Log.IsInfoEnabled)
            {
                _engine.Log.Info($"{ToString()}: {message}");
            }

            throw new InvalidOperationException(message);
        }

        using (var topologyJson = context.ReadObject(_snapshot.Topology, "topology"))
        {
            if (_engine.Log.IsInfoEnabled)
            {
                _engine.Log.Info($"{ToString()}: topology on install snapshot: {topologyJson}");
            }

            var topology = JsonDeserializationRachis<ClusterTopology>.Deserialize(topologyJson);

            RachisConsensus.SetTopology(_engine, context, topology);
        }

        _engine.SetSnapshotRequest(context, false);

        context.Transaction.InnerTransaction.LowLevelTransaction.OnDispose += t =>
        {
            if (t is LowLevelTransaction llt && llt.Committed)
            {
                // we might have moved from passive node, so we need to start the timeout clock
                _engine.Timeout.Start(_engine.SwitchToCandidateStateOnTimeout);
            }
        };

        return 1;
    }

    private Task ApplySnapshot(ClusterOperationContext context, InstallSnapshot snapshot, Stream stream, CancellationToken token)
    {
        var recorder = _follower.DebugRecorder;

        recorder.Record("Start applying the snapshot");

        var txw = context.Transaction.InnerTransaction;
        using (var fileReader = new StreamSnapshotReader(recorder, stream))
        {
            _follower.ReadSnapshot(fileReader, context, txw, dryRun: false, token);
        }

        recorder.Record("Finished applying the snapshot");

        if (_engine.Log.IsInfoEnabled)
        {
            _engine.Log.Info(
                $"{ToString()}: Installed snapshot with last index={snapshot.LastIncludedIndex} with LastIncludedTerm={snapshot.LastIncludedTerm} ");
        }

        _engine.SetLastCommitIndex(context, snapshot.LastIncludedIndex, snapshot.LastIncludedTerm);
        _engine.ClearLogEntriesAndSetLastTruncate(context, snapshot.LastIncludedIndex, snapshot.LastIncludedTerm);

        return _engine.OnSnapshotInstalled(context, snapshot.LastIncludedIndex, token);
    }

    private void ValidateSnapshotLastIncludedIndex(ClusterOperationContext context, InstallSnapshot snapshot)
    {
        var lastEntryIndex = _engine.GetLastEntryIndex(context);
        if (lastEntryIndex < snapshot.LastIncludedIndex)
        {
            var message =
            $"The snapshot installation had failed because the last included index {snapshot.LastIncludedIndex} in term {snapshot.LastIncludedTerm} doesn't match the last entry {lastEntryIndex}";
            if (_engine.Log.IsInfoEnabled)
            {
                _engine.Log.Info($"{ToString()}: {message}");
            }

            throw new InvalidOperationException(message);
        }
    }

    public enum SnapshotReadAndCommitType
    {
        None,
        Apply,
        ValidateLastIncludedIndex
    }

    public override IReplayableCommandDto<ClusterOperationContext, ClusterTransaction, MergedTransactionCommand<ClusterOperationContext, ClusterTransaction>> ToDto(
        ClusterOperationContext context)
    {
        throw new NotImplementedException();
    }

}

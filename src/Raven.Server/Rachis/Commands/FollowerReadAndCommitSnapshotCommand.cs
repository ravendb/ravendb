using System;
using System.Threading;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Http;
using Raven.Server.Documents.TransactionMerger.Commands;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Voron.Impl;

namespace Raven.Server.Rachis.Commands;

public class FollowerReadAndCommitSnapshotCommand : MergedTransactionCommand<ClusterOperationContext, ClusterTransaction>
{
    private readonly RachisConsensus _engine;
    private readonly Follower _follower;
    private readonly InstallSnapshot _snapshot;
    private readonly CancellationToken _token;

    public Task OnFullSnapshotInstalledTask { get; private set; }

    public FollowerReadAndCommitSnapshotCommand([NotNull] RachisConsensus engine, Follower follower, [NotNull] InstallSnapshot snapshot, CancellationToken token)
    {
        _engine = engine ?? throw new ArgumentNullException(nameof(engine));
        _follower = follower;
        _snapshot = snapshot ?? throw new ArgumentNullException(nameof(snapshot));
        _token = token;
    }

    protected override long ExecuteCmd(ClusterOperationContext context)
    {
        var lastTerm = _engine.GetTermFor(context, _snapshot.LastIncludedIndex);
        var lastCommitIndex = _engine.GetLastEntryIndex(context);

        if (_engine.GetSnapshotRequest(context) == false &&
            _snapshot.LastIncludedTerm == lastTerm && _snapshot.LastIncludedIndex < lastCommitIndex)
        {
            if (_engine.Log.IsInfoEnabled)
            {
                _engine.Log.Info(
                    $"{ToString()}: Got installed snapshot with last index={_snapshot.LastIncludedIndex} while our lastCommitIndex={lastCommitIndex}, will just ignore it");
            }

            //This is okay to ignore because we will just get the committed entries again and skip them
            _follower.ReadInstallSnapshotAndIgnoreContent(_token);
        }
        else if (_follower.InstallSnapshot(context, _token))
        {
            if (_engine.Log.IsInfoEnabled)
            {
                _engine.Log.Info(
                    $"{ToString()}: Installed snapshot with last index={_snapshot.LastIncludedIndex} with LastIncludedTerm={_snapshot.LastIncludedTerm} ");
            }

            _engine.SetLastCommitIndex(context, _snapshot.LastIncludedIndex, _snapshot.LastIncludedTerm);
            _engine.ClearLogEntriesAndSetLastTruncate(context, _snapshot.LastIncludedIndex, _snapshot.LastIncludedTerm);

            OnFullSnapshotInstalledTask = _engine.OnSnapshotInstalled(context, _snapshot.LastIncludedIndex, _token);
        }
        else
        {
            var lastEntryIndex = _engine.GetLastEntryIndex(context);
            if (lastEntryIndex < _snapshot.LastIncludedIndex)
            {
                var message =
                    $"The snapshot installation had failed because the last included index {_snapshot.LastIncludedIndex} in term {_snapshot.LastIncludedTerm} doesn't match the last entry {lastEntryIndex}";
                if (_engine.Log.IsInfoEnabled)
                {
                    _engine.Log.Info($"{ToString()}: {message}");
                }
                throw new InvalidOperationException(message);
            }
        }

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

    public override IReplayableCommandDto<ClusterOperationContext, ClusterTransaction, MergedTransactionCommand<ClusterOperationContext, ClusterTransaction>> ToDto(ClusterOperationContext context)
    {
        throw new NotImplementedException();
    }

}

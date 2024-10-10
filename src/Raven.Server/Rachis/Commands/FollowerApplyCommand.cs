using Raven.Client.Http;
using System;
using JetBrains.Annotations;
using Raven.Server.Documents.TransactionMerger.Commands;
using Raven.Server.ServerWide.Context;
using System.Collections.Generic;
using System.Diagnostics;

namespace Raven.Server.Rachis.Commands;

public sealed class FollowerApplyCommand : MergedTransactionCommand<ClusterOperationContext, ClusterTransaction>
{
    private readonly RachisConsensus _engine;
    private readonly long _term;
    private readonly List<RachisEntry> _entries;
    private readonly AppendEntries _appendEntries;
    private readonly Stopwatch _sw;

    public long LastTruncate { get; private set; }

    public long LastCommit { get; private set; }

    public bool RemovedFromTopology { get; private set; }

    public FollowerApplyCommand([NotNull] RachisConsensus engine, long term, [NotNull] List<RachisEntry> entries, [NotNull] AppendEntries appendEntries, [NotNull] Stopwatch sw)
    {
        _engine = engine ?? throw new ArgumentNullException(nameof(engine));
        _term = term;
        _entries = entries ?? throw new ArgumentNullException(nameof(entries));
        _appendEntries = appendEntries ?? throw new ArgumentNullException(nameof(appendEntries));
        _sw = sw ?? throw new ArgumentNullException(nameof(sw));
    }

    protected override long ExecuteCmd(ClusterOperationContext context)
    {
        _engine.ValidateTermIn(context, _term);
        if (_engine.Log.IsDebugEnabled)
        {
            _engine.Log.Debug($"{ToString()}: Tx running in {_sw.Elapsed}");
        }
        if (_entries.Count > 0)
        {
            var (lastTopology, lastTopologyIndex) = _engine.AppendToLog(context, _entries);
            using (lastTopology)
            {
                if (lastTopology != null)
                {
                    if (_engine.Log.IsInfoEnabled)
                    {
                        _engine.Log.Info($"Topology changed to {lastTopology}");
                    }

                    var topology = JsonDeserializationRachis<ClusterTopology>.Deserialize(lastTopology);
                    if (topology.Members.ContainsKey(_engine.Tag) ||
                        topology.Promotables.ContainsKey(_engine.Tag) ||
                        topology.Watchers.ContainsKey(_engine.Tag))
                    {
                        RachisConsensus.SetTopology(_engine, context, topology);
                    }
                    else
                    {
                        RemovedFromTopology = true;
                        _engine.ClearAppendedEntriesAfter(context, lastTopologyIndex);
                    }
                }
            }
        }

        var lastEntryIndexToCommit = Math.Min(
            _engine.GetLastEntryIndex(context),
            _appendEntries.LeaderCommit);

        var lastAppliedIndex = _engine.GetLastCommitIndex(context);
        var lastAppliedTerm = _engine.GetTermFor(context, lastEntryIndexToCommit);

        // we start to commit only after we have any log with a term of the current leader
        if (lastEntryIndexToCommit > lastAppliedIndex && lastAppliedTerm == _appendEntries.Term)
        {
            lastAppliedIndex = _engine.Apply(context, lastEntryIndexToCommit, null, _sw);
        }

        LastTruncate = Math.Min(_appendEntries.TruncateLogBefore, lastAppliedIndex);
        _engine.TruncateLogBefore(context, LastTruncate);

        LastCommit = lastAppliedIndex;
        if (_engine.Log.IsDebugEnabled)
        {
            _engine.Log.Debug($"{ToString()}: Ready to commit in {_sw.Elapsed}");
        }

        return 1;
    }

    public override IReplayableCommandDto<ClusterOperationContext, ClusterTransaction, MergedTransactionCommand<ClusterOperationContext, ClusterTransaction>> ToDto(ClusterOperationContext context)
    {
        throw new NotImplementedException();
    }
}

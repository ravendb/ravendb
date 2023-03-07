using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents.Conventions;
using Raven.Client.Http;
using Raven.Client.ServerWide;
using Raven.Server.ServerWide;
using Sparrow.Json;
using Sparrow.Utils;

namespace Raven.Server.Documents.Sharding.Executors;

public class AllOrchestratorNodesExecutor : AbstractExecutor
{
    private readonly ServerStore _store;
    private readonly DatabaseRecord _record;

    private ClusterTopology _clusterTopology;

    private Dictionary<string, RequestExecutor> _current = new Dictionary<string, RequestExecutor>(StringComparer.OrdinalIgnoreCase);
    private AllNodesExecutorState _state;

    // this executor will contact every single node in the cluster
    public AllOrchestratorNodesExecutor(ServerStore store, DatabaseRecord record) : base(store)
    {
        DevelopmentHelper.ShardingToDo(DevelopmentHelper.TeamMember.Karmel, DevelopmentHelper.Severity.Normal,
            "RavenDB-19065 We might want to kill this entire class and do it differently by returning an index to the client which he will pass on for the next request");

        _store = store;
        _record = record;

        RegisterForTopologyChange();

        _clusterTopology = _store.GetClusterTopology();
        UpdateExecutors(_clusterTopology, _record.Sharding.Orchestrator.Topology);
    }

    public async Task<TResult> ExecuteForNodeAsync<TResult>(RavenCommand<TResult> command, string tag, CancellationToken token = default)
    {
        var executor = _current[tag];
        using (executor.ContextPool.AllocateOperationContext(out JsonOperationContext ctx))
        {
            await executor.ExecuteAsync(command, ctx, token: token);
            return command.Result;
        }
    }

    public RequestExecutor GetRequestExecutorForNode(string tag)
    {
        _current.TryGetValue(tag, out var executor);
        return executor;
    }

    private void RegisterForTopologyChange()
    {
        var task = _store.Engine.GetTopologyChanged();
        task.ContinueWith(_ =>
        {
            RegisterForTopologyChange();

            var topology = _store.GetClusterTopology();
            Interlocked.Exchange(ref _clusterTopology, topology);

            UpdateExecutors(topology, _record.Sharding.Orchestrator.Topology);
        }, TaskContinuationOptions.OnlyOnRanToCompletion);
    }

    private void UpdateExecutors(ClusterTopology cluster, OrchestratorTopology orchestrator)
    {
        var disposables = new List<IDisposable>();

        var oldCurrent = _current;
        var newCurrent = new Dictionary<string, RequestExecutor>(StringComparer.OrdinalIgnoreCase);

        foreach (var node in cluster.AllNodes)
        {
            var tag = node.Key;
            var url = node.Value;

            if (orchestrator.AllNodes.Contains(tag, StringComparer.OrdinalIgnoreCase) == false)
                continue;

            if (oldCurrent.TryGetValue(tag, out var requestExecutor) == false)
            {
                newCurrent[tag] = RequestExecutor.CreateForSingleNodeWithoutConfigurationUpdates(url, _record.DatabaseName, _store.Server.Certificate.Certificate,
                    DocumentConventions.DefaultForServer);
                continue;
            }

            if (string.Equals(requestExecutor.Url, url, StringComparison.OrdinalIgnoreCase) == false)
            {
                disposables.Add(requestExecutor); // will dispose outside the lock
                newCurrent[tag] = RequestExecutor.CreateForSingleNodeWithoutConfigurationUpdates(url, _record.DatabaseName, _store.Server.Certificate.Certificate,
                    DocumentConventions.DefaultForServer);
                continue;
            }

            if (_store.Server.Certificate.Certificate?.Thumbprint != requestExecutor.Certificate?.Thumbprint)
            {
                disposables.Add(requestExecutor);
                newCurrent[tag] = RequestExecutor.CreateForSingleNodeWithoutConfigurationUpdates(url, _record.DatabaseName, _store.Server.Certificate.Certificate,
                    DocumentConventions.DefaultForServer);
                continue;
            }

            newCurrent[tag] = requestExecutor;
        }

        lock (this)
        {
            _state = new AllNodesExecutorState
            {
                FullRange = Enumerable.Range(0, orchestrator.Count).ToArray(),
                Executors = newCurrent.Values.ToArray()
            };

            _current = newCurrent;
        }

        foreach (var tag in oldCurrent.Keys)
        {
            if (orchestrator.AllNodes.Contains(tag, StringComparer.OrdinalIgnoreCase))
                continue;

            if (oldCurrent.TryGetValue(tag, out var re))
                disposables.Add(re);
        }

        foreach (var disposable in disposables)
        {
            try
            {
                disposable.Dispose();
            }
            catch
            {
                // ignored
            }
        }
    }

    public override void Dispose()
    {
        foreach (var executor in _current)
        {
            try
            {
                executor.Value.Dispose();
            }
            catch
            {
                // ignored
            }
        }

        _current.Clear();
    }

    public override RequestExecutor GetRequestExecutorAt(int position)
    {
        return _state.Executors[position];
    }

    protected override Memory<int> GetAllPositions() => _state.FullRange;

    protected override void OnCertificateChange(object sender, EventArgs e)
    {
        UpdateExecutors(_clusterTopology, _record.Sharding.Orchestrator.Topology);
    }

    private class AllNodesExecutorState
    {
        public int[] FullRange;
        public RequestExecutor[] Executors;
    }
}

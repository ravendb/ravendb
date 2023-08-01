using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Http;
using Raven.Client.ServerWide;
using Raven.Server.ServerWide;
using Sparrow.Json;

namespace Raven.Server.Documents.Sharding.Executors;

public sealed class AllOrchestratorNodesExecutor : AbstractExecutor
{
    private readonly ServerStore _store;
    private readonly DatabaseRecord _record;

    private readonly ClusterTopology _clusterTopology;

    private Dictionary<string, RequestExecutor> _current = new Dictionary<string, RequestExecutor>(StringComparer.OrdinalIgnoreCase);
    private AllNodesExecutorState _state;

    // this executor will contact every single node in the cluster
    public AllOrchestratorNodesExecutor(ServerStore store, DatabaseRecord record) : base(store)
    {
        _store = store;
        _record = record;

        _clusterTopology = _store.GetClusterTopology();
        UpdateExecutors(_clusterTopology, _record.Sharding.Orchestrator.Topology);
    }

    public async Task<TResult> ExecuteForNodeAsync<TResult>(RavenCommand<TResult> command, string nodeTag, CancellationToken token = default)
    {
        var executor = GetRequestExecutorForNode(nodeTag);
        using (executor.ContextPool.AllocateOperationContext(out JsonOperationContext ctx))
        {
            await executor.ExecuteAsync(command, ctx, token: token);
            return command.Result;
        }
    }

    public bool TryGetRequestExecutorForNode(string nodeTag, out RequestExecutor requestExecutor) => _current.TryGetValue(nodeTag, out requestExecutor);

    public RequestExecutor GetRequestExecutorForNode(string nodeTag)
    {
        if (TryGetRequestExecutorForNode(nodeTag, out var requestExecutor))
            return requestExecutor;

        throw new InvalidOperationException($"There is no request executor available for node '{nodeTag}'.");
    }

    private void UpdateExecutors(ClusterTopology clusterTopology, OrchestratorTopology orchestrator)
    {
        var oldCurrent = _current;
        var newCurrent = new Dictionary<string, RequestExecutor>(StringComparer.OrdinalIgnoreCase);

        foreach (var node in clusterTopology.AllNodes)
        {
            var tag = node.Key;

            if (orchestrator.AllNodes.Contains(tag, StringComparer.OrdinalIgnoreCase) == false)
                continue;

            var url = _store.PublishedServerUrls.SelectUrl(tag, clusterTopology);

            newCurrent[tag] = RequestExecutor.CreateForSingleNodeWithoutConfigurationUpdates(url, _record.DatabaseName, _store.Server.Certificate.Certificate,
                ServerStore.Sharding.DocumentConventionsForOrchestrator);
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

        SafelyDisposeExecutors(oldCurrent.Values);
    }

    public override void Dispose()
    {
        base.Dispose();
        SafelyDisposeExecutors(_current.Values);
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

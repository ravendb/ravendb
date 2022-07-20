using System;
using System.Collections.Concurrent;
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

public class AllNodesExecutor : AbstractExecutor
{
    private readonly ServerStore _store;
    private readonly ShardedDatabaseContext _database;

    private ClusterTopology _clusterTopology;
        
    private readonly ConcurrentDictionary<string, RequestExecutor> _current = new ConcurrentDictionary<string, RequestExecutor>();
    private AllNodesExecutorState _state;

    // this executor will contact every single node in the cluster
    public AllNodesExecutor(ServerStore store, ShardedDatabaseContext database) : base(store)
    {
        DevelopmentHelper.ShardingToDo(DevelopmentHelper.TeamMember.Karmel, DevelopmentHelper.Severity.Normal, 
            "We might want to kill this entire class and do it differently by returning an index to the client which he will pass on for the next request");

        _store = store;
        _database = database;

        RegisterForTopologyChange();

        _clusterTopology = _store.GetClusterTopology();
        UpdateExecutors(_clusterTopology, _database.DatabaseRecord.Sharding.Orchestrator.Topology);
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

    private void RegisterForTopologyChange()
    {
        var task = _store.Engine.GetTopologyChanged();
        task.ContinueWith(_ =>
        {
            RegisterForTopologyChange();

            var topology = _store.GetClusterTopology();
            Interlocked.Exchange(ref _clusterTopology, topology);

            UpdateExecutors(topology, _database.DatabaseRecord.Sharding.Orchestrator.Topology);
        }, TaskContinuationOptions.OnlyOnRanToCompletion);
    }

    private void UpdateExecutors(ClusterTopology cluster, OrchestratorTopology orchestrator)
    {
        var disposables = new List<IDisposable>();
        lock (this)
        {
            foreach (var node in cluster.AllNodes)
            {
                var tag = node.Key;
                var url = node.Value;

                if (orchestrator.AllNodes.Contains(tag) == false)
                {
                    if (_current.TryRemove(tag, out var re))
                        disposables.Add(re);

                    continue;
                }

                if (_current.TryGetValue(tag, out var requestExecutor) == false)
                {
                    _current[tag] = RequestExecutor.CreateForSingleNodeWithoutConfigurationUpdates(url, _database.DatabaseName, _store.Server.Certificate.Certificate, DocumentConventions.DefaultForServer);
                    continue;
                }

                if (string.Equals(requestExecutor.Url, url, StringComparison.OrdinalIgnoreCase) == false)
                {
                    disposables.Add(requestExecutor); // will dispose outside the lock
                    _current[tag] = RequestExecutor.CreateForSingleNodeWithoutConfigurationUpdates(url, _database.DatabaseName, _store.Server.Certificate.Certificate, DocumentConventions.DefaultForServer);
                }
            }

            _state = new AllNodesExecutorState
            {
                FullRange = Enumerable.Range(0, orchestrator.Count).ToArray(),
                Executors = _current.Values.ToArray()
            };
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
        Dispose();
        UpdateExecutors(_clusterTopology, _database.DatabaseRecord.Sharding.Orchestrator.Topology);
    }

    private class AllNodesExecutorState
    {
        public int[] FullRange;
        public RequestExecutor[] Executors;
    }
}

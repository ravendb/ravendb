using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;
using Raven.Server;
using Raven.Server.Documents;
using Raven.Server.Rachis;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Tests.Infrastructure;
using Xunit;

namespace FastTests;

public partial class RavenTestBase
{
    public readonly ClusterTestBase2 Cluster;

    public class ClusterTestBase2
    {
        private readonly RavenTestBase _parent;

        public ClusterTestBase2(RavenTestBase parent)
        {
            _parent = parent ?? throw new ArgumentNullException(nameof(parent));
        }

        public async Task WaitForRaftCommandToBeAppliedInClusterAsync(RavenServer leader, string commandType)
        {
            var updateIndex = LastRaftIndexForCommand(leader, commandType);
            await WaitForRaftIndexToBeAppliedInClusterAsync(updateIndex, TimeSpan.FromSeconds(10));
        }

        public async Task WaitForRaftCommandToBeAppliedInLocalServerAsync(string commandType)
        {
            var updateIndex = LastRaftIndexForCommand(_parent.Server, commandType);
            await _parent.Server.ServerStore.Cluster.WaitForIndexNotification(updateIndex, TimeSpan.FromSeconds(10));
        }

        public async Task<long> WaitForRaftCommandToBeAppendedInClusterAsync(List<RavenServer> nodes, string commandType, int timeout = 15_000, int interval = 100)
        {
            // Assuming that I have only 1 command of this type (commandType) in the raft log
            var tasks = new List<Task<bool>>();
            foreach (var server in nodes)
            {
                var t = WaitForValueAsync( () =>
                {
                    var commandFound = TryGetLastRaftIndexForCommand(server, commandType, out _);
                    return Task.FromResult(commandFound);
                }, true, timeout: timeout, interval: interval);
                tasks.Add(t);
            }

            await Task.WhenAll(tasks);

            foreach (var t in tasks)
            {
                if (await t == false)
                    return -1;
            }

            TryGetLastRaftIndexForCommand(nodes[0], commandType, out var index);
            return index;
        }

        public async Task CreateIndexInClusterAsync(IDocumentStore store, AbstractIndexCreationTask index, List<RavenServer> nodes = null)
        {
            var results = (await store.Maintenance.ForDatabase(store.Database)
                                        .SendAsync(new PutIndexesOperation(index.CreateIndexDefinition())))
                                        .Single(r => r.Index == index.IndexName);

            // wait for index creation on cluster
            nodes ??= _parent.Servers;
            await WaitForRaftIndexToBeAppliedOnClusterNodesAsync(results.RaftCommandIndex, nodes);
        }

        public async Task WaitForAllNodesToBeMembersAsync(IDocumentStore store, string databaseName = null, CancellationToken token = default)
        {
            using var _ = GetOrCreateCancellationToken(ref token);
            while (true)
            {
                token.ThrowIfCancellationRequested();

                await Task.Delay(TimeSpan.FromMilliseconds(250), token);
                var res = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(databaseName ?? store.Database), token);
                if (res != null)
                {
                    if (res.IsSharded == false)
                    {
                        if (res.Topology.Members.Count == res.Topology.Count)
                            return;
                    }
                    else
                    {
                        if (res.Sharding.Orchestrator.Topology.Count != res.Sharding.Orchestrator.Topology.Members.Count)
                            continue;

                        if (res.Sharding.Shards.Sum(t => t.Value.Count) != res.Sharding.Shards.Sum(t => t.Value.Members.Count))
                            continue;

                        return;
                    }
                }
            }
        }

        public IDisposable GetOrCreateCancellationToken(ref CancellationToken token)
        {
            if (token.CanBeCanceled)
                return null;

            var cts = new CancellationTokenSource(TimeSpan.FromMinutes(5));
            token = cts.Token;
            return cts;
        }

        public async Task WaitForNodeToBeRehabAsync(IDocumentStore store, string node, string databaseName = null, CancellationToken token = default)
        {
            using var _ = GetOrCreateCancellationToken(ref token);
            while (true)
            {
                token.ThrowIfCancellationRequested();

                await Task.Delay(TimeSpan.FromMilliseconds(250), token);
                var res = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(databaseName ?? store.Database), token);
                if (res != null)
                {
                    if (res.IsSharded == false)
                    {
                        if (res.Topology.Rehabs.Contains(node))
                            return;
                    }
                    else
                    {
                        if (res.Sharding.Orchestrator.Topology.RelevantFor(node))
                        {
                            if (res.Sharding.Orchestrator.Topology.Rehabs.Contains(node) == false)
                                continue;
                        }

                        if (res.Sharding.Shards.Where(s => s.Value.RelevantFor(node)).All(s => s.Value.Rehabs.Contains(node)) == false)
                            continue;

                        return;
                    }
                }
            }
        }
        public virtual Task<DocumentDatabase> GetAnyDocumentDatabaseInstanceFor(IDocumentStore store, List<RavenServer> cluster, string database = null)
        {
            foreach (var node in cluster)
            {
                var db = node.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(database ?? store.Database);
                if (db != null)
                    return db;
            }

            return null;
        }

        public long LastRaftIndexForCommand(RavenServer server, string commandType)
        {
            var commandFound = TryGetLastRaftIndexForCommand(server, commandType, out var updateIndex);
            Assert.True(commandFound, $"{commandType} wasn't found in the log.");
            return updateIndex;
        }

        public bool TryGetLastRaftIndexForCommand(RavenServer server, string commandType, out long updateIndex)
        {
            updateIndex = 0L;
            using (server.ServerStore.Engine.ContextPool.AllocateOperationContext(out ClusterOperationContext context))
            using (context.OpenReadTransaction())
            {
                foreach (var entry in server.ServerStore.Engine.LogHistory.GetHistoryLogs(context))
                {
                    var type = entry[nameof(RachisLogHistory.LogHistoryColumn.Type)].ToString();
                    if (type == commandType)
                    {
                        updateIndex = long.Parse(entry[nameof(RachisLogHistory.LogHistoryColumn.Index)].ToString());
                    }
                }
            }

            return updateIndex > 0L;
        }

        public IEnumerable<DynamicJsonValue> GetRaftCommands(RavenServer server, string commandType = null)
        {
            using (server.ServerStore.Engine.ContextPool.AllocateOperationContext(out ClusterOperationContext context))
            using (context.OpenReadTransaction())
            {
                foreach (var entry in server.ServerStore.Engine.LogHistory.GetHistoryLogs(context))
                {
                    var type = entry[nameof(RachisLogHistory.LogHistoryColumn.Type)].ToString();
                    if (commandType == null || commandType == type)
                        yield return entry;
                }
            }
        }

        public string GetRaftHistory(RavenServer server)
        {
            var sb = new StringBuilder();

            using (server.ServerStore.Engine.ContextPool.AllocateOperationContext(out ClusterOperationContext context))
            using (context.OpenReadTransaction())
            {
                foreach (var entry in server.ServerStore.Engine.LogHistory.GetHistoryLogs(context))
                {
                    sb.AppendLine(context.ReadObject(entry, "raft-command-history").ToString());
                }
            }

            return sb.ToString();
        }

        public async Task WaitForRaftIndexToBeAppliedInClusterAsync(long index, TimeSpan? timeout = null, List<RavenServer> nodes = null)
        {
            await WaitForRaftIndexToBeAppliedOnClusterNodesAsync(index, nodes ?? _parent.Servers, timeout);
        }

        public async Task WaitForRaftIndexToBeAppliedInClusterWithNodesValidationAsync(long index, TimeSpan? timeout = null, List<RavenServer> nodes = null)
        {
            var servers = nodes ?? _parent.Servers;
            var notDisposed = servers.Count(s => s.ServerStore.Disposed == false);
            var notPassive = servers.Count(s => s.ServerStore.Engine.CurrentState != RachisState.Passive);

            Assert.True(servers.Count == notDisposed, $"Unequal not disposed nodes {servers.Count} != {notDisposed}");
            Assert.True(servers.Count == notPassive, $"Unequal not passive nodes {servers.Count} != {notPassive}");

            await WaitForRaftIndexToBeAppliedOnClusterNodesAsync(index, servers, timeout);
        }

        public async Task WaitForRaftIndexToBeAppliedOnClusterNodesAsync(long index, List<RavenServer> nodes, TimeSpan? timeout = null)
        {
            if (nodes.Count == 0)
                throw new InvalidOperationException("Cannot wait for raft index to be applied when the cluster is empty. Make sure you are using the right server.");

            if (timeout.HasValue == false)
                timeout = Debugger.IsAttached ? TimeSpan.FromSeconds(300) : TimeSpan.FromSeconds(60);

            var tasks = nodes.Where(s => s.ServerStore.Disposed == false &&
                                          s.ServerStore.Engine.CurrentState != RachisState.Passive)
                .Select(server => server.ServerStore.Cluster.WaitForIndexNotification(index))
                .ToList();

            if (await Task.WhenAll(tasks).WaitWithoutExceptionAsync(timeout.Value))
                return;

            ThrowTimeoutException(nodes, tasks, index, timeout.Value);
        }

        public void WaitForFirstCompareExchangeTombstonesClean(RavenServer server)
        {
            Assert.True(WaitForValue(() =>
            {
                // wait for compare exchange tombstone cleaner run
                if (server.ServerStore.Observer == null)
                    return false;

                if (server.ServerStore.Observer._lastTombstonesCleanupTimeInTicks == 0)
                    return false;

                return true;
            }, true));
        }

        private static void ThrowTimeoutException(List<RavenServer> nodes, List<Task> tasks, long index, TimeSpan timeout)
        {
            var message = $"Timed out after {timeout} waiting for index {index} because out of {nodes.Count} servers" +
                          " we got confirmations that it was applied only on the following servers: ";

            for (var i = 0; i < tasks.Count; i++)
            {
                message += $"{Environment.NewLine}Url: {nodes[i].WebUrl}. Applied: {tasks[i].IsCompleted}.";
                if (tasks[i].IsCompleted == false)
                {
                    using (nodes[i].ServerStore.Engine.ContextPool.AllocateOperationContext(out ClusterOperationContext context))
                    {
                        context.OpenReadTransaction();
                        message += $"{Environment.NewLine}Log state for non responsing server:{Environment.NewLine}{nodes[i].ServerStore.Engine.LogHistory.GetHistoryLogsAsString(context)}";
                    }
                }
            }

            throw new TimeoutException(message);
        }

        public string CollectLogsFromNodes(List<RavenServer> nodes)
        {
            var message = "";
            for (var i = 0; i < nodes.Count; i++)
            {
                message += $"{Environment.NewLine}Url: {nodes[i].WebUrl}.";
                if (nodes[i].Disposed)
                {
                    message += "Disposed";
                    continue;
                }
                using (nodes[i].ServerStore.Engine.ContextPool.AllocateOperationContext(out ClusterOperationContext context))
                using (context.OpenReadTransaction())
                {
                    message += CollectLogs(context, nodes[i]);
                }
            }

            return message;
        }

        public string CollectLogs(ClusterOperationContext context, RavenServer server)
        {
            return
                $"{Environment.NewLine}Log for server '{server.ServerStore.NodeTag}':" +
                $"{Environment.NewLine}Last notified Index '{server.ServerStore.Cluster.LastNotifiedIndex}':" +
                $"{Environment.NewLine}{context.ReadObject(server.ServerStore.GetLogDetails(context, start: 0, take: int.MaxValue), "LogSummary/" + server.ServerStore.NodeTag)}" +
                $"{Environment.NewLine}{server.ServerStore.Engine.LogHistory.GetHistoryLogsAsString(context)}";
        }

        public string GetLastStatesFromAllServersOrderedByTime()
        {
            List<(string tag, RachisConsensus.StateTransition transition)> states = new List<(string tag, RachisConsensus.StateTransition transition)>();
            foreach (var s in _parent.Servers)
            {
                foreach (var state in s.ServerStore.Engine.PrevStates)
                {
                    states.Add((s.ServerStore.NodeTag, state));
                }
            }
            return string.Join(Environment.NewLine, states.OrderBy(x => x.transition.When).Select(x => $"State for {x.tag}-term{x.Item2.CurrentTerm}:{Environment.NewLine}{x.Item2.From}=>{x.Item2.To} at {x.Item2.When:o} {Environment.NewLine}because {x.Item2.Reason}"));
        }

        public void SuspendObserver(RavenServer server)
        {
            // observer is set in the background task, hence we are waiting for it to not be null
            WaitForValue(() => server.ServerStore.Observer != null, true);

            server.ServerStore.Observer.Suspended = true;
        }

        public async Task AssertNumberOfCommandsPerNode(long expectedNumberOfCommands, List<RavenServer> servers, string commandType, int timeout = 30_000, int interval = 1_000)
        {
            var numberOfCommandsPerNode = new Dictionary<string, long>();
            var isExpectedNumberOfCommandsPerNode = await WaitForValueAsync(() =>
                {
                    numberOfCommandsPerNode = new Dictionary<string, long>();

                    foreach (var server in servers)
                    {
                        var nodeTag = server.ServerStore.NodeTag;
                        var numberOfCommands = GetRaftCommands(server, commandType).Count();

                        numberOfCommandsPerNode.Add(nodeTag, numberOfCommands);
                    }

                    return Task.FromResult(numberOfCommandsPerNode.All(x => x.Value == expectedNumberOfCommands));
                },
                expectedVal: true,
                timeout, interval);

            Assert.True(isExpectedNumberOfCommandsPerNode, BuildErrorMessage());
            
            return;
            string BuildErrorMessage()
            {
                var stringBuilder = new StringBuilder();

                using (var context = JsonOperationContext.ShortTermSingleUse())
                {
                    stringBuilder.AppendLine($"Expected number of commands per node: {expectedNumberOfCommands}. Actual number of commands per node: ");
                    foreach ((string nodeTag, long numberOfCommands) in numberOfCommandsPerNode)
                    {
                        stringBuilder.AppendLine($"Node tag: '{nodeTag}' with actual number of commands: '{numberOfCommands}'. Commands:");

                        var server = servers.Find(x => x.ServerStore.NodeTag == nodeTag);
                        var raftCommands = GetRaftCommands(server).Select(djv => context.ReadObject(djv, "raftCommand").ToString()).ToArray();

                        stringBuilder.AppendLine($"{string.Join($"{Environment.NewLine}\t", raftCommands)}");
                    }
                }

                return stringBuilder.ToString();
            }
        }
    }
}

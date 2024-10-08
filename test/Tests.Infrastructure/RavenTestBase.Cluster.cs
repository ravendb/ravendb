﻿using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Esprima.Ast;
using Lucene.Net.Documents;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Client.ServerWide;
using Raven.Server;
using Raven.Server.Monitoring.Snmp;
using Raven.Server.Rachis;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Tests.Infrastructure;
using Xunit;
using Xunit.Sdk;

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

        public async Task WaitForRaftIndexToBeAppliedInClusterWithNodesValidationAsync(long index, TimeSpan? timeout = null)
        {
            var notDisposed = _parent.Servers.Count(s => s.ServerStore.Disposed == false);
            var notPassive = _parent.Servers.Count(s => s.ServerStore.Engine.CurrentState != RachisState.Passive);

            Assert.True(_parent.Servers.Count == notDisposed, $"Unequal not disposed nodes {_parent.Servers.Count} != {notDisposed}");
            Assert.True(_parent.Servers.Count == notPassive, $"Unequal not passive nodes {_parent.Servers.Count} != {notPassive}");

            await WaitForRaftIndexToBeAppliedInClusterAsync(index, timeout);
        }

        public async Task WaitForRaftIndexToBeAppliedInClusterAsync(long index, TimeSpan? timeout = null)
        {
            await WaitForRaftIndexToBeAppliedOnClusterNodesAsync(index, _parent.Servers, timeout);
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
                $"{Environment.NewLine}{context.ReadObject(server.ServerStore.Engine.GetLogDetails(context, fromIndex: 0, take: int.MaxValue, detailed: true).ToJson(), "LogSummary/" + server.ServerStore.NodeTag)}" +
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

        internal async Task<(bool, Dictionary<string, long>)> GetNumberOfCommandsPerNode(long expectedNumberOfCommands, List<RavenServer> servers, string commandType, int timeout = 30_000, int interval = 1_000)
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

            bool allNodesHaveSameNumberOfCommands = numberOfCommandsPerNode.Values.Distinct().Count() == 1;
            Assert.True(allNodesHaveSameNumberOfCommands, BuildErrorMessage(expectedNumberOfCommands, numberOfCommandsPerNode, servers));

            return (isExpectedNumberOfCommandsPerNode, numberOfCommandsPerNode);
        }

        internal string BuildErrorMessage(long expectedNumberOfCommands, Dictionary<string, long> numberOfCommandsPerNode, List<RavenServer> servers)
        {
            var stringBuilder = new StringBuilder();

            using (var context = JsonOperationContext.ShortTermSingleUse())
            {
                stringBuilder.AppendLine($"Expected number of commands per node: '{expectedNumberOfCommands}'. Actual number of commands per node: ");
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

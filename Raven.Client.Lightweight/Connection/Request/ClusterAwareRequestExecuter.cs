// -----------------------------------------------------------------------
//  <copyright file="ClusterAwareRequestExecuter.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;

using Raven.Abstractions;
using Raven.Abstractions.Cluster;
using Raven.Abstractions.Connection;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Replication;
using Raven.Abstractions.Util;
using Raven.Client.Connection.Async;
using Raven.Client.Connection.Implementation;
using Raven.Client.Extensions;
using Raven.Client.Metrics;

namespace Raven.Client.Connection.Request
{
    public class ClusterAwareRequestExecuter : IRequestExecuter
    {
        private const int WaitForLeaderTimeoutInSeconds = 30;

        private const int GetReplicationDestinationsTimeoutInSeconds = 2;

        private readonly ManualResetEventSlim leaderNodeSelected = new ManualResetEventSlim();

        private Task refreshReplicationInformationTask;

        private OperationMetadata leaderNode;

        private DateTime lastUpdate = DateTime.MinValue;

        private bool firstTime = true;

        private int readStripingBase;

        public OperationMetadata LeaderNode
        {
            get
            {
                return leaderNode;
            }

            private set
            {
                if (value == null)
                {
                    leaderNodeSelected.Reset();
                    leaderNode = null;
                    return;
                }

                leaderNode = value;
                leaderNodeSelected.Set();
            }
        }

        public List<OperationMetadata> NodeUrls
        {
            get
            {
                return Nodes
                    .Select(x => new OperationMetadata(x))
                    .ToList();
            }
        }

        public List<OperationMetadata> Nodes { get; private set; }

        public FailureCounters FailureCounters { get; private set; }

        public ClusterAwareRequestExecuter()
        {
            Nodes = new List<OperationMetadata>();
            FailureCounters = new FailureCounters();
        }

        public int GetReadStripingBase(bool increment)
        {
            return increment ? Interlocked.Increment(ref readStripingBase) : readStripingBase;
        }

        public ReplicationDestination[] FailoverServers { get; set; }

        public Task<T> ExecuteOperationAsync<T>(AsyncServerClient serverClient, HttpMethod method, int currentRequest, Func<OperationMetadata, IRequestTimeMetric, Task<T>> operation, CancellationToken token)
        {
            return ExecuteWithinClusterInternalAsync(serverClient, method, operation, token);
        }

        public Task UpdateReplicationInformationIfNeededAsync(AsyncServerClient serverClient, bool force = false)
        {
            if (force == false && lastUpdate.AddMinutes(5) > SystemTime.UtcNow && LeaderNode != null)
                return new CompletedTask();

            LeaderNode = null;
            return UpdateReplicationInformationForCluster(serverClient, new OperationMetadata(serverClient.Url, serverClient.PrimaryCredentials, null), operationMetadata =>
            {
                return serverClient.DirectGetReplicationDestinationsAsync(operationMetadata, null, timeout: TimeSpan.FromSeconds(GetReplicationDestinationsTimeoutInSeconds)).ContinueWith(t =>
                {
                    if (t.IsFaulted || t.IsCanceled)
                        return null;

                    return t.Result;
                });
            });
        }

        public void AddHeaders(HttpJsonRequest httpJsonRequest, AsyncServerClient serverClient, string currentUrl)
        {
            httpJsonRequest.AddHeader(Constants.Cluster.ClusterAwareHeader, "true");

            if (serverClient.convention.FailoverBehavior == FailoverBehavior.ReadFromAllWriteToLeader)
                httpJsonRequest.AddHeader(Constants.Cluster.ClusterReadBehaviorHeader, "All");

            if (serverClient.convention.FailoverBehavior == FailoverBehavior.ReadFromAllWriteToLeaderWithFailovers || serverClient.convention.FailoverBehavior == FailoverBehavior.ReadFromLeaderWriteToLeaderWithFailovers)
                httpJsonRequest.AddHeader(Constants.Cluster.ClusterFailoverBehaviorHeader, "true");
        }

        public void SetReadStripingBase(int strippingBase)
        {
            this.readStripingBase = strippingBase;
        }

        private async Task<T> ExecuteWithinClusterInternalAsync<T>(AsyncServerClient serverClient, HttpMethod method, Func<OperationMetadata, IRequestTimeMetric, Task<T>> operation, CancellationToken token, int numberOfRetries = 2)
        {
            token.ThrowIfCancellationRequested();

            if (numberOfRetries < 0)
                throw new InvalidOperationException("Cluster is not reachable. Out of retries, aborting.");

            var node = LeaderNode;
            if (node == null)
            {
#pragma warning disable 4014
                UpdateReplicationInformationIfNeededAsync(serverClient); // maybe start refresh task
#pragma warning restore 4014

                switch (serverClient.convention.FailoverBehavior)
                {
                    case FailoverBehavior.ReadFromAllWriteToLeaderWithFailovers:
                    case FailoverBehavior.ReadFromLeaderWriteToLeaderWithFailovers:
                        if (Nodes.Count == 0)
                            leaderNodeSelected.Wait(TimeSpan.FromSeconds(WaitForLeaderTimeoutInSeconds));
                        break;
                    default:
                        if (leaderNodeSelected.Wait(TimeSpan.FromSeconds(WaitForLeaderTimeoutInSeconds)) == false)
                            throw new InvalidOperationException("Cluster is not reachable. No leader was selected, aborting.");
                        break;
                }

                node = LeaderNode;
            }

            switch (serverClient.convention.FailoverBehavior)
            {
                case FailoverBehavior.ReadFromAllWriteToLeader:
                    if (method == HttpMethods.Get)
                        node = GetNodeForReadOperation(node) ?? node;
                    break;
                case FailoverBehavior.ReadFromAllWriteToLeaderWithFailovers:
                    if (node == null)
                    {
                        return await HandleWithFailovers(operation, token).ConfigureAwait(false);
                    }

                    if (method == HttpMethods.Get)
                        node = GetNodeForReadOperation(node) ?? node;
                    break;
                case FailoverBehavior.ReadFromLeaderWriteToLeaderWithFailovers:
                    if (node == null)
                        return await HandleWithFailovers(operation, token).ConfigureAwait(false);
                    break;
            }
            var operationResult = await TryClusterOperationAsync(node, operation, false, token).ConfigureAwait(false);
            if (operationResult.Success)
                return operationResult.Result;

            LeaderNode = null;
            FailureCounters.IncrementFailureCount(node.Url);
            return await ExecuteWithinClusterInternalAsync(serverClient, method, operation, token, numberOfRetries - 1).ConfigureAwait(false);
        }

        private OperationMetadata GetNodeForReadOperation(OperationMetadata node)
        {
            Debug.Assert(node != null);

            var nodes = new List<OperationMetadata>(NodeUrls);

            if (readStripingBase == -1)
                return LeaderNode;

            if (nodes.Count == 0)
                return null;


            var nodeIndex = readStripingBase % nodes.Count;
            var readNode = nodes[nodeIndex];
            if (ShouldExecuteUsing(readNode))
                return readNode;

            return node;
        }

        private async Task<T> HandleWithFailovers<T>(Func<OperationMetadata, IRequestTimeMetric, Task<T>> operation, CancellationToken token)
        {
            var nodes = NodeUrls;
            for (var i = 0; i < nodes.Count; i++)
            {
                var n = nodes[i];
                if (ShouldExecuteUsing(n) == false)
                    continue;

                var hasMoreNodes = nodes.Count > i + 1;
                var result = await TryClusterOperationAsync(n, operation, hasMoreNodes, token).ConfigureAwait(false);
                if (result.Success)
                    return result.Result;

                FailureCounters.IncrementFailureCount(n.Url);
            }

            throw new InvalidOperationException("Cluster is not reachable. Executing operation on any of the nodes failed, aborting.");
        }

        private bool ShouldExecuteUsing(OperationMetadata operationMetadata)
        {
            var failureCounter = FailureCounters.GetHolder(operationMetadata.Url);
            if (failureCounter.Value <= 1) // can fail once
                return true;

            return false;
        }

        private async Task<AsyncOperationResult<T>> TryClusterOperationAsync<T>(OperationMetadata node, Func<OperationMetadata, IRequestTimeMetric, Task<T>> operation, bool avoidThrowing, CancellationToken token)
        {
            Debug.Assert(node != null);

            token.ThrowIfCancellationRequested();
            var shouldRetry = false;

            var operationResult = new AsyncOperationResult<T>();
            try
            {
                operationResult.Result = await operation(node, null).ConfigureAwait(false);
                operationResult.Success = true;
            }
            catch (Exception e)
            {
                bool wasTimeout;
                if (HttpConnectionHelper.IsServerDown(e, out wasTimeout))
                {
                    shouldRetry = true;
                    operationResult.WasTimeout = wasTimeout;
                }
                else
                {
                    var ae = e as AggregateException;
                    ErrorResponseException errorResponseException;
                    if (ae != null)
                        errorResponseException = ae.ExtractSingleInnerException() as ErrorResponseException;
                    else
                        errorResponseException = e as ErrorResponseException;

                    if (errorResponseException != null && (errorResponseException.StatusCode == HttpStatusCode.Redirect || errorResponseException.StatusCode == HttpStatusCode.ExpectationFailed))
                        shouldRetry = true;
                }

                if (shouldRetry == false && avoidThrowing == false)
                    throw;
            }

            if (operationResult.Success)
                FailureCounters.ResetFailureCount(node.Url);

            return operationResult;
        }

        private Task UpdateReplicationInformationForCluster(AsyncServerClient serverClient, OperationMetadata primaryNode, Func<OperationMetadata, Task<ReplicationDocumentWithClusterInformation>> getReplicationDestinationsTask)
        {
            lock (this)
            {
                var serverHash = ServerHash.GetServerHash(primaryNode.Url);

                var taskCopy = refreshReplicationInformationTask;
                if (taskCopy != null)
                    return taskCopy;

                if (firstTime)
                {
                    firstTime = false;

                    var nodes = ReplicationInformerLocalCache.TryLoadClusterNodesFromLocalCache(serverHash);
                    if (nodes != null)
                    {
                        Nodes = nodes;
                        LeaderNode = GetLeaderNode(Nodes);

                        if (LeaderNode != null)
                            return new CompletedTask();
                    }
                }

                return refreshReplicationInformationTask = Task.Factory.StartNew(() =>
                {
                    var tryFailoverServers = false;
                    var triedFailoverServers = FailoverServers == null || FailoverServers.Length == 0;
                    for (;;)
                    {
                        var nodes = NodeUrls.ToHashSet();

                        if (tryFailoverServers == false)
                        {
                            if (nodes.Count == 0)
                                nodes.Add(primaryNode);
                        }
                        else
                        {
                            nodes.Add(primaryNode); // always check primary node during failover check

                            foreach (var failoverServer in FailoverServers)
                            {
                                var node = ConvertReplicationDestinationToOperationMetadata(failoverServer, ClusterInformation.NotInCluster);
                                if (node != null)
                                    nodes.Add(node);
                            }

                            triedFailoverServers = true;
                        }

                        var replicationDocuments = nodes
                            .Select(operationMetadata => new
                            {
                                Node = operationMetadata,
                                Task = getReplicationDestinationsTask(operationMetadata)
                            })
                            .ToArray();

                        var tasks = replicationDocuments
                            .Select(x => x.Task)
                            .ToArray();

                        Task.WaitAll(tasks);

                        replicationDocuments.ForEach(x =>
                        {
                            if (x.Task.Result == null)
                                return;

                            FailureCounters.ResetFailureCount(x.Node.Url);
                        });

                        var newestTopology = replicationDocuments
                            .Where(x => x.Task.Result != null)
                            .OrderByDescending(x => x.Task.Result.Term)
                            .ThenByDescending(x =>
                            {
                                var index = x.Task.Result.ClusterCommitIndex;
                                return x.Task.Result.ClusterInformation.IsLeader ? index + 1 : index;
                            })
                            .FirstOrDefault();


                        if (newestTopology == null && FailoverServers != null && FailoverServers.Length > 0 && tryFailoverServers == false)
                            tryFailoverServers = true;

                        if (newestTopology == null && triedFailoverServers)
                        {
                            LeaderNode = primaryNode;
                            Nodes = new List<OperationMetadata>
                            {
                                primaryNode
                            };
                            return;
                        }

                        if (newestTopology != null)
                        {
                            Nodes = GetNodes(newestTopology.Node, newestTopology.Task.Result);
                            LeaderNode = newestTopology.Task.Result.ClusterInformation.IsLeader ?
                                Nodes.FirstOrDefault(n => n.Url == newestTopology.Node.Url) : null;

                            ReplicationInformerLocalCache.TrySavingClusterNodesToLocalCache(serverHash, Nodes);

                            if (newestTopology.Task.Result.ClientConfiguration != null)
                                serverClient.convention.UpdateFrom(newestTopology.Task.Result.ClientConfiguration);

                            if (LeaderNode != null)
                                return;
                        }

                        Thread.Sleep(500);
                    }
                }).ContinueWith(t =>
                {
                    lastUpdate = SystemTime.UtcNow;
                    refreshReplicationInformationTask = null;
                });
            }
        }

        private static OperationMetadata GetLeaderNode(IEnumerable<OperationMetadata> nodes)
        {
            return nodes.FirstOrDefault(x => x.ClusterInformation != null && x.ClusterInformation.IsLeader);
        }

        private static List<OperationMetadata> GetNodes(OperationMetadata node, ReplicationDocumentWithClusterInformation replicationDocument)
        {
            var nodes = replicationDocument.Destinations
                .Select(x => ConvertReplicationDestinationToOperationMetadata(x, x.ClusterInformation))
                .Where(x => x != null)
                .ToList();

            nodes.Add(new OperationMetadata(node.Url, node.Credentials, replicationDocument.ClusterInformation));

            return nodes;
        }

        private static OperationMetadata ConvertReplicationDestinationToOperationMetadata(ReplicationDestination destination, ClusterInformation clusterInformation)
        {
            var url = string.IsNullOrEmpty(destination.ClientVisibleUrl) ? destination.Url : destination.ClientVisibleUrl;
            if (string.IsNullOrEmpty(url) || destination.CanBeFailover() == false)
                return null;

            if (string.IsNullOrEmpty(destination.Database))
                return new OperationMetadata(url, destination.Username, destination.Password, destination.Domain, destination.ApiKey, clusterInformation);

            return new OperationMetadata(MultiDatabase.GetRootDatabaseUrl(url).ForDatabase(destination.Database), destination.Username, destination.Password, destination.Domain, destination.ApiKey, clusterInformation);
        }

        public IDisposable ForceReadFromMaster()
        {
            var strippingBase = readStripingBase;
            readStripingBase = -1;
            return new DisposableAction(() => { readStripingBase = strippingBase; });
        }

        public event EventHandler<FailoverStatusChangedEventArgs> FailoverStatusChanged = delegate { };
    }
}

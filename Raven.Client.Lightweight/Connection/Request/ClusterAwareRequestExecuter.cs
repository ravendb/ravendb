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
using Raven.Abstractions.Logging;
using Raven.Abstractions.Replication;
using Raven.Abstractions.Util;
using Raven.Client.Connection.Async;
using Raven.Client.Connection.Implementation;
using Raven.Client.Extensions;
using Raven.Client.Metrics;
using Raven.Imports.Newtonsoft.Json;
using Raven.Json.Linq;

namespace Raven.Client.Connection.Request
{
    public class ClusterAwareRequestExecuter : IRequestExecuter
    {
        public TimeSpan WaitForLeaderTimeout { get; set; } = TimeSpan.FromSeconds(10);

        public TimeSpan ReplicationDestinationsTopologyTimeout { get; set; } = TimeSpan.FromSeconds(2);

        private readonly ManualResetEventSlim leaderNodeSelected = new ManualResetEventSlim();

        private Task refreshReplicationInformationTask;

        private volatile OperationMetadata leaderNode;

        private DateTime lastUpdate = DateTime.MinValue;

        private bool firstTime = true;

        private int readStripingBase;

        private static readonly ILog Log = LogManager.GetLogger(typeof(ClusterAwareRequestExecuter));

        public OperationMetadata LeaderNode
        {
            get
            {
                return leaderNode;
            }

        }

        /// <summary>
        /// Sets the leader node to a known leader that is not null and sets the leader selected event
        /// </summary>
        /// <param name="newLeader"></param>
        public void SetLeaderNodeToKnownLeader(OperationMetadata newLeader)
        {
            if (newLeader == null)
            {
                if (Log.IsDebugEnabled)
                {
                    Log.Debug($"An attempt to change the leader node to null from SetLeaderNodeToKnownLeader was detected.{Environment.NewLine}" +
                              $"{Environment.StackTrace}");
                }
                return;
            }
            if (Log.IsDebugEnabled)
            {
                var oldLeader = leaderNode == null ? "null" : leaderNode.ToString();
                Log.Debug($"Leader node is changing from {oldLeader} to {newLeader}");
            }
            leaderNode = newLeader;
            leaderNodeSelected.Set();
        }

        /// <summary>
        /// Sets the value of leader node to null and reset the leader node selected event
        /// </summary>
        /// <param name="prevValue">The condition value upon we decide if we make the set to null or not</param>
        /// <returns>true - if leader was set to null or was null already, otherwise returns false</returns>
        public bool SetLeaderNodeToNullIfPrevIsTheSame(OperationMetadata prevValue)
        {
            var realPrevValue = Interlocked.CompareExchange(ref leaderNode, null, prevValue);
            var res = realPrevValue == null || realPrevValue.Equals(prevValue);
            if (res && realPrevValue != null)
            {
                if (Log.IsDebugEnabled)
                {
                    Log.Debug($"Leader node is changing from {realPrevValue} to null.");
                }
                leaderNodeSelected.Reset();
            }
            return res;
        }

        /// <summary>
        /// This will change the leader node to the given node and raise the leader changed event if the
        /// new leader was set to a value not equal to null.
        /// </summary>
        /// <param name="newLeader">The new leader to be set.</param>
        /// <param name="isRealLeader">An indication if this is a real leader or just the primary, this will affect if we raise the leader selected event.</param>
        /// <returns>true if the leader node was changed from null to the given value, otherwise returns false</returns>
        private bool SetLeaderNodeIfLeaderIsNull(OperationMetadata newLeader, bool isRealLeader = true)
        {
            var changed = (Interlocked.CompareExchange(ref leaderNode, newLeader, null) == null);
            if (changed && isRealLeader && newLeader != null)
                leaderNodeSelected.Set();
            if (Log.IsDebugEnabled && changed)
            {
                Log.Debug($"Leader node is changing from null to {newLeader}, isRealLeader={isRealLeader}.");
            }
            return changed;
        }

        /// <summary>
        /// This method sets the leader node to null and reset the leader selected event.
        /// You should not use this method unless you're sure nobody can set the leader node 
        /// to some other value.
        /// </summary>
        public void SetLeaderNodeToNull()
        {
            leaderNode = null;
            leaderNodeSelected.Reset();
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
            return ExecuteWithinClusterInternalAsync(serverClient, method, operation, token, numberOfRetries: Math.Max(NodeUrls.Count,3));
        }

        public Task UpdateReplicationInformationIfNeededAsync(AsyncServerClient serverClient, bool force = false)
        {
            var localLeaderNode = LeaderNode;
            var updateRecently = lastUpdate.AddMinutes(5) > SystemTime.UtcNow;
            if (force == false && updateRecently && localLeaderNode != null)
            {
                if (Log.IsDebugEnabled)
                    Log.Debug($"Will not update replication information because we have a leader:{localLeaderNode} and we recently updated the topology.");
                return new CompletedTask();
            }
            //This will prevent setting leader node to null if it was updated already.
            if (SetLeaderNodeToNullIfPrevIsTheSame(localLeaderNode) == false)
                return new CompletedTask();

            return UpdateReplicationInformationForCluster(serverClient, new OperationMetadata(serverClient.Url, serverClient.PrimaryCredentials, null), operationMetadata =>
            {
                return serverClient.DirectGetReplicationDestinationsAsync(operationMetadata, null, timeout: ReplicationDestinationsTopologyTimeout).ContinueWith(t =>
                {
                    if (t.IsFaulted || t.IsCanceled)
                        return null;

                    return t.Result;
                }, CancellationToken.None, TaskContinuationOptions.ExecuteSynchronously, TaskScheduler.Default);
            });
        }

        public void AddHeaders(HttpJsonRequest httpJsonRequest, AsyncServerClient serverClient, string currentUrl, bool withClusterFailoverHeader = false)
        {
            httpJsonRequest.AddHeader(Constants.Cluster.ClusterAwareHeader, "true");

            if (serverClient.convention.FailoverBehavior == FailoverBehavior.ReadFromAllWriteToLeader)
                httpJsonRequest.AddHeader(Constants.Cluster.ClusterReadBehaviorHeader, "All");

            if (withClusterFailoverHeader)
                httpJsonRequest.AddHeader(Constants.Cluster.ClusterFailoverBehaviorHeader, "true");
        }

        public void SetReadStripingBase(int strippingBase)
        {
            this.readStripingBase = strippingBase;
        }

        private async Task<T> ExecuteWithinClusterInternalAsync<T>(AsyncServerClient serverClient, HttpMethod method, Func<OperationMetadata, IRequestTimeMetric, Task<T>> operation, CancellationToken token, int numberOfRetries = 3, bool withClusterFailoverHeader = false, int? readStripingBaseForRetries = null)
        {
            token.ThrowIfCancellationRequested();
            bool isFaultedNode = false;
            var node = LeaderNode;
            if (node == null)
            {
                if (Log.IsDebugEnabled)
                {
                    Log.Debug($"Fetching topology, {serverClient.Url}: Retries={numberOfRetries} When={DateTime.UtcNow}");
                }
#pragma warning disable 4014
                //We always want to fetch a new topology if we don't know who the leader is.
                UpdateReplicationInformationIfNeededAsync(serverClient, force: true);
#pragma warning restore 4014
                //there is no reason for us to throw cluster not reachable for a read operation when we can read from all nodes.
                if (method == HttpMethod.Get &&
                    (serverClient.convention.FailoverBehavior == FailoverBehavior.ReadFromAllWriteToLeader ||
                     serverClient.convention.FailoverBehavior == FailoverBehavior.ReadFromAllWriteToLeaderWithFailovers))
                {
                    var primaryNode = new OperationMetadata(serverClient.Url, serverClient.PrimaryCredentials, null);
                    node = GetNodeForReadOperation(primaryNode, readStripingBaseForRetries, out isFaultedNode, out readStripingBaseForRetries);
                }
                else
                {
                    switch (serverClient.convention.FailoverBehavior)
                    {
                        case FailoverBehavior.ReadFromAllWriteToLeaderWithFailovers:
                        case FailoverBehavior.ReadFromLeaderWriteToLeaderWithFailovers:
                            var waitResult = leaderNodeSelected.Wait(WaitForLeaderTimeout);
                            if (Log.IsDebugEnabled && waitResult == false)
                                Log.Debug($"Failover behavior is {serverClient.convention.FailoverBehavior}, waited for {WaitForLeaderTimeout.TotalSeconds} seconds and no leader was selected.");
                            break;
                        default:
                            if (leaderNodeSelected.Wait(WaitForLeaderTimeout) == false)
                            {
                                if (Log.IsDebugEnabled)
                                    Log.Debug($"Failover behavior is {serverClient.convention.FailoverBehavior}, waited for {WaitForLeaderTimeout.TotalSeconds} seconds and no leader was selected.");
                                throw new InvalidOperationException($"Cluster is not in a stable state. No leader was selected, but we require one for making a request using {serverClient.convention.FailoverBehavior}.");
                            }
                            break;
                    }

                    node = LeaderNode;
                }
            }

            switch (serverClient.convention.FailoverBehavior)
            {
                case FailoverBehavior.ReadFromAllWriteToLeader:
                    if (method == HttpMethods.Get)
                        node = GetNodeForReadOperation(node, readStripingBaseForRetries, out isFaultedNode, out readStripingBaseForRetries);
                    break;
                case FailoverBehavior.ReadFromAllWriteToLeaderWithFailovers:
                    if (node == null)
                    {
                        try
                        {
                            return await HandleWithFailovers(operation, token, withClusterFailoverHeader).ConfigureAwait(false);
                        }
                        catch (Exception ex)
                        {
                            if (withClusterFailoverHeader == false)
                                return await HandleWithFailovers(operation, token, true).ConfigureAwait(false);
                            throw;
                        }

                    }

                    if (method == HttpMethods.Get)
                        node = GetNodeForReadOperation(node, readStripingBaseForRetries, out isFaultedNode, out readStripingBaseForRetries);
                    break;
                case FailoverBehavior.ReadFromLeaderWriteToLeaderWithFailovers:
                    if (node == null)
                    {
                        try
                        {
                            return await HandleWithFailovers(operation, token, withClusterFailoverHeader).ConfigureAwait(false);
                        }
                        catch (Exception ex)
                        {
                            if (withClusterFailoverHeader == false)
                                return await HandleWithFailovers(operation, token, true).ConfigureAwait(false);
                            throw;
                        }                        
                    }
                    break;
            }

            var originalWithClusterFailoverValue = node.ClusterInformation.WithClusterFailoverHeader;
            AsyncOperationResult<T> operationResult;
            node.ClusterInformation.WithClusterFailoverHeader = withClusterFailoverHeader;
            try
            {
                operationResult = await TryClusterOperationAsync(node, operation, false, token).ConfigureAwait(false);
            }
            finally
            {
                node.ClusterInformation.WithClusterFailoverHeader = originalWithClusterFailoverValue;
            }

            if (operationResult.Success)
            {
                return operationResult.Result;
            }

            if (isFaultedNode) //the node had more than one failure, but we tried it anyway.
            {
                if (Log.IsDebugEnabled)
                    Log.Debug($"Failed executing operation on node {node.Url}. Connecting to this node has failed already at least once, but we tried again anyway and failed. Got the following result: {operationResult.Result}. (Timeout = {operationResult.WasTimeout})");
                
                throw operationResult.Error;
            }

            if (Log.IsDebugEnabled)
                Log.Debug($"Failed executing operation on node {node.Url} number of remaining retries: {numberOfRetries}.");

            SetLeaderNodeToNullIfPrevIsTheSame(node);
            FailureCounters.IncrementFailureCount(node.Url);

            if (serverClient.convention.FailoverBehavior == FailoverBehavior.ReadFromLeaderWriteToLeaderWithFailovers
                || serverClient.convention.FailoverBehavior == FailoverBehavior.ReadFromAllWriteToLeaderWithFailovers)
            {
                withClusterFailoverHeader = true;
            }

            if (numberOfRetries <= 0)
            {
                throw new InvalidOperationException("Cluster is not reachable. Out of retries, aborting.", operationResult.Error);
            }

            return await ExecuteWithinClusterInternalAsync(serverClient, method, operation, token, numberOfRetries - 1, withClusterFailoverHeader, readStripingBaseForRetries).ConfigureAwait(false);
        }            

  

        private OperationMetadata GetNodeForReadOperation(OperationMetadata node, int? readStripingBaseForRetriesInput, out bool isFaultedNode, out int? readStripingBaseForRetriesOutput)
        {
            readStripingBaseForRetriesOutput = readStripingBaseForRetriesInput;
            Debug.Assert(node != null);

            var nodes = new List<OperationMetadata>(NodeUrls);
            isFaultedNode = false;
            if (readStripingBase == -1)
            {
                var nodeForReadOperation = LeaderNode;
                if (ShouldExecuteUsing(nodeForReadOperation))
                    return nodeForReadOperation;

                if (Log.IsDebugEnabled)
                {
                    Log.Debug($"The leader node = {nodeForReadOperation.Url} is faulted. (Trying the leader because we are in 'ForceReadFromMaster' scope. We will try anyway, maybe it was a transient error.");
                }

                isFaultedNode = true;
                return nodeForReadOperation;
            }

            if (nodes.Count == 0)
            {
                if (ShouldExecuteUsing(node))
                    return node;

                if (Log.IsDebugEnabled)
                {
                    Log.Debug($"The node {node.Url} is faulted. We will try anyway, maybe it was a transient error.");
                }
                isFaultedNode = true;
                return node;
            }

            int stripingBase;
            if (readStripingBaseForRetriesInput.HasValue == false)
            {
                stripingBase = readStripingBase;                
            }
            else
            {
                stripingBase = readStripingBaseForRetriesInput.Value + 1;
            }

            readStripingBaseForRetriesOutput = stripingBase;

            var nodeIndex = stripingBase % nodes.Count;
            var readNode = nodes[nodeIndex];

            if (ShouldExecuteUsing(readNode))
                return readNode;

            int faultedNodes = 0;
            var foundAvailableNode = false;
            do
            {                                
                nodeIndex = ++stripingBase % nodes.Count;
                readNode = nodes[nodeIndex];
                if (ShouldExecuteUsing(readNode))
                {
                    foundAvailableNode = true;
                    break;
                }
                faultedNodes++;
            } while(faultedNodes < nodes.Count - 1); //go over all nodes except the initial

            if (foundAvailableNode == false)
            {
                if(Log.IsDebugEnabled)
                    Log.Debug($"Cluster is not reachable. Executing operation on any of the cluster nodes failed, aborting. Tried the following nodes: {string.Join(" , ", nodes.Select(x => x.Url))}");

                isFaultedNode = true;
            }

            return node;
        }

        private async Task<T> HandleWithFailovers<T>(Func<OperationMetadata, IRequestTimeMetric, Task<T>> operation, CancellationToken token, bool withClusterFailoverHeader)
        {
            var nodes = NodeUrls;
            for (var i = 0; i < nodes.Count; i++)
            {
                var n = nodes[i];

                // Have to be here more thread safe
                n.ClusterInformation.WithClusterFailoverHeader = withClusterFailoverHeader;
                if (ShouldExecuteUsing(n) == false)
                    continue;

                var hasMoreNodes = nodes.Count > i + 1;
                var result = await TryClusterOperationAsync(n, operation, hasMoreNodes, token).ConfigureAwait(false);
                if (result.Success)
                    return result.Result;
                if (Log.IsDebugEnabled)
                    Log.Debug($"Tried executing operation on failover server {n.Url} with no success.");
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

        private const int RedirectLimit = 2;
        private async Task<AsyncOperationResult<T>> TryClusterOperationAsync<T>(OperationMetadata node, Func<OperationMetadata, IRequestTimeMetric, Task<T>> operation, bool avoidThrowing, CancellationToken token, int redirectLimit = RedirectLimit)
        {
            if (redirectLimit == 0)
            {
                throw new InvalidOperationException($"Cluster is not reachable. Already got redirected {RedirectLimit} times.");
            }
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
                    if (Log.IsDebugEnabled)
                        Log.Debug($"Operation failed because server {node.Url} is down.");
                }
                else
                {
                    var ae = e as AggregateException;
                    ErrorResponseException errorResponseException;
                    if (ae != null)
                        errorResponseException = ae.ExtractSingleInnerException() as ErrorResponseException;
                    else
                        errorResponseException = e as ErrorResponseException;

                    if (errorResponseException != null)
                    {
                        if (errorResponseException.StatusCode == HttpStatusCode.Redirect)
                        {
                            IEnumerable<string> values;
                            if (errorResponseException.Response.Headers.TryGetValues("Raven-Leader-Redirect", out values) == false
                                && values.Contains("true") == false)
                            {
                                throw new InvalidOperationException("Got 302 Redirect, but without Raven-Leader-Redirect: true header, maybe there is a proxy in the middle", e);
                            }
                            var redirectUrl = errorResponseException.Response.Headers.Location.ToString();
                            var newLeaderNode = Nodes.FirstOrDefault(n => n.Url.Equals(redirectUrl)) ?? new OperationMetadata(redirectUrl, node.Credentials, node.ClusterInformation);
                            SetLeaderNodeToKnownLeader(newLeaderNode);
                            if (Log.IsDebugEnabled)
                                Log.Debug($"Redirecting to {redirectUrl} because {node.Url} responded with 302-redirect.");
                            return await TryClusterOperationAsync(newLeaderNode, operation, avoidThrowing, token, redirectLimit - 1).ConfigureAwait(false);
                        }

                        if (errorResponseException.StatusCode == HttpStatusCode.ExpectationFailed)
                        {
                            if (Log.IsDebugEnabled)
                                Log.Debug($"Operation failed with status code {HttpStatusCode.ExpectationFailed}, will retry.");
                            shouldRetry = true;
                        }
                    }
                }

                if (shouldRetry == false && avoidThrowing == false)
                    throw;

                operationResult.Error = e;

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
                    var document = ReplicationInformerLocalCache.TryLoadReplicationInformationFromLocalCache(serverHash);
                    var nodes = GetNodes(primaryNode, document?.DataAsJson.JsonDeserialization<ReplicationDocumentWithClusterInformation>());
                    if (nodes != null)
                    {
                        Nodes = nodes;
                        var newLeaderNode = GetLeaderNode(Nodes);
                        if (newLeaderNode != null)
                        {
                            if (Log.IsDebugEnabled)
                            {
                                Log.Debug($"Fetched topology from cache, Leader is {LeaderNode}\n Nodes:" + string.Join(",", Nodes.Select(n => n.Url)));
                            }
                            SetLeaderNodeToKnownLeader(newLeaderNode);
                            return new CompletedTask();
                        }
                        if (Log.IsDebugEnabled)
                        {
                            Log.Debug($"Fetched topology from cache, no leader found.\n Nodes:" + string.Join(",", Nodes.Select(n => n.Url)));
                        }
                        SetLeaderNodeToNull();
                    }
                }

                return refreshReplicationInformationTask = Task.Factory.StartNew(async () =>
                {
                    var tryFailoverServers = false;
                    var triedFailoverServers = FailoverServers == null || FailoverServers.Length == 0;
                    for (; ; )
                    {
                        //taking a snapshot so we could tell if the value changed while we fetch the topology
                        var prevLeader = LeaderNode;
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
                                Task = getReplicationDestinationsTask(operationMetadata),
                            })
                            .ToArray();

                        var tasks = replicationDocuments
                            .Select(x => (Task)x.Task)
                            .ToArray();

                        var tasksCompleted = Task.WaitAll(tasks, ReplicationDestinationsTopologyTimeout);
                        if (Log.IsDebugEnabled && tasksCompleted == false)
                        {
                            Log.Debug($"During fetch topology {tasks.Count(t => t.IsCompleted)} servers have responded out of {tasks.Length}");
                        }
                        replicationDocuments.ForEach(x =>
                        {
                            if (x.Task.IsCompleted && x.Task.Result != null)
                                FailureCounters.ResetFailureCount(x.Node.Url);
                        });

                        var newestTopologies = replicationDocuments
                            .Where(x => x.Task.IsCompleted && x.Task.Result != null)
                            .OrderByDescending(x => x.Task.Result.Term)
                            .ThenByDescending(x =>
                            {
                                var index = x.Task.Result.ClusterCommitIndex;
                                return x.Task.Result.ClusterInformation.IsLeader ? index + 1 : index;
                            }).ToList();

                        var newestTopology = newestTopologies.FirstOrDefault();

                        var hasLeaderCount = replicationDocuments
                            .Count(x => x.Task.IsCompleted && x.Task.Result != null && x.Task.Result.HasLeader);

                        if (newestTopology == null && FailoverServers != null && FailoverServers.Length > 0 && tryFailoverServers == false)
                            tryFailoverServers = true;

                        if (newestTopology == null && triedFailoverServers)
                        {
                            if (Log.IsDebugEnabled)
                                Log.Debug($"Fetching topology resulted with no topology, tried failoever servers, setting leader node to primary node ({primaryNode}).");
                            //if the leader Node is not null this means that somebody updated it, we don't want to overwrite it with the primary.
                            // i'm raising the leader changed event although we don't have a real leader because some tests don't wait for leader but actually any node
                            //Todo: change back to: if (SetLeaderNodeIfLeaderIsNull(primaryNode, false) == false)
                            if (SetLeaderNodeIfLeaderIsNull(primaryNode) == false)
                            {
                                return;
                            }

                            if (Nodes.Count == 0)
                                Nodes = new List<OperationMetadata>
                                {
                                    primaryNode
                                };
                            return;
                        }
                        if (Log.IsDebugEnabled)
                        {
                            foreach (var x in replicationDocuments)
                            {
                                Log.Debug($"Topology fetched from {x.Node.Url}");
                                Log.Debug($"{JsonConvert.SerializeObject(x.Task?.Result)}");
                            }
                        }
                        var majorityOfNodesAgreeThereIsLeader = Nodes.Count == 1 || hasLeaderCount > (newestTopology?.Task.Result.Destinations.Count + 1) / 2;
                        if (newestTopology != null && majorityOfNodesAgreeThereIsLeader)
                        {
                            var replicationDocument = newestTopology.Task.Result;
                            var node = newestTopology.Node;

                            if (newestTopologies.Count > 1 && node.Url.Equals(serverClient.Url) == false)
                            {
                                // we got the replication document not from the primary url
                                // need to add the node url destination to the destinations
                                // (we know it exists since we have majority of nodes that agree on the leader)
                                // and remove the primary url destination from the destinations

                                var sourceNode = node;
                                var sourceNodeClusterInformation = replicationDocument.ClusterInformation;
                                var destination = replicationDocument.Destinations
                                    .FirstOrDefault(x => DestinationUrl(x.Url, x.Database).Equals(serverClient.Url, StringComparison.OrdinalIgnoreCase));
                                if (destination != null)
                                {
                                    replicationDocument.Destinations.Remove(destination);
                                    // we need to update the cluster information of the primary url for this node
                                    replicationDocument.ClusterInformation = destination.ClusterInformation;
                                    node = ConvertReplicationDestinationToOperationMetadata(destination, destination.ClusterInformation);
                                }

                                destination = destination ?? replicationDocument.Destinations.FirstOrDefault();
                                if (destination != null)
                                {
                                    var database = destination.Database;
                                    var networkCredentials = sourceNode.Credentials?.Credentials as NetworkCredential;
                                    replicationDocument.Destinations.Add(new ReplicationDestination.ReplicationDestinationWithClusterInformation
                                    {
                                        Url = sourceNode.Url,
                                        Database = database,
                                        ApiKey = sourceNode.Credentials?.ApiKey,
                                        Username = networkCredentials?.UserName,
                                        Password = networkCredentials?.Password,
                                        Domain = networkCredentials?.Domain,
                                        ClusterInformation = sourceNodeClusterInformation
                                    });
                                }
                            }
                            
                            if (UpdateTopology(serverClient, node, replicationDocument, serverHash, prevLeader))
                            {
                                return;
                            }
                        }
                        await Task.Delay(3000).ConfigureAwait(false);
                    }
                }).ContinueWith(t =>
                {
                    lastUpdate = SystemTime.UtcNow;
                    refreshReplicationInformationTask = null;
                });
            }
        }

        private static string DestinationUrl(string baseUrl, string defaultDatabase)
        {
            return $"{baseUrl}/databases/{defaultDatabase}";
        }

        internal bool UpdateTopology(AsyncServerClient serverClient, OperationMetadata node, ReplicationDocumentWithClusterInformation replicationDocument, string serverHash, OperationMetadata prevLeader)
        {
            Nodes = GetNodes(node, replicationDocument);
            var newLeader = Nodes.SingleOrDefault(x => x.ClusterInformation.IsLeader);
            var document = new JsonDocument
            {
                DataAsJson = RavenJObject.FromObject(replicationDocument)
            };

            ReplicationInformerLocalCache.TrySavingReplicationInformationToLocalCache(serverHash, document);

            if (replicationDocument.ClientConfiguration != null)
            {
                if (replicationDocument.ClientConfiguration.FailoverBehavior == null)
                {
                    if (Log.IsDebugEnabled)
                        Log.Debug($"Server side fail-over configuration is set to let client decide, client decided on {serverClient.convention.FailoverBehavior}. ");
                    replicationDocument.ClientConfiguration.FailoverBehavior = serverClient.convention.FailoverBehavior;
                }
                else if (Log.IsDebugEnabled)
                {
                    Log.Debug($"Server enforced fail-over behavior {replicationDocument.ClientConfiguration.FailoverBehavior}. ");
                }
                serverClient.convention.UpdateFrom(replicationDocument.ClientConfiguration);
            }
            if (newLeader != null)
            {
                SetLeaderNodeToKnownLeader(newLeader);
                return true;
            }
            //here we try to set leader node to null but we might fail since it was changed.
            //We just need to make sure that the leader node is not null and we can stop searching.
            if (SetLeaderNodeToNullIfPrevIsTheSame(prevLeader) == false && LeaderNode != null)
                return true;
            return false;
        }

        private static OperationMetadata GetLeaderNode(IEnumerable<OperationMetadata> nodes)
        {
            return nodes.FirstOrDefault(x => x.ClusterInformation != null && x.ClusterInformation.IsLeader);
        }

        internal static List<OperationMetadata> GetNodes(OperationMetadata node, ReplicationDocumentWithClusterInformation replicationDocument)
        {
            if (node == null || replicationDocument == null)
                return null;

            var nodes = replicationDocument.Destinations
                .Select(x => ConvertReplicationDestinationToOperationMetadata(x, x.ClusterInformation))
                .Where(x => x != null)
                .ToList();

            if (nodes.Exists(x => x.Url.Equals(node.Url, StringComparison.OrdinalIgnoreCase)) == false)
            {
                nodes.Add(new OperationMetadata(node.Url, node.Credentials, replicationDocument.ClusterInformation));
            }
            
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

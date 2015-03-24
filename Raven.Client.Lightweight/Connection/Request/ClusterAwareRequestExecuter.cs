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
using Raven.Client.Extensions;

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

		public ClusterAwareRequestExecuter()
		{
			Nodes = new List<OperationMetadata>();
		}

		public int GetReadStripingBase(bool increment)
		{
			return increment ? Interlocked.Increment(ref readStripingBase) : readStripingBase;
		}

		public ReplicationDestination[] FailoverServers { get; set; }

		public Task<T> ExecuteOperationAsync<T>(AsyncServerClient serverClient, string method, int currentRequest, Func<OperationMetadata, Task<T>> operation, CancellationToken token)
		{
			return ExecuteWithinClusterInternalAsync(serverClient, method, operation, token);
		}

		public Task UpdateReplicationInformationIfNeeded(AsyncServerClient serverClient, bool force = false)
		{
			if (force == false)
			{
				if (LeaderNode != null)
					return new CompletedTask();

				if (lastUpdate.AddMinutes(5) > SystemTime.UtcNow)
					return new CompletedTask();
			}

			LeaderNode = null;
			return UpdateReplicationInformationForCluster(new OperationMetadata(serverClient.Url, serverClient.PrimaryCredentials, null), operationMetadata =>
			{
				return serverClient.DirectGetReplicationDestinationsAsync(operationMetadata, TimeSpan.FromSeconds(GetReplicationDestinationsTimeoutInSeconds)).ContinueWith(t =>
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

			if (serverClient.ClusterBehavior == ClusterBehavior.ReadFromAllWriteToLeader)
				httpJsonRequest.AddHeader(Constants.Cluster.ClusterReadBehaviorHeader, "All");

			if (serverClient.ClusterBehavior == ClusterBehavior.ReadFromAllWriteToLeaderWithFailovers || serverClient.ClusterBehavior == ClusterBehavior.ReadFromLeaderWriteToLeaderWithFailovers)
				httpJsonRequest.AddHeader(Constants.Cluster.ClusterFailoverBehaviorHeader, "true");
		}

		private async Task<T> ExecuteWithinClusterInternalAsync<T>(AsyncServerClient serverClient, string method, Func<OperationMetadata, Task<T>> operation, CancellationToken token, int numberOfRetries = 2)
		{
			token.ThrowIfCancellationRequested();

			if (numberOfRetries < 0)
				throw new InvalidOperationException("Cluster is not reachable. Out of retries, aborting.");

			var node = LeaderNode;
			if (node == null)
			{
				UpdateReplicationInformationIfNeeded(serverClient); // maybe start refresh task

				switch (serverClient.ClusterBehavior)
				{
					case ClusterBehavior.ReadFromAllWriteToLeaderWithFailovers:
					case ClusterBehavior.ReadFromLeaderWriteToLeaderWithFailovers:
						if (lastUpdate.AddMinutes(5) > SystemTime.UtcNow) 
							leaderNodeSelected.Wait(TimeSpan.FromSeconds(WaitForLeaderTimeoutInSeconds));

						node = LeaderNode;
						if (node == null)
						{
							foreach (var n in NodeUrls)
							{
								var result = await TryClusterOperationAsync(n, operation, token).ConfigureAwait(false);
								if (result.Success) 
									return result.Result;
							}

							throw new InvalidOperationException("Cluster is not reachable. Executing operation on any of the nodes failed, aborting.");
						}
						break;
					default:
						if (leaderNodeSelected.Wait(TimeSpan.FromSeconds(WaitForLeaderTimeoutInSeconds)) == false)
							throw new InvalidOperationException("Cluster is not reachable. No leader was selected, aborting.");
						node = LeaderNode;
						break;
				}
			}

			if (serverClient.ClusterBehavior == ClusterBehavior.ReadFromAllWriteToLeader && method == "GET")
			{
				var nodes = NodeUrls;
				var nodeIndex = readStripingBase % nodes.Count;
				node = nodes[nodeIndex];
			}

			var operationResult = await TryClusterOperationAsync(node, operation, token).ConfigureAwait(false);
			if (operationResult.Success)
				return operationResult.Result;

			LeaderNode = null;
			return await ExecuteWithinClusterInternalAsync(serverClient, method, operation, token, numberOfRetries - 1).ConfigureAwait(false);
		}

		private static async Task<AsyncOperationResult<T>> TryClusterOperationAsync<T>(OperationMetadata localLeaderNode, Func<OperationMetadata, Task<T>> operation, CancellationToken token)
		{
			Debug.Assert(localLeaderNode != null);

			token.ThrowIfCancellationRequested();
			var shouldRetry = false;

			var operationResult = new AsyncOperationResult<T>();
			try
			{
				operationResult.Result = await operation(localLeaderNode).ConfigureAwait(false);
				operationResult.Success = true;
			}
			catch (Exception e)
			{
				operationResult.Success = false;

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

					if (errorResponseException == null)
						throw;

					if (errorResponseException.StatusCode == HttpStatusCode.Redirect || errorResponseException.StatusCode == HttpStatusCode.ExpectationFailed)
						shouldRetry = true;
				}

				if (shouldRetry == false)
					throw;
			}

			return operationResult;
		}

		private Task UpdateReplicationInformationForCluster(OperationMetadata primaryNode, Func<OperationMetadata, Task<ReplicationDocumentWithClusterInformation>> getReplicationDestinationsTask)
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
						{
							lastUpdate = SystemTime.UtcNow;
							return new CompletedTask();
						}
					}
				}

				return refreshReplicationInformationTask = Task.Factory.StartNew(() =>
				{
					var tryFailoverServers = false;
					var triedFailoverServers = FailoverServers == null || FailoverServers.Length == 0;
					for (; ; )
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

						var newestTopology = replicationDocuments
							.Where(x => x.Task.Result != null)
							.OrderByDescending(x => x.Task.Result.ClusterCommitIndex)
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
							LeaderNode = GetLeaderNode(Nodes);

							ReplicationInformerLocalCache.TrySavingClusterNodesToLocalCache(serverHash, Nodes);

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
			if (string.IsNullOrEmpty(url) || destination.Disabled || destination.IgnoredClient)
				return null;

			if (string.IsNullOrEmpty(destination.Database))
				return new OperationMetadata(url, destination.Username, destination.Password, destination.Domain, destination.ApiKey, clusterInformation);

			return new OperationMetadata(MultiDatabase.GetRootDatabaseUrl(url).ForDatabase(destination.Database), destination.Username, destination.Password, destination.Domain, destination.ApiKey, clusterInformation);
		}

		public IDisposable ForceReadFromMaster()
		{
			return new DisposableAction(() => { });
		}

		public event EventHandler<FailoverStatusChangedEventArgs> FailoverStatusChanged = delegate { };
	}
}
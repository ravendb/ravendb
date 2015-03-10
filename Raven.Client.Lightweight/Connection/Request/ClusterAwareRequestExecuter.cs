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
		private const int WaitForLeaderTimeoutInSeconds = 15;

		private const int GetReplicationDestinationsTimeoutInSeconds = WaitForLeaderTimeoutInSeconds / 3;

		private readonly AsyncServerClient serverClient;

		private readonly ManualResetEventSlim leaderNodeSelected = new ManualResetEventSlim();

		private Task refreshReplicationInformationTask;

		private OperationMetadata leaderNode;

		private DateTime lastUpdate = DateTime.MinValue;

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

		public ClusterAwareRequestExecuter(AsyncServerClient serverClient)
		{
			this.serverClient = serverClient;
			Nodes = new List<OperationMetadata>();
		}

		public ReplicationDestination[] FailoverServers { get; set; }

		public Task<T> ExecuteOperationAsync<T>(string method, int currentRequest, int currentReadStripingBase, Func<OperationMetadata, Task<T>> operation, CancellationToken token)
		{
			return ExecuteWithinClusterInternalAsync(method, operation, token);
		}

		public Task UpdateReplicationInformationIfNeeded(bool force = false)
		{
			if (force == false && lastUpdate.AddMinutes(5) > SystemTime.UtcNow && LeaderNode != null)
				return new CompletedTask();

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

		public HttpJsonRequest AddHeaders(HttpJsonRequest httpJsonRequest)
		{
			httpJsonRequest.AddHeader(Constants.Cluster.ClusterAwareHeader, "true");
			return httpJsonRequest;
		}

		private async Task<T> ExecuteWithinClusterInternalAsync<T>(string method, Func<OperationMetadata, Task<T>> operation, CancellationToken token, int numberOfRetries = 2)
		{
			token.ThrowIfCancellationRequested();

			if (numberOfRetries < 0)
				throw new InvalidOperationException("Cluster is not reachable. Out of retries, aborting.");

			var localLeaderNode = LeaderNode;
			if (localLeaderNode == null)
			{
				UpdateReplicationInformationIfNeeded(); // maybe start refresh task

				if (leaderNodeSelected.Wait(TimeSpan.FromSeconds(WaitForLeaderTimeoutInSeconds)) == false)
					throw new InvalidOperationException("Cluster is not reachable. No leader was selected, aborting.");

				localLeaderNode = LeaderNode;
			}

			return await TryClusterOperationAsync(method, localLeaderNode, operation, token, numberOfRetries).ConfigureAwait(false);
		}

		private async Task<T> TryClusterOperationAsync<T>(string method, OperationMetadata localLeaderNode, Func<OperationMetadata, Task<T>> operation, CancellationToken token, int numberOfRetries)
		{
			Debug.Assert(localLeaderNode != null);

			token.ThrowIfCancellationRequested();

			var shouldRetry = false;
			try
			{
				return await operation(localLeaderNode).ConfigureAwait(false);
			}
			catch (Exception e)
			{
				var ae = e as AggregateException;
				ErrorResponseException errorResponseException;
				if (ae != null)
					errorResponseException = ae.ExtractSingleInnerException() as ErrorResponseException;
				else
					errorResponseException = e as ErrorResponseException;

				bool wasTimeout;
				if (HttpConnectionHelper.IsServerDown(e, out wasTimeout))
					shouldRetry = true;
				else
				{
					if (errorResponseException == null)
						throw;

					if (errorResponseException.StatusCode == HttpStatusCode.Redirect || errorResponseException.StatusCode == HttpStatusCode.ExpectationFailed)
						shouldRetry = true;
				}

				if (shouldRetry == false)
					throw;
			}

			LeaderNode = null;
			return await ExecuteWithinClusterInternalAsync(method, operation, token, numberOfRetries - 1).ConfigureAwait(false);
		}

		private Task UpdateReplicationInformationForCluster(OperationMetadata primaryNode, Func<OperationMetadata, Task<ReplicationDocumentWithClusterInformation>> getReplicationDestinationsTask)
		{
			lock (this)
			{
				var taskCopy = refreshReplicationInformationTask;
				if (taskCopy != null)
					return taskCopy;

				return refreshReplicationInformationTask = Task.Factory.StartNew(() =>
				{
					for (var i = 0; i < 20; i++)
					{
						var nodes = NodeUrls;

						if (nodes.Count == 0)
							nodes.Add(primaryNode);

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

						if (newestTopology == null)
						{
							LeaderNode = primaryNode;
							Nodes = new List<OperationMetadata>
							{
								primaryNode
							};
							return;
						}

						Nodes = GetNodes(newestTopology.Node, newestTopology.Task.Result);
						LeaderNode = GetLeaderNode(Nodes);

						if (LeaderNode != null)
							return;

						Thread.Sleep(500);
					}

					LeaderNode = primaryNode;
					Nodes = new List<OperationMetadata>
					{
						primaryNode
					};
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
			var nodes = replicationDocument.Destinations.Select(x =>
			{
				var url = string.IsNullOrEmpty(x.ClientVisibleUrl) ? x.Url : x.ClientVisibleUrl;
				if (string.IsNullOrEmpty(url) || x.Disabled || x.IgnoredClient)
					return null;

				if (string.IsNullOrEmpty(x.Database))
					return new OperationMetadata(url, x.Username, x.Password, x.Domain, x.ApiKey, x.ClusterInformation);

				return new OperationMetadata(MultiDatabase.GetRootDatabaseUrl(url) + "/databases/" + x.Database + "/", x.Username, x.Password, x.Domain, x.ApiKey, x.ClusterInformation);
			})
			.Where(x => x != null)
			.ToList();

			nodes.Add(new OperationMetadata(node.Url, node.Credentials, replicationDocument.ClusterInformation));

			return nodes;
		}
	}
}
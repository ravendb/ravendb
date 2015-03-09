// -----------------------------------------------------------------------
//  <copyright file="ClusterRequestExecuter.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Threading;
using System.Threading.Tasks;

using Raven.Abstractions.Cluster;
using Raven.Abstractions.Connection;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Replication;
using Raven.Client.Connection.Async;
using Raven.Client.Extensions;

namespace Raven.Client.Connection.Request
{
	public class ClusterRequestExecuter : IRequestExecuter
	{
		private readonly AsyncServerClient serverClient;

		private readonly ManualResetEventSlim leaderNodeSelected = new ManualResetEventSlim();

		private Task refreshReplicationInformationTask;

		private OperationMetadata leaderNode;

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

		public ClusterRequestExecuter(AsyncServerClient serverClient)
		{
			if (serverClient.convention.ClusterBehavior == ClusterBehavior.None)
				throw new InvalidOperationException("Invalid cluster behavior.");

			this.serverClient = serverClient;
			Nodes = new List<OperationMetadata>();
		}

		public Task<T> ExecuteOperationAsync<T>(string method, int currentRequest, int currentReadStripingBase, Func<OperationMetadata, Task<T>> operation, CancellationToken token)
		{
			return ExecuteWithinClusterInternalAsync(method, operation, token);
		}

		public Task UpdateReplicationInformationIfNeeded()
		{
			return UpdateReplicationInformationForCluster(new OperationMetadata(serverClient.Url, serverClient.PrimaryCredentials, null), operationMetadata => serverClient.DirectGetReplicationDestinationsAsync(operationMetadata).ResultUnwrap());
		}

		private async Task<T> ExecuteWithinClusterInternalAsync<T>(string method, Func<OperationMetadata, Task<T>> operation, CancellationToken token, int numberOfRetries = 3)
		{
			token.ThrowIfCancellationRequested();

			if (numberOfRetries < 0)
				throw new InvalidOperationException("Cluster is not reachable. Out of retries, aborting.");

			var localLeaderNode = LeaderNode;
			if (localLeaderNode == null)
			{
				UpdateReplicationInformationForCluster(new OperationMetadata(serverClient.Url, serverClient.PrimaryCredentials, null), operationMetadata => serverClient.DirectGetReplicationDestinationsAsync(operationMetadata).ResultUnwrap());

				if (leaderNodeSelected.Wait(TimeSpan.FromSeconds(10)) == false)
					throw new InvalidOperationException("Cluster is not reachable. No leader was selected, aborting.");
			}

			localLeaderNode = LeaderNode;
			if (localLeaderNode == null)
				return await ExecuteWithinClusterInternalAsync(method, operation, token, numberOfRetries - 1);

			return await TryClusterOperationAsync(method, localLeaderNode, operation, token, numberOfRetries);
		}

		private Task<T> TryClusterOperationAsync<T>(string method, OperationMetadata localLeaderNode, Func<OperationMetadata, Task<T>> operation, CancellationToken token, int numberOfRetries)
		{
			token.ThrowIfCancellationRequested();

			if (numberOfRetries < 0)
				throw new InvalidOperationException("Cluster is not reachable. Out of retries, aborting.");

			try
			{
				return operation(localLeaderNode);
			}
			catch (Exception e)
			{
				var ae = e as AggregateException;
				ErrorResponseException errorResponseException;
				if (ae != null)
					errorResponseException = ae.ExtractSingleInnerException() as ErrorResponseException;
				else
					errorResponseException = e as ErrorResponseException;

				if (errorResponseException == null)
					throw;

				if (errorResponseException.StatusCode == HttpStatusCode.Redirect)
				{
					LeaderNode = null;
					return ExecuteWithinClusterInternalAsync(method, operation, token, numberOfRetries);
				}

				if (errorResponseException.StatusCode == HttpStatusCode.Unauthorized)
				{
					return TryClusterOperationAsync(method, localLeaderNode, operation, token, numberOfRetries - 1);
				}

				throw;
			}
		}

		private Task UpdateReplicationInformationForCluster(OperationMetadata primaryNode, Func<OperationMetadata, ReplicationDocumentWithClusterInformation> getReplicationDestinations)
		{
			if (serverClient.convention.ClusterBehavior == ClusterBehavior.None)
				throw new InvalidOperationException("Invalid cluster behavior.");

			lock (this)
			{
				var taskCopy = refreshReplicationInformationTask;
				if (taskCopy != null)
					return taskCopy;

				return refreshReplicationInformationTask = Task.Factory.StartNew(() =>
				{
					for(var i = 0; i < 20; i++)
					{
						var nodes = NodeUrls;

						if (nodes.Count == 0)
							nodes.Add(primaryNode);

						var replicationDocuments = nodes
							.Select(operationMetadata => new
							{
								Node = operationMetadata,
								ReplicationDocument = getReplicationDestinations(operationMetadata)
							})
							.ToArray();

						var newestTopology = replicationDocuments
							.Where(x => x.ReplicationDocument != null)
							.OrderByDescending(x => x.ReplicationDocument.ClusterCommitIndex)
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

						Nodes = GetNodes(newestTopology.Node, newestTopology.ReplicationDocument);
						LeaderNode = GetLeaderNode(Nodes);

						if (LeaderNode != null)
							return;

						Thread.Sleep(500);
					}
				}).ContinueWith(t => refreshReplicationInformationTask = null);
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
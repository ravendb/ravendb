// -----------------------------------------------------------------------
//  <copyright file="DefaultRequestExecuter.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Threading;
using System.Threading.Tasks;

using Raven.Client.Connection.Async;

namespace Raven.Client.Connection.Request
{
	public class DefaultRequestExecuter : IRequestExecuter
	{
		private readonly AsyncServerClient serverClient;

		private readonly IDocumentStoreReplicationInformer replicationInformer;

		public DefaultRequestExecuter(AsyncServerClient serverClient, IDocumentStoreReplicationInformer replicationInformer)
		{
			this.serverClient = serverClient;
			this.replicationInformer = replicationInformer;
		}

		public Task<T> ExecuteOperationAsync<T>(string method, int currentRequest, int currentReadStripingBase, Func<OperationMetadata, Task<T>> operation, CancellationToken token)
		{
			return replicationInformer.ExecuteWithReplicationAsync(method, serverClient.Url, serverClient.PrimaryCredentials, currentRequest, currentReadStripingBase, operation, token);
		}

		public Task UpdateReplicationInformationIfNeeded()
		{
			return replicationInformer.UpdateReplicationInformationIfNeeded(serverClient);
		}
	}
}
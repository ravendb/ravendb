// -----------------------------------------------------------------------
//  <copyright file="IReplicationInformerBase.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using Raven.NewClient.Abstractions.Connection;
using Raven.NewClient.Client.Connection.Request;
using Raven.NewClient.Client.Metrics;

namespace Raven.NewClient.Client.Connection
{
    public interface IReplicationInformerBase : IDisposable
    {
        /// <summary>
        /// Notify when the failover status changed
        /// </summary>
        event EventHandler<FailoverStatusChangedEventArgs> FailoverStatusChanged;

        /// <summary>
        /// Set how long we will wait between pinging failed servers
        /// Is set to 0, will not ping failed servers
        /// </summary>
        int DelayTimeInMiliSec { get; set; }

        List<OperationMetadata> ReplicationDestinations { get; }
        /// <summary>
        /// Gets the replication destinations.
        /// </summary>
        /// <value>The replication destinations.</value>
        List<OperationMetadata> ReplicationDestinationsUrls { get; }

        int GetReadStripingBase(bool increment);

        FailureCounters FailureCounters { get; }

        Task<T> ExecuteWithReplicationAsync<T>(HttpMethod method, string primaryUrl, OperationCredentials primaryCredentials, RequestTimeMetric primaryRequestTimeMetric, int currentRequest, int currentReadStripingBase, Func<OperationMetadata, Task<T>> operation, CancellationToken token = default (CancellationToken));
    }

    public interface IReplicationInformerBase<in TClient> : IReplicationInformerBase
    {
        /// <summary>
        /// Refreshes the replication information.
        /// Expert use only.
        /// </summary>
        void RefreshReplicationInformation(TClient client);

        /// <summary>
        /// Clears the replication information local cache.
        /// Expert use only.
        /// </summary>
        void ClearReplicationInformationLocalCache(TClient client);
    }
}

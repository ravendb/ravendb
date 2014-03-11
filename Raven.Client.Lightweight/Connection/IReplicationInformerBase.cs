// -----------------------------------------------------------------------
//  <copyright file="IReplicationInformerBase.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Net;
using System.Threading.Tasks;
using Raven.Abstractions.Connection;

namespace Raven.Client.Connection
{
    public interface IReplicationInformerBase<in TClient> : IDisposable
    {
        /// <summary>
        /// Notify when the failover status changed
        /// </summary>
        event EventHandler<FailoverStatusChangedEventArgs> FailoverStatusChanged;

        List<OperationMetadata> ReplicationDestinations { get; }
        /// <summary>
        /// Gets the replication destinations.
        /// </summary>
        /// <value>The replication destinations.</value>
        List<OperationMetadata> ReplicationDestinationsUrls { get; }

        /// <summary>
        /// Updates the replication information if needed.
        /// </summary>
        Task UpdateReplicationInformationIfNeeded(TClient client);

        /// <summary>
        /// Refreshes the replication information.
        /// Expert use only.
        /// </summary>
        void RefreshReplicationInformation(TClient client);

        /// <summary>
        /// Get the current failure count for the url
        /// </summary>
        long GetFailureCount(string operationUrl);

        /// <summary>
        /// Get failure last check time for the url
        /// </summary>
        DateTime GetFailureLastCheck(string operationUrl);

        int GetReadStripingBase();

	    T ExecuteWithReplication<T>(string method, string primaryUrl, OperationCredentials primaryCredentials, int currentRequest, int currentReadStripingBase, Func<OperationMetadata, T> operation);

        Task<T> ExecuteWithReplicationAsync<T>(string method, string primaryUrl, OperationCredentials primaryCredentials, int currentRequest, int currentReadStripingBase, Func<OperationMetadata, Task<T>> operation);

        void ForceCheck(string primaryUrl, bool shouldForceCheck);

		bool IsServerDown(Exception exception, out bool timeout);

	    bool IsHttpStatus(Exception e, params HttpStatusCode[] httpStatusCode);
    }
}
// -----------------------------------------------------------------------
//  <copyright file="ReplicationAwareRequestExecuter.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Collections.Specialized;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using Raven.NewClient.Abstractions.Data;
using Raven.NewClient.Abstractions.Extensions;
using Raven.NewClient.Abstractions.Replication;
using Raven.NewClient.Client.Connection.Async;
using Raven.NewClient.Client.Connection.Implementation;
using Raven.NewClient.Client.Helpers;
using Raven.NewClient.Client.Metrics;

namespace Raven.NewClient.Client.Connection.Request
{
    public class ReplicationAwareRequestExecuter : IRequestExecuter
    {
        private readonly IDocumentStoreReplicationInformer replicationInformer;

        private readonly RequestTimeMetric requestTimeMetric;

        private int readStripingBase;

        public ReplicationAwareRequestExecuter(IDocumentStoreReplicationInformer replicationInformer, RequestTimeMetric requestTimeMetric)
        {
            this.replicationInformer = replicationInformer;
            this.requestTimeMetric = requestTimeMetric;
        }

        public IDocumentStoreReplicationInformer ReplicationInformer
        {
            get { return replicationInformer; }
        }

        public int GetReadStripingBase(bool increment)
        {
            return readStripingBase = replicationInformer.GetReadStripingBase(increment);
        }

        public ReplicationDestination[] FailoverServers
        {
            get
            {
                return replicationInformer.FailoverServers;
            }
            set
            {
                replicationInformer.FailoverServers = value;
            }
        }

        public Task<T> ExecuteOperationAsync<T>(AsyncServerClient serverClient, HttpMethod method, int currentRequest, Func<OperationMetadata, Task<T>> operation, CancellationToken token)
        {
            return replicationInformer.ExecuteWithReplicationAsync(method, serverClient.Url, serverClient.PrimaryCredentials, requestTimeMetric, currentRequest, readStripingBase, operation, token);
        }

        public Task UpdateReplicationInformationIfNeededAsync(AsyncServerClient serverClient, bool force = false)
        {
            if (force)
                throw new NotSupportedException("Force is not supported in ReplicationAwareRequestExecuter");

            return replicationInformer.UpdateReplicationInformationIfNeededAsync(serverClient);
        }

        public void AddHeaders(HttpJsonRequest httpJsonRequest, AsyncServerClient serverClient, string currentUrl)
        {
            if (serverClient.Url.Equals(currentUrl, StringComparison.OrdinalIgnoreCase))
                return;
            if (ReplicationInformer.FailureCounters.GetFailureCount(serverClient.Url) <= 0)
                return; // not because of failover, no need to do this.

            var lastPrimaryCheck = ReplicationInformer.FailureCounters.GetFailureLastCheck(serverClient.Url);
            httpJsonRequest.AddHeader(Constants.Headers.RavenClientPrimaryServerUrl, ToRemoteUrl(serverClient.Url));
            httpJsonRequest.AddHeader(Constants.Headers.RavenClientPrimaryServerLastCheck, lastPrimaryCheck.ToString("s"));

            httpJsonRequest.AddReplicationStatusChangeBehavior(serverClient.Url, currentUrl, HandleReplicationStatusChanges);

        }

        private static string ToRemoteUrl(string primaryUrl)
        {
            var uriBuilder = new UriBuilder(primaryUrl);
            if (uriBuilder.Host == "localhost" || uriBuilder.Host == "127.0.0.1")
                uriBuilder.Host = EnvironmentHelper.MachineName;
            return uriBuilder.Uri.ToString();
        }

        public IDisposable ForceReadFromMaster()
        {
            var old = readStripingBase;
            readStripingBase = -1;// this means that will have to use the master url first
            return new DisposableAction(() => readStripingBase = old);
        }

        public void HandleReplicationStatusChanges(NameValueCollection headers, string primaryUrl, string currentUrl)
        {
            if (primaryUrl.Equals(currentUrl, StringComparison.OrdinalIgnoreCase))
                return;

            var forceCheck = headers[Constants.Headers.RavenForcePrimaryServerCheck];
            bool shouldForceCheck;
            if (!string.IsNullOrEmpty(forceCheck) && bool.TryParse(forceCheck, out shouldForceCheck))
            {
                replicationInformer.FailureCounters.ForceCheck(primaryUrl, shouldForceCheck);
            } 
        }

        public event EventHandler<FailoverStatusChangedEventArgs> FailoverStatusChanged {
            add { replicationInformer.FailoverStatusChanged += value; }
            remove { replicationInformer.FailoverStatusChanged -= value; }
        }
    }
}

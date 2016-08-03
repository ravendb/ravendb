using System;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;

using Raven.Abstractions.Replication;
using Raven.Client.Connection.Async;
using Raven.Client.Connection.Implementation;
using Raven.Client.Metrics;

namespace Raven.Client.Connection.Request
{
    public interface IRequestExecuter
    {
        int GetReadStripingBase(bool increment);

        ReplicationDestination[] FailoverServers { get; set; }

        Task<T> ExecuteOperationAsync<T>(AsyncServerClient serverClient, HttpMethod method, int currentRequest, Func<OperationMetadata, IRequestTimeMetric, Task<T>> operation, CancellationToken token);

        Task UpdateReplicationInformationIfNeededAsync(AsyncServerClient serverClient, bool force = false);

        IDisposable ForceReadFromMaster();

        event EventHandler<FailoverStatusChangedEventArgs> FailoverStatusChanged;

        void AddHeaders(HttpJsonRequest httpJsonRequest, AsyncServerClient serverClient, string currentUrl, bool withClusterFailoverHeader = false);
        void SetReadStripingBase(int strippingBase);
    }
}

using System;
using System.Diagnostics.CodeAnalysis;
using System.Security.Cryptography.X509Certificates;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client;
using Raven.Client.Documents.Conventions;
using Raven.Client.Http;
using Raven.Client.ServerWide.Commands;
using Raven.Client.Util;
using Raven.Server.ServerWide.Commands;
using Sparrow.Json;

namespace Raven.Server.Utils
{
    internal static class ReplicationUtils
    {
        public static TcpConnectionInfo GetTcpInfoForInternalReplication(string url, string databaseName, string databaseId, long etag, string tag, X509Certificate2 certificate, string localNodeTag, CancellationToken token)
        {
            return AsyncHelpers.RunSync(() => GetTcpInfoForInternalReplicationAsync(url, databaseName, databaseId, etag, tag, certificate, localNodeTag, token));
        }

        public static TcpConnectionInfo GetServerTcpInfo(string url, string tag, X509Certificate2 certificate, CancellationToken token)
        {
            return AsyncHelpers.RunSync(() => GetServerTcpInfoAsync(url, tag, certificate, token));
        }

        public static async Task<TcpConnectionInfo> GetServerTcpInfoAsync(string url, string tag, X509Certificate2 certificate, CancellationToken token)
        {
            var getTcpInfoCommand = new GetTcpInfoCommand(tag);
            return await GetTcpInfoAsync(url, getTcpInfoCommand, certificate, token);
        }

        public static async Task<TcpConnectionInfo> GetDatabaseTcpInfoAsync(string senderUrl, string url, string databaseName, string tag, X509Certificate2 certificate, CancellationToken token)
        {
            var getTcpInfoCommand = new GetTcpInfoCommand(senderUrl, tag, databaseName);
            return await GetTcpInfoAsync(url, getTcpInfoCommand, certificate, token);
        }

        private static async Task<TcpConnectionInfo> GetTcpInfoAsync(string url, GetTcpInfoCommand getTcpInfoCommand, X509Certificate2 certificate, CancellationToken token)
        {
            using (var requestExecutor = ClusterRequestExecutor.CreateForShortTermUse(url, certificate, DocumentConventions.DefaultForServer))//TODO stav: createForFixed instead?
            using (requestExecutor.ContextPool.AllocateOperationContext(out var context))
            {
                await requestExecutor.ExecuteAsync(getTcpInfoCommand, context, token: token);

                var tcpConnectionInfo = getTcpInfoCommand.Result;
                if (url.StartsWith("https", StringComparison.OrdinalIgnoreCase) && tcpConnectionInfo.Certificate == null)
                    throw new InvalidOperationException("Getting TCP info over HTTPS but the server didn't return the expected certificate to use over TCP, invalid response, aborting");
                return tcpConnectionInfo;
            }
        }

        private static async Task<TcpConnectionInfo> GetTcpInfoForInternalReplicationAsync(string url, string databaseName, string databaseId, long etag, string tag, X509Certificate2 certificate, string localNodeTag, CancellationToken token)
        {
            using (var requestExecutor = ClusterRequestExecutor.CreateForShortTermUse(url, certificate, DocumentConventions.DefaultForServer))
            using (requestExecutor.ContextPool.AllocateOperationContext(out var context))
            {
                var getTcpInfoCommand = databaseId == null ? new GetTcpInfoForReplicationCommand(localNodeTag, tag, databaseName) : new GetTcpInfoForReplicationCommand(localNodeTag, tag, databaseName, databaseId, etag);
                await requestExecutor.ExecuteAsync(getTcpInfoCommand, context, token: token);

                var tcpConnectionInfo = getTcpInfoCommand.Result;
                if (url.StartsWith("https", StringComparison.OrdinalIgnoreCase) && tcpConnectionInfo.Certificate == null)
                    throw new InvalidOperationException("Getting TCP info over HTTPS but the server didn't return the expected certificate to use over TCP, invalid response, aborting");
                return tcpConnectionInfo;
            }
        }

        public static void EnsureCollectionTag(BlittableJsonReaderObject obj, string collection)
        {
            if (obj.TryGet(Constants.Documents.Metadata.Key, out BlittableJsonReaderObject metadata) == false ||
                metadata.TryGet(Constants.Documents.Metadata.Collection, out string actualCollection) == false ||
                actualCollection != collection)
            {
                if (collection == Constants.Documents.Collections.EmptyCollection)
                    return;

                ThrowInvalidCollectionAfterResolve(collection, null);
            }
        }

        [DoesNotReturn]
        private static void ThrowInvalidCollectionAfterResolve(string collection, string actual)
        {
            throw new InvalidOperationException(
                "Resolving script did not setup the appropriate '@collection'. Expected " + collection + " but got " +
                actual);
        }
    }
}

using System;
using System.Security.Cryptography.X509Certificates;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client;
using Raven.Client.Http;
using Raven.Client.ServerWide.Commands;
using Raven.Client.Util;
using Raven.Server.Documents;
using Sparrow.Json;

namespace Raven.Server.Utils
{
    internal static class ReplicationUtils
    {
        public static TcpConnectionInfo GetTcpInfo(string url, string databaseName, string databaseId, long etag, string tag, X509Certificate2 certificate, CancellationToken token)
        {
            return AsyncHelpers.RunSync(() => GetTcpInfoAsync(url, databaseName, databaseId, etag, tag, certificate, token));
        }

        public static TcpConnectionInfo GetTcpInfo(string url, string databaseName, string tag, X509Certificate2 certificate, CancellationToken token)
        {
            return AsyncHelpers.RunSync(() => GetTcpInfoAsync(url, databaseName, tag, certificate, token));
        }

        public static async Task<TcpConnectionInfo> GetTcpInfoAsync(string url, string databaseName, string tag, X509Certificate2 certificate, CancellationToken token)
        {
            return await GetTcpInfoAsync(url, databaseName, null, default, tag, certificate, token);
        }

        public static async Task<TcpConnectionInfo> GetTcpInfoAsync(string url, string databaseName, string databaseId, long etag, string tag, X509Certificate2 certificate, CancellationToken token)
        {
            using (var requestExecutor = ClusterRequestExecutor.CreateForSingleNode(url, certificate))
            using (requestExecutor.ContextPool.AllocateOperationContext(out var context))
            {
                var getTcpInfoCommand = databaseId == null ? new GetTcpInfoCommand(tag, databaseName) : new GetTcpInfoCommand(tag, databaseName, databaseId, etag);
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

        private static void ThrowInvalidCollectionAfterResolve(string collection, string actual)
        {
            throw new InvalidOperationException(
                "Resolving script did not setup the appropriate '@collection'. Expected " + collection + " but got " +
                actual);
        }
    }
}

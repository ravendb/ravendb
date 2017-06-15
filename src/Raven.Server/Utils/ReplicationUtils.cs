using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Raven.Client;
using Raven.Client.Documents.Replication;
using Raven.Client.Documents.Replication.Messages;
using Raven.Client.Http;
using Raven.Client.Server.Commands;
using Raven.Server.Documents;
using Raven.Server.Documents.Replication;
using Sparrow.Json;

namespace Raven.Server.Utils
{
    internal static class ReplicationUtils
    {
        public static TcpConnectionInfo GetTcpInfo(string url, string databaseName, string apiKey, string tag)
        {
            JsonOperationContext context;
            using (var requestExecutor = ClusterRequestExecutor.CreateForSingleNode(url, apiKey))
            using (requestExecutor.ContextPool.AllocateOperationContext(out context))
            {
                var getTcpInfoCommand = new GetTcpInfoCommand(tag + "/" + databaseName);
                requestExecutor.Execute(getTcpInfoCommand, context);

                return getTcpInfoCommand.Result;
            }
        }

        public static async Task<TcpConnectionInfo> GetTcpInfoAsync(string url, string databaseName, string apiKey, string tag)
        {
            using (var requestExecutor = ClusterRequestExecutor.CreateForSingleNode(url, apiKey))
            using (requestExecutor.ContextPool.AllocateOperationContext(out JsonOperationContext context))
            {                
                var getTcpInfoCommand = new GetTcpInfoCommand(tag + "/" + databaseName);
                await requestExecutor.ExecuteAsync(getTcpInfoCommand, context);

                return getTcpInfoCommand.Result;
            }
        }

        public static void EnsureCollectionTag(BlittableJsonReaderObject obj, string collection)
        {
            if (obj.TryGet(Constants.Documents.Metadata.Key, out BlittableJsonReaderObject metadata) == false ||
                metadata.TryGet(Constants.Documents.Metadata.Collection, out string actualCollection) == false ||
                actualCollection != collection)
            {
                if (collection == CollectionName.EmptyCollection)
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
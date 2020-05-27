// -----------------------------------------------------------------------
//  <copyright file="CompressionHandler.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System.Net;
using System.Threading.Tasks;
using Raven.Client.ServerWide;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers
{
    public class CompressionHandler : DatabaseRequestHandler
    {
        [RavenAction("/databases/*/compression/config", "GET", AuthorizationStatus.ValidUser)]
        public Task GetCompressionConfig()
        {
            using (Server.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            using (context.OpenReadTransaction())
            {
                DocumentsCompressionConfiguration compressionConfig;
                using (var recordRaw = Server.ServerStore.Cluster.ReadRawDatabaseRecord(context, Database.Name))
                {
                    compressionConfig = recordRaw?.DocumentsCompressionConfiguration;
                }

                if (compressionConfig != null)
                {
                    using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
                    {
                        context.Write(writer, compressionConfig.ToJson());
                    }
                }
                else
                {
                    HttpContext.Response.StatusCode = (int)HttpStatusCode.NotFound;
                }
            }
            return Task.CompletedTask;
        }

        [RavenAction("/databases/*/admin/compression/config", "POST", AuthorizationStatus.DatabaseAdmin)]
        public async Task ConfigCompression()
        {
            await DatabaseConfigurations(ServerStore.ModifyDocumentsCompression, "write-compression-config", GetRaftRequestIdFromQuery());
        }
    }
}

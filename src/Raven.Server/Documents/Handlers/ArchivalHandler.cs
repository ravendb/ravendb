// -----------------------------------------------------------------------
//  <copyright file="RevisionsHandler.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System.Net;
using System.Threading.Tasks;
using Raven.Client.Documents.Operations.Archival;
using Raven.Server.Documents.Handlers.Processors.Archival;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers
{
    public class ArchivalHandler : DatabaseRequestHandler
    {
        [RavenAction("/databases/*/archival/config", "GET", AuthorizationStatus.ValidUser, EndpointType.Read)]
        public async Task GetArchivalConfig()
        {
            using (Server.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            using (context.OpenReadTransaction())
            {
                ArchivalConfiguration archivalConfiguration;
                using (var recordRaw = Server.ServerStore.Cluster.ReadRawDatabaseRecord(context, Database.Name))
                {
                    archivalConfiguration = recordRaw?.ArchivalConfiguration;
                }

                if (archivalConfiguration != null)
                {
                    await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
                    {
                        context.Write(writer, archivalConfiguration.ToJson());
                    }
                }
                else
                {
                    HttpContext.Response.StatusCode = (int)HttpStatusCode.NotFound;
                }
            }
        }

        [RavenAction("/databases/*/admin/archival/config", "POST", AuthorizationStatus.DatabaseAdmin)]
        public async Task ConfigArchival()
        {
            using (var processor = new ArchivalHandlerProcessorForPost(this))
                await processor.ExecuteAsync();
        }
    }
}

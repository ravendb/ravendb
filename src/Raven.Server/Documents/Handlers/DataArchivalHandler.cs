// -----------------------------------------------------------------------
//  <copyright file="RevisionsHandler.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System.Net;
using System.Threading.Tasks;
using Raven.Client.Documents.Operations.DataArchival;
using Raven.Server.Documents.Handlers.Processors.DataArchival;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers
{
    public class DataArchivalHandler : DatabaseRequestHandler
    {
        [RavenAction("/databases/*/data-archival/config", "GET", AuthorizationStatus.ValidUser, EndpointType.Read)]
        public async Task GetArchivalConfig()
        {
            using (Server.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            using (context.OpenReadTransaction())
            {
                DataArchivalConfiguration dataArchivalConfiguration;
                using (var recordRaw = Server.ServerStore.Cluster.ReadRawDatabaseRecord(context, Database.Name))
                {
                    dataArchivalConfiguration = recordRaw?.DataArchivalConfiguration;
                }

                if (dataArchivalConfiguration != null)
                {
                    await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
                    {
                        context.Write(writer, dataArchivalConfiguration.ToJson());
                    }
                }
                else
                {
                    HttpContext.Response.StatusCode = (int)HttpStatusCode.NotFound;
                }
            }
        }

        [RavenAction("/databases/*/admin/data-archival/config", "POST", AuthorizationStatus.DatabaseAdmin)]
        public async Task ConfigArchival()
        {
            using (var processor = new DataArchivalHandlerProcessorForPost(this))
                await processor.ExecuteAsync();
        }
    }
}

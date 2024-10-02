// -----------------------------------------------------------------------
//  <copyright file="RevisionsHandler.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System.Threading.Tasks;
using Raven.Server.Documents.Handlers.Processors.DataArchival;
using Raven.Server.Routing;

namespace Raven.Server.Documents.Handlers
{
    public class DataArchivalHandler : DatabaseRequestHandler
    {
        [RavenAction("/databases/*/data-archival/config", "GET", AuthorizationStatus.ValidUser, EndpointType.Read)]
        public async Task GetArchivalConfig()
        {
            using (var processor = new DataArchivalHandlerProcessorForGet(this))
                await processor.ExecuteAsync();
        }

        [RavenAction("/databases/*/admin/data-archival/config", "POST", AuthorizationStatus.DatabaseAdmin)]
        public async Task ConfigArchival()
        {
            using (var processor = new DataArchivalHandlerProcessorForPost(this))
                await processor.ExecuteAsync();
        }
    }
}

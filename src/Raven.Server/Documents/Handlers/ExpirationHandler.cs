// -----------------------------------------------------------------------
//  <copyright file="RevisionsHandler.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System.Threading.Tasks;
using Raven.Server.Documents.Handlers.Processors.Expiration;
using Raven.Server.Routing;

namespace Raven.Server.Documents.Handlers
{
    public class ExpirationHandler : DatabaseRequestHandler
    {
        [RavenAction("/databases/*/expiration/config", "GET", AuthorizationStatus.ValidUser, EndpointType.Read)]
        public async Task GetExpirationConfig()
        {
            using (var processor = new ExpirationHandlerProcessorForGet(this))
                await processor.ExecuteAsync();
        }

        [RavenAction("/databases/*/admin/expiration/config", "POST", AuthorizationStatus.DatabaseAdmin)]
        public async Task ConfigExpiration()
        {
            using (var processor = new ExpirationHandlerProcessorForPost(this))
                await processor.ExecuteAsync();
        }
    }
}

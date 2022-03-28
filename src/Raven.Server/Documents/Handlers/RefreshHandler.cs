// -----------------------------------------------------------------------
//  <copyright file="RevisionsHandler.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System.Threading.Tasks;
using Raven.Server.Documents.Handlers.Processors.Refresh;
using Raven.Server.Routing;

namespace Raven.Server.Documents.Handlers
{
    public class RefreshHandler : DatabaseRequestHandler
    {
        [RavenAction("/databases/*/refresh/config", "GET", AuthorizationStatus.ValidUser, EndpointType.Read)]
        public async Task GetRefreshConfiguration()
        {
            using (var processor = new RefreshHandlerProcessorForGetRefreshConfiguration(this))
                await processor.ExecuteAsync();
        }

        [RavenAction("/databases/*/admin/refresh/config", "POST", AuthorizationStatus.DatabaseAdmin)]
        public async Task PostRefreshConfiguration()
        {
            using (var processor = new RefreshHandlerProcessorForPostRefreshConfiguration(this))
                await processor.ExecuteAsync();
        }
    }
}

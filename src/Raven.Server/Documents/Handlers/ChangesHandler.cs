// ------------------------------------------------------------[-----------
//  <copyright file="ChangesHandler.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System.Threading.Tasks;
using Raven.Server.Documents.Handlers.Processors.Changes;
using Raven.Server.Routing;

namespace Raven.Server.Documents.Handlers
{
    public class ChangesHandler : DatabaseRequestHandler
    {
        [RavenAction("/databases/*/changes", "GET", AuthorizationStatus.ValidUser, EndpointType.Read, SkipUsagesCount = true, DisableOnCpuCreditsExhaustion = true)]
        public async Task GetChanges()
        {
            using (var processor = new ChangesHandlerProcessorForGetChanges(this))
                await processor.ExecuteAsync();
        }

        [RavenAction("/databases/*/changes/debug", "GET", AuthorizationStatus.ValidUser, EndpointType.Read)]
        public async Task GetConnectionsDebugInfo()
        {
            using (var processor = new ChangesHandlerProcessorForGetConnectionsDebugInfo(this))
                await processor.ExecuteAsync();
        }

        [RavenAction("/databases/*/changes", "DELETE", AuthorizationStatus.ValidUser, EndpointType.Write)]
        public async Task DeleteConnections()
        {
            using (var processor = new ChangesHandlerProcessorForDeleteConnections(this))
                await processor.ExecuteAsync();
        }
    }
}

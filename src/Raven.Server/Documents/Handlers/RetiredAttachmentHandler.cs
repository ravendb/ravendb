// -----------------------------------------------------------------------
//  <copyright file="DocumentHandler.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System.Threading.Tasks;
using Raven.Server.Documents.Handlers.Processors.Attachments.Retired;
using Raven.Server.Routing;

namespace Raven.Server.Documents.Handlers
{
    public sealed class RetiredAttachmentHandler : DatabaseRequestHandler
    {
        [RavenAction("/databases/*/attachments/retire/config", "GET", AuthorizationStatus.ValidUser, EndpointType.Read)]
        public async Task GetRetireConfig()
        {
            using (var processor = new RetiredAttachmentHandlerProcessorForGetRetireConfig(this))
                await processor.ExecuteAsync();
        }

        [RavenAction("/databases/*/admin/attachments/retire/config", "POST", AuthorizationStatus.DatabaseAdmin)]
        public async Task AddRetireConfig()
        {
            using (var processor = new RetiredAttachmentHandlerProcessorForAddRetireConfig(this))
                await processor.ExecuteAsync();
        }
    }
}

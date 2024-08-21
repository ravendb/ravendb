// -----------------------------------------------------------------------
//  <copyright file="DocumentHandler.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System.Threading.Tasks;
using Raven.Server.Documents.Handlers.Processors.Attachments;
using Raven.Server.Documents.Handlers.Processors.Attachments.Retired;
using Raven.Server.Routing;

namespace Raven.Server.Documents.Handlers
{
    public sealed class RetiredAttachmentHandler : DatabaseRequestHandler
    {
        [RavenAction("/databases/*/attachments/retire", "HEAD", AuthorizationStatus.ValidUser, EndpointType.Read)]
        public async Task Head()
        {
            using (var processor = new AttachmentHandlerProcessorForHeadAttachment(this))
                await processor.ExecuteAsync();
        }

        [RavenAction("/databases/*/attachments/retire", "GET", AuthorizationStatus.ValidUser, EndpointType.Read)]
        public async Task Get()
        {
            using (var processor = new RetiredAttachmentHandlerProcessorForGetRetiredAttachment(this, isDocument: true))
                await processor.ExecuteAsync();
        }

        [RavenAction("/databases/*/attachments/retire/bulk", "POST", AuthorizationStatus.ValidUser, EndpointType.Read)]
        public async Task GetAttachments()
        {
            using (var processor = new RetiredAttachmentHandlerProcessorForBulkRetiredAttachment(this))
                await processor.ExecuteAsync();
        }

        [RavenAction("/databases/*/debug/attachments/retire/hash", "GET", AuthorizationStatus.ValidUser, EndpointType.Read, DisableOnCpuCreditsExhaustion = true)]
        public async Task GetHashCount()
        {
            using (var processor = new AttachmentHandlerProcessorForGetHashCount(this))
                await processor.ExecuteAsync();
        }

        [RavenAction("/databases/*/debug/attachments/retire/metadata", "GET", AuthorizationStatus.ValidUser, EndpointType.Read, DisableOnCpuCreditsExhaustion = true)]
        public async Task GetAttachmentMetadataWithCounts()
        {
            using (var processor = new AttachmentHandlerProcessorForGetAttachmentMetadataWithCounts(this))
                await processor.ExecuteAsync();
        }

        [RavenAction("/databases/*/attachments/retire", "DELETE", AuthorizationStatus.ValidUser, EndpointType.Write, DisableOnCpuCreditsExhaustion = true)]
        public async Task Delete()
        {
            using (var processor = new RetiredAttachmentHandlerProcessorForDeleteRetiredAttachment(this))
            {
                await processor.ExecuteAsync();
            }
        }

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

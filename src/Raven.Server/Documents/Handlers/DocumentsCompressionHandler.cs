// -----------------------------------------------------------------------
//  <copyright file="DocumentsCompressionHandler.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System.Net;
using System.Threading.Tasks;
using Raven.Server.Documents.Handlers.Processors.DocumentsCompression;
using Raven.Server.Routing;

namespace Raven.Server.Documents.Handlers
{
    public class DocumentsCompressionHandler : DatabaseRequestHandler
    {
        [RavenAction("/databases/*/documents-compression/config", "GET", AuthorizationStatus.ValidUser, EndpointType.Read)]
        public async Task GetDocumentsCompressionConfig()
        {
            using (var processor = new DocumentsCompressionHandlerProcessorForGet(this))
                await processor.ExecuteAsync();
        }

        [RavenAction("/databases/*/admin/documents-compression/config", "POST", AuthorizationStatus.DatabaseAdmin)]
        public async Task ConfigDocumentsCompression()
        {
            using (var processor = new DocumentsCompressionHandlerProcessorForPost(this))
                await processor.ExecuteAsync();
        }
    }
}

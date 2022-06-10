// -----------------------------------------------------------------------
//  <copyright file="MultiGetHandler.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System.Threading.Tasks;
using Raven.Server.Documents.Handlers.Processors.MultiGet;
using Raven.Server.Routing;

namespace Raven.Server.Documents.Handlers
{
    public class MultiGetHandler : DatabaseRequestHandler
    {
        [RavenAction("/databases/*/multi_get", "POST", AuthorizationStatus.ValidUser, EndpointType.Read)]
        public async Task Post()
        {
            using (var processor = new MultiGetHandlerProcessorForPost(this))
                await processor.ExecuteAsync();
        }
    }
}

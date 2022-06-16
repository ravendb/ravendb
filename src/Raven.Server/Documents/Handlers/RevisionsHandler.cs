// -----------------------------------------------------------------------
//  <copyright file="RevisionsHandler.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Threading.Tasks;
using Raven.Client.Documents.Session.Operations;
using Raven.Server.Documents.Handlers.Processors.Revisions;
using Raven.Server.Documents.Operations;
using Raven.Server.Documents.Revisions;
using Raven.Server.Json;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers
{
    public class RevisionsHandler : DatabaseRequestHandler
    {
        [RavenAction("/databases/*/revisions/config", "GET", AuthorizationStatus.ValidUser, EndpointType.Read)]
        public async Task GetRevisionsConfiguration()
        {
            using (var processor = new RevisionsHandlerProcessorForGetRevisionsConfiguration(this))
                await processor.ExecuteAsync();
        }

        [RavenAction("/databases/*/revisions/conflicts/config", "GET", AuthorizationStatus.ValidUser, EndpointType.Read)]
        public async Task GetConflictRevisionsConfig()
        {
            using (var processor = new RevisionsHandlerProcessorForGetRevisionsConflictsConfiguration(this))
                await processor.ExecuteAsync();
        }

        [RavenAction("/databases/*/revisions/count", "GET", AuthorizationStatus.ValidUser, EndpointType.Read)]
        public async Task GetRevisionsCountFor()
        {
            using (var processor = new RevisionsHandlerProcessorForGetRevisionsCount(this))
            {
                await processor.ExecuteAsync();
            }
        }

        [RavenAction("/databases/*/revisions", "GET", AuthorizationStatus.ValidUser, EndpointType.Read)]
        public async Task GetRevisionsFor()
        {
            using (var processor = new RevisionsHandlerProcessorForGetRevisions(this))
            {
                await processor.ExecuteAsync();
            }
        }

        [RavenAction("/databases/*/revisions/revert", "POST", AuthorizationStatus.ValidUser, EndpointType.Write, DisableOnCpuCreditsExhaustion = true)]
        public async Task Revert()
        {
            using (var processor = new RevisionsHandlerProcessorForRevertRevisions(this))
                await processor.ExecuteAsync();
        }

        [RavenAction("/databases/*/revisions/resolved", "GET", AuthorizationStatus.ValidUser, EndpointType.Read)]
        public async Task GetResolvedConflictsSince()
        {
            using (var processor = new RevisionsHandlerProcessorForGetResolvedRevisions(this))
                await processor.ExecuteAsync();
        }

        [RavenAction("/databases/*/revisions/bin", "GET", AuthorizationStatus.ValidUser, EndpointType.Read)]
        public async Task GetRevisionsBin()
        {
            using (var processor = new RevisionsHandlerProcessorForGetRevisionsBin(this))
                await processor.ExecuteAsync();
        }
    }
}

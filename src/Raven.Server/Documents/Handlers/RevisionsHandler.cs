// -----------------------------------------------------------------------
//  <copyright file="RevisionsHandler.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Threading.Tasks;
using Raven.Client.Documents.Commands;
using Raven.Server.Documents.Handlers.Processors.Revisions;
using Raven.Server.Json;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Utils;

namespace Raven.Server.Documents.Handlers
{
    public sealed class RevisionsHandler : DatabaseRequestHandler
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
                await processor.ExecuteAsync();
        }

        [RavenAction("/databases/*/revisions", "GET", AuthorizationStatus.ValidUser, EndpointType.Read)]
        public async Task GetRevisionsFor()
        {
            using (var processor = new RevisionsHandlerProcessorForGetRevisions(this))
                await processor.ExecuteAsync();
        }

        [RavenAction("/databases/*/revisions/size", "GET", AuthorizationStatus.ValidUser, EndpointType.Read)]
        public async Task GetRevisionsSize()
        {
            RevisionSizeDetails size;
            var changeVector = GetQueryStringValueAndAssertIfSingleAndNotEmpty("changeVector");

            using (ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            using (context.OpenReadTransaction())
            {
                var metrics = Database.DocumentsStorage.RevisionsStorage.GetRevisionMetrics(context, changeVector);
                if (metrics.HasValue == false)
                {
                    HttpContext.Response.StatusCode = (int)HttpStatusCode.NotFound;
                    return;
                }

                size = new RevisionSizeDetails(changeVector, metrics.Value);
            }
            

            using (ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext ctx))
            await using (var writer = new AsyncBlittableJsonTextWriter(ctx, ResponseBodyStream()))
            {
                ctx.Write(writer, size.ToJson());
            }
        }


        [RavenAction("/databases/*/revisions/revert", "POST", AuthorizationStatus.ValidUser, EndpointType.Write, DisableOnCpuCreditsExhaustion = true)]
        public async Task Revert()
        {
            using (var processor = new RevisionsHandlerProcessorForRevertRevisions(this))
                await processor.ExecuteAsync();
        }

        [RavenAction("/databases/*/revisions/revert/docs", "POST", AuthorizationStatus.ValidUser, EndpointType.Write)]
        public async Task RevertDocument()
        {
            using (var processor = new RevisionsHandlerProcessorForRevertRevisionsForDocument(this))
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

    internal sealed class RevisionSizeDetails : SizeDetails
    {
        public RevisionSizeDetails(){ }

        public RevisionSizeDetails(string changeVector, (int ActualSize, int AllocatedSize, bool IsCompressed) metrics)
        {
            ChangeVector = changeVector;
            ActualSize = metrics.ActualSize;
            HumaneActualSize = Sizes.Humane(metrics.ActualSize);
            AllocatedSize = metrics.AllocatedSize;
            HumaneAllocatedSize = Sizes.Humane(metrics.AllocatedSize);
            IsCompressed = metrics.IsCompressed;
        }

        public string ChangeVector { get; set; }

        public override DynamicJsonValue ToJson()
        {
            var json = base.ToJson();
            json[nameof(ChangeVector)] = ChangeVector;
            return json;
        }
    }
}

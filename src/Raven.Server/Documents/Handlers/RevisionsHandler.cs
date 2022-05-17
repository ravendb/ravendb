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
            RevertRevisionsRequest configuration;

            using (ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext context))
            {
                var json = await context.ReadForMemoryAsync(RequestBodyStream(), "revisions/revert");

                configuration = JsonDeserializationServer.RevertRevisions(json);
            }

            var token = CreateTimeLimitedOperationToken();
            var operationId = ServerStore.Operations.GetNextOperationId();

            var t = Database.Operations.AddLocalOperation(
                operationId,
                OperationType.DatabaseRevert,
                $"Revert database '{Database.Name}' to {configuration.Time} UTC.",
                detailedDescription: null,
                onProgress => Database.DocumentsStorage.RevisionsStorage.RevertRevisions(configuration.Time, TimeSpan.FromSeconds(configuration.WindowInSec), onProgress, token),
                token: token);

            using (ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext context))
            await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
            {
                writer.WriteOperationIdAndNodeTag(context, operationId, ServerStore.NodeTag);
            }
        }

        private async Task GetRevisionsCount(DocumentsOperationContext documentContext)
        {
            var docId = GetQueryStringValueAndAssertIfSingleAndNotEmpty("id");

            var documentRevisionsDetails = new GetRevisionsCountOperation.DocumentRevisionsCount()
            {
                RevisionsCount = 0
            };

            documentRevisionsDetails.RevisionsCount = Database.DocumentsStorage.RevisionsStorage.GetRevisionsCount(documentContext, docId);

            await using (var writer = new AsyncBlittableJsonTextWriter(documentContext, ResponseBodyStream()))
            {
                documentContext.Write(writer, documentRevisionsDetails.ToJson());
            }
        }

        [RavenAction("/databases/*/revisions/resolved", "GET", AuthorizationStatus.ValidUser, EndpointType.Read)]
        public async Task GetResolvedConflictsSince()
        {
            var since = GetStringQueryString("since", required: false);
            var take = GetIntValueQueryString("take", required: false) ?? 1024;
            var date = Convert.ToDateTime(since).ToUniversalTime();
            using (ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            using (context.OpenReadTransaction())
            using (var token = CreateOperationToken())
            await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
            {
                writer.WriteStartObject();
                writer.WritePropertyName("Results");
                var revisions = Database.DocumentsStorage.RevisionsStorage.GetResolvedDocumentsSince(context, date, take);
                await writer.WriteDocumentsAsync(context, revisions, metadataOnly: false, token.Token);
                writer.WriteEndObject();
            }
        }

        [RavenAction("/databases/*/revisions/bin", "GET", AuthorizationStatus.ValidUser, EndpointType.Read)]
        public async Task GetRevisionsBin()
        {
            using (var processor = new RevisionsHandlerProcessorForGetRevisionsBin(this))
                await processor.ExecuteAsync();
        }
    }
}

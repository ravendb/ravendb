// -----------------------------------------------------------------------
//  <copyright file="RevisionsHandler.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System.Collections.Generic;
using System.Linq;
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
            List<RevisionSizeDetails> sizes;
            List<string> changeVectors;

            using (ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            {
                var json = await context.ReadForMemoryAsync(RequestBodyStream(), "ChangeVectors");
                var parameters = JsonDeserializationServer.Parameters.GetRevisionsSizeParameters(json);

                using (context.OpenReadTransaction())
                using (var token = CreateHttpRequestBoundOperationToken())
                {
                    changeVectors = parameters.ChangeVectors;
                    sizes = GetRevisionsSizeByChangeVector(context, changeVectors);
                }
            }

            using (ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext ctx))
            await using (var writer = new AsyncBlittableJsonTextWriter(ctx, ResponseBodyStream()))
            {
                writer.WriteStartObject();

                writer.WriteArray("Sizes", sizes.Select(size => size.ToJson()), ctx);

                writer.WriteEndObject();
            }
        }

        public class GetRevisionsSizeParameters
        {
            public List<string> ChangeVectors;
        }

        private List<RevisionSizeDetails> GetRevisionsSizeByChangeVector(DocumentsOperationContext context, List<string> changeVectors)
        {
            var revisionsStorage = Database.DocumentsStorage.RevisionsStorage;

            var sizes = new List<RevisionSizeDetails>(changeVectors.Count);

            foreach (var cv in changeVectors)
            {
                var metrics = revisionsStorage.GetRevisionMetrics(context, cv);

                var exist = metrics != null;
                if (exist == false)
                    metrics = (0, 0, false);

                sizes.Add(new RevisionSizeDetails
                {
                    ChangeVector = cv,
                    Exist = exist,
                    ActualSize = metrics.Value.ActualSize,
                    HumaneActualSize = Sizes.Humane(metrics.Value.ActualSize),
                    AllocatedSize = metrics.Value.AllocatedSize,
                    HumaneAllocatedSize = Sizes.Humane(metrics.Value.AllocatedSize),
                    IsCompressed = metrics.Value.IsCompressed
                });
            }

            return sizes;
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
        public string ChangeVector { get; set; }

        public bool Exist { get; set; }

        public override DynamicJsonValue ToJson()
        {
            var json = base.ToJson();
            json[nameof(ChangeVector)] = ChangeVector;
            json[nameof(Exist)] = Exist;
            return json;
        }
    }
}

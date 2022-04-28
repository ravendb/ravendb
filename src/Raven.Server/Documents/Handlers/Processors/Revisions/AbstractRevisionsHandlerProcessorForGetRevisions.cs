using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Json;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.Processors.Revisions
{
    internal abstract class AbstractRevisionsHandlerProcessorForGetRevisions<TRequestHandler, TOperationContext> : AbstractDatabaseHandlerProcessor<TRequestHandler, TOperationContext>
        where TOperationContext : JsonOperationContext 
        where TRequestHandler : AbstractDatabaseRequestHandler<TOperationContext>
    {
        protected AbstractRevisionsHandlerProcessorForGetRevisions([NotNull] TRequestHandler requestHandler) : base(requestHandler)
        {
        }

        protected abstract bool NotModified(string actualEtag);

        protected abstract ValueTask GetRevisionByChangeVectorAsync(TOperationContext context, Microsoft.Extensions.Primitives.StringValues changeVectors, bool metadataOnly, CancellationToken token);
        
        protected abstract ValueTask GetRevisionsAsync(TOperationContext context, string id, DateTime? before, int start, int pageSize, bool metadataOnly, CancellationToken token);

        public override async ValueTask ExecuteAsync()
        {
            using(ContextPool.AllocateOperationContext(out TOperationContext context))
            using (var token = RequestHandler.CreateOperationToken())
            {
                var changeVectors = RequestHandler.GetStringValuesQueryString("changeVector", required: false);
                var metadataOnly = RequestHandler.GetBoolValueQueryString("metadataOnly", required: false) ?? false;
                
                if (changeVectors.Count > 0)
                {
                    await GetRevisionByChangeVectorAsync(context, changeVectors, metadataOnly, token.Token);
                }
                else
                {
                    var id = RequestHandler.GetQueryStringValueAndAssertIfSingleAndNotEmpty("id");
                    var before = RequestHandler.GetDateTimeQueryString("before", required: false);
                    var start = RequestHandler.GetStart();
                    var pageSize = RequestHandler.GetPageSize();

                    //get revisions by id is sent to a single shard on sharded
                    await GetRevisionsAsync(context, id, before, start, pageSize, metadataOnly, token.Token);
                }
            }
        }

        protected void WriteRevisionsBlittable(JsonOperationContext context, IEnumerable<BlittableJsonReaderObject> documentsToWrite, out long numberOfResults, out long totalDocumentsSizeInBytes)
        {
            numberOfResults = 0;
            totalDocumentsSizeInBytes = 0;
            HttpContext.Response.Headers["Content-Type"] = "binary/blittable-json";

            using (var streamBuffer = new UnmanagedStreamBuffer(context, RequestHandler.ResponseBodyStream()))
            using (var writer = new ManualBlittableJsonDocumentBuilder<UnmanagedStreamBuffer>(context,
                       null, new BlittableWriter<UnmanagedStreamBuffer>(context, streamBuffer)))
            {
                writer.StartWriteObjectDocument();

                writer.StartWriteObject();
                writer.WritePropertyName(nameof(BlittableArrayResult.Results));

                writer.StartWriteArray();
                foreach (BlittableJsonReaderObject document in documentsToWrite)
                {
                    numberOfResults++;
                    writer.WriteEmbeddedBlittableDocument(document);
                    totalDocumentsSizeInBytes += document.Size;
                }
                writer.WriteArrayEnd();

                writer.WriteObjectEnd();

                writer.FinalizeDocument();
            }
        }
    }
}

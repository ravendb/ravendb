using System;
using System.Net;
using System.Net.Http;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Exceptions.Documents.Indexes;
using Raven.Server.Documents.Indexes;
using Raven.Server.Documents.Queries;
using Raven.Server.NotificationCenter;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.Processors.Streaming
{
    internal sealed class StreamingHandlerProcessorForGetStreamQuery : AbstractStreamingHandlerProcessorForGetStreamQuery<DatabaseRequestHandler, DocumentsOperationContext>
    {
        private QueryOperationContext _queryContext;

        public StreamingHandlerProcessorForGetStreamQuery([NotNull] DatabaseRequestHandler requestHandler, HttpMethod method) : base(requestHandler, method)
        {
        }

        protected override RequestTimeTracker GetTimeTracker()
        {
            return new RequestTimeTracker(HttpContext, Logger, RequestHandler.Database.NotificationCenter, RequestHandler.Database.Configuration, "StreamQuery",
                doPerformanceHintIfTooLong: false);
        }

        protected override ValueTask<BlittableJsonReaderObject> GetDocumentDataAsync(DocumentsOperationContext context, string fromDocument)
        {
            using (context.OpenReadTransaction())
            {
                var document = RequestHandler.Database.DocumentsStorage.Get(context, fromDocument);
                return ValueTask.FromResult(document?.Data.Clone(context));
            }
        }

        protected override IDisposable AllocateContext(out DocumentsOperationContext context)
        {
            var queryContext = QueryOperationContext.Allocate(RequestHandler.Database);
            context = queryContext.Documents;
            _queryContext = queryContext;
            return queryContext;
        }

        protected override QueryMetadataCache GetQueryMetadataCache()
        {
            return RequestHandler.Database.QueryMetadataCache;
        }

        protected override async ValueTask ExecuteAndWriteIndexQueryStreamEntriesAsync(DocumentsOperationContext context, IndexQueryServerSide query, string format,
            string[] propertiesArray, string fileNamePrefix, bool ignoreLimit, OperationCancelToken token)
        {
            await using (var writer = GetResultWriter<BlittableJsonReaderObject>(format, context, HttpContext.Response, RequestHandler.ResponseBodyStream(), propertiesArray, fileNamePrefix))
            {
                try
                {
                    await RequestHandler.Database.QueryRunner.ExecuteStreamIndexEntriesQuery(query, _queryContext, HttpContext.Response, writer, ignoreLimit, token)
                        .ConfigureAwait(false);
                }
                catch (IndexDoesNotExistException)
                {
                    HttpContext.Response.StatusCode = (int)HttpStatusCode.NotFound;
                    await writer.WriteErrorAsync($"Index {query.Metadata.IndexName} does not exist");
                }
            }
        }

        protected override async ValueTask ExecuteAndWriteQueryStreamAsync(DocumentsOperationContext context, IndexQueryServerSide query, string format,
            string[] propertiesArray, string fileNamePrefix, OperationCancelToken token)
        {
            await using (var writer = GetResultWriter<Document>(format, context, HttpContext.Response, RequestHandler.ResponseBodyStream(), propertiesArray,
                             fileNamePrefix))
            {
                try
                {
                    await RequestHandler.Database.QueryRunner.ExecuteStreamQuery(query, _queryContext, HttpContext.Response, writer, token).ConfigureAwait(false);
                }
                catch (IndexDoesNotExistException)
                {
                    HttpContext.Response.StatusCode = (int)HttpStatusCode.NotFound;
                    await writer.WriteErrorAsync($"Index {query.Metadata.IndexName} does not exist");
                }
                catch (Exception e)
                {
                    if (_method == HttpMethod.Get)
                    {
                        try
                        {
                            await writer.WriteErrorAsync($"Failed to execute stream query. Error: {e}");
                        }
                        catch (Exception ie)
                        {
                            if (Logger.IsOperationsEnabled)
                            {
                                Logger.Operations($"Failed to write error. Error: {e}", ie);
                            }
                        }
                    }

                    throw;
                }
            }
        }
    }
}

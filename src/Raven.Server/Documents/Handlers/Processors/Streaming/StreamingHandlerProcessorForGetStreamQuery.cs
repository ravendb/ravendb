using System;
using System.IO;
using System.Net;
using System.Net.Http;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Microsoft.AspNetCore.Http;
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

        protected override async ValueTask ExecuteAndWriteIndexQueryStreamEntriesAsync(DocumentsOperationContext context, IndexQueryServerSide query, string format, string _,
            string[] propertiesArray, string fileNamePrefix, bool ignoreLimit, bool fromSharded, OperationCancelToken token)
        {
            //writes either csv or blittable documents if is shard
            await using (var writer = GetBlittableQueryResultWriter(format, isDebug: true, context, HttpContext.Response, RequestHandler.ResponseBodyStream(),
                             fromSharded, propertiesArray, fileNamePrefix))
            {
                try
                {
                    await RequestHandler.Database.QueryRunner.ExecuteStreamIndexEntriesQuery(query, _queryContext, HttpContext.Response, writer, ignoreLimit, token)
                        .ConfigureAwait(false);
                }
                catch (IndexDoesNotExistException)
                {
                    HttpContext.Response.StatusCode = (int)HttpStatusCode.NotFound;

                    if (fromSharded)
                        throw;

                    await writer.WriteErrorAsync($"Index {query.Metadata.IndexName} does not exist");
                }
            }
        }

        protected override async ValueTask ExecuteAndWriteQueryStreamAsync(DocumentsOperationContext context, IndexQueryServerSide query, string format,
            string[] propertiesArray, string fileNamePrefix, bool _, bool fromSharded, OperationCancelToken token)
        {
            await using (var writer = GetDocumentQueryResultWriter(format, HttpContext.Response, context, RequestHandler.ResponseBodyStream(), propertiesArray,
                             fileNamePrefix))
            {
                try
                {
                    await RequestHandler.Database.QueryRunner.ExecuteStreamQuery(query, _queryContext, HttpContext.Response, writer, token).ConfigureAwait(false);
                }
                catch (IndexDoesNotExistException)
                {
                    HttpContext.Response.StatusCode = (int)HttpStatusCode.NotFound;

                    if (fromSharded)
                        throw;

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

        private IStreamQueryResultWriter<Document> GetDocumentQueryResultWriter(string format, HttpResponse response, JsonOperationContext context, Stream responseBodyStream,
            string[] propertiesArray, string fileNamePrefix = null)
        {
            var queryFormat = GetQueryResultFormat(format);
            switch (queryFormat)
            {
                case QueryResultFormat.Json:
                    return new StreamJsonFileDocumentQueryResultWriter(response, responseBodyStream, context, propertiesArray, fileNamePrefix);
                case QueryResultFormat.Csv:
                    return new StreamCsvDocumentQueryResultWriter(response, responseBodyStream, propertiesArray, fileNamePrefix);
            }

            if (propertiesArray != null)
            {
                ThrowUnsupportedException("Using json output format with custom fields is not supported.");
            }

            switch (queryFormat)
            {
                case QueryResultFormat.Jsonl:
                    return new StreamJsonlDocumentQueryResultWriter(responseBodyStream, context);
                default:
                    return new StreamJsonDocumentQueryResultWriter(responseBodyStream, context);
            }
        }

        protected override IStreamQueryResultWriter<BlittableJsonReaderObject> GetBlittableQueryResultWriter(string format, bool isDebug, JsonOperationContext context, HttpResponse response, Stream responseBodyStream, bool fromSharded,
            string[] propertiesArray, string fileNamePrefix = null)
        {
            if (fromSharded)
            {
                return new StreamBlittableDocumentQueryResultWriter(responseBodyStream, context);
            }

            var queryFormat = GetQueryResultFormat(format);
            if (queryFormat != QueryResultFormat.Csv)
                ThrowUnsupportedException($"You have selected \"{format}\" file format, which is not supported.");

            //does not write query stats to stream
            return new StreamCsvBlittableQueryResultWriter(response, responseBodyStream, propertiesArray, fileNamePrefix);
        }
    }
}

using System;
using System.Net;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Microsoft.Extensions.Primitives;
using Raven.Client.Documents.Changes;
using Raven.Client.Exceptions.Documents;
using Raven.Client.Exceptions.Documents.Indexes;
using Raven.Server.Documents.Indexes;
using Raven.Server.Documents.Queries;
using Raven.Server.NotificationCenter;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Raven.Server.TrafficWatch;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.Processors.Streaming
{
    internal class StreamingHandlerProcessorForGetStreamQuery : AbstractStreamingHandlerProcessorForGetStreamQuery<DatabaseRequestHandler, DocumentsOperationContext>
    {
        protected QueryOperationContext _queryContext;

        public StreamingHandlerProcessorForGetStreamQuery([NotNull] DatabaseRequestHandler requestHandler, HttpMethod method) : base(requestHandler, method)
        {
        }

        protected override RequestTimeTracker GetTimeTracker()
        {
            return new RequestTimeTracker(HttpContext, Logger, RequestHandler.Database, "StreamQuery", doPerformanceHintIfTooLong: false);
        }

        protected override ValueTask<BlittableJsonReaderObject> GetDocumentData(DocumentsOperationContext context, string fromDocument)
        {
            using (context.OpenReadTransaction())
            {
                var document = RequestHandler.Database.DocumentsStorage.Get(context, fromDocument);
                return ValueTask.FromResult(document?.Data);
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

        protected override async ValueTask ExecuteQueryAndWriteAsync(DocumentsOperationContext context, IndexQueryServerSide query, string format, string debug, bool ignoreLimit, StringValues properties, RequestTimeTracker tracker, OperationCancelToken token)
        {
            // ReSharper disable once ArgumentsStyleLiteral
            
            var propertiesArray = properties.Count == 0 ? null : properties.ToArray();
            // set the exported file name prefix
            var fileNamePrefix = query.Metadata.IsCollectionQuery ? query.Metadata.CollectionName + "_collection" : "query_result";
            fileNamePrefix = $"{RequestHandler.Database.Name}_{fileNamePrefix}";
            if (string.IsNullOrWhiteSpace(debug) == false)
            {
                if (string.Equals(debug, "entries", StringComparison.OrdinalIgnoreCase))
                {
                    await using (var writer = GetIndexEntriesQueryResultWriter(format, HttpContext.Response, RequestHandler.ResponseBodyStream(), propertiesArray, fileNamePrefix))
                    {
                        try
                        {
                            await RequestHandler.Database.QueryRunner.ExecuteStreamIndexEntriesQuery(query, _queryContext, HttpContext.Response, writer, ignoreLimit, token).ConfigureAwait(false);
                        }
                        catch (IndexDoesNotExistException)
                        {
                            HttpContext.Response.StatusCode = (int)HttpStatusCode.NotFound;
                            await writer.WriteErrorAsync($"Index {query.Metadata.IndexName} does not exist");
                        }
                    }
                }
                else
                {
                    ThrowUnsupportedException($"You have selected {debug} debug mode, which is not supported.");
                }
            }
            else
            {
                await using (var writer = GetQueryResultWriter(format, HttpContext.Response, _queryContext.Documents, RequestHandler.ResponseBodyStream(), propertiesArray, fileNamePrefix))
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
                    catch (Exception e) //TODO stav: this chunk is not in Post, only Get. Is there a reason
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

                        throw;
                    }
                }
            }
        }
    }
}

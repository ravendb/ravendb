using System;
using System.IO;
using System.Text;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Microsoft.AspNetCore.Http;
using Raven.Client;
using Raven.Client.Documents.Changes;
using Raven.Client.Exceptions.Documents;
using Raven.Server.Documents.Queries;
using Raven.Server.NotificationCenter;
using Raven.Server.ServerWide;
using Raven.Server.TrafficWatch;
using Sparrow.Json;
using HttpMethod = System.Net.Http.HttpMethod;

namespace Raven.Server.Documents.Handlers.Processors.Streaming
{
    internal abstract class AbstractStreamingHandlerProcessorForGetStreamQuery<TRequestHandler, TOperationContext> : AbstractDatabaseHandlerProcessor<TRequestHandler, TOperationContext>
        where TRequestHandler : AbstractDatabaseRequestHandler<TOperationContext>
        where TOperationContext : JsonOperationContext
    {
        protected readonly HttpMethod _method;

        protected AbstractStreamingHandlerProcessorForGetStreamQuery([NotNull] TRequestHandler requestHandler, HttpMethod method) : base(requestHandler)
        {
            _method = method;
        }
        
        protected abstract RequestTimeTracker GetTimeTracker();

        protected abstract ValueTask<BlittableJsonReaderObject> GetDocumentData(TOperationContext context, string fromDocument);

        protected abstract IDisposable AllocateContext(out TOperationContext context);

        protected abstract QueryMetadataCache GetQueryMetadataCache();

        protected abstract IStreamQueryResultWriter<BlittableJsonReaderObject> GetBlittableQueryResultWriter(string format, bool isDebug, JsonOperationContext context,
            HttpResponse response, Stream responseBodyStream, bool fromSharded,
            string[] propertiesArray, string fileNamePrefix = null);

        protected abstract ValueTask ExecuteAndWriteQueryStreamAsync(TOperationContext context, IndexQueryServerSide query, string format,
            string[] propertiesArray, string fileNamePrefix, bool ignoreLimit, bool fromSharded, OperationCancelToken token);

        protected abstract ValueTask ExecuteAndWriteIndexQueryStreamEntriesAsync(TOperationContext context, IndexQueryServerSide query, string format, string debug,
            string[] propertiesArray, string fileNamePrefix, bool ignoreLimit, bool fromSharded, OperationCancelToken token);

        public override async ValueTask ExecuteAsync()
        {
            var format = RequestHandler.GetStringQueryString("format", false);
            var debug = RequestHandler.GetStringQueryString("debug", false);
            var ignoreLimit = RequestHandler.GetBoolValueQueryString("ignoreLimit", required: false) ?? false;
            var properties = RequestHandler.GetStringValuesQueryString("field", false);

            // ReSharper disable once ArgumentsStyleLiteral
            using (var tracker = GetTimeTracker())
            using (var token = RequestHandler.CreateTimeLimitedQueryToken())
            using(AllocateContext(out TOperationContext context))
            {
                IndexQueryServerSide query;
                string overrideQuery = null;

                if (_method == HttpMethod.Get)
                {
                    var start = RequestHandler.GetStart();
                    var pageSize = RequestHandler.GetPageSize();

                    var fromDocument = RequestHandler.GetStringQueryString("fromDocument", false);
                    if (string.IsNullOrEmpty(fromDocument) == false)
                    {
                        var docData = await GetDocumentData(context, fromDocument);
                        if (docData == null)
                        {
                            throw new DocumentDoesNotExistException($"Was request to stream a query taken from {fromDocument} document, but it does not exist.");
                        }

                        if (docData.TryGet("Query", out overrideQuery) == false)
                        {
                            throw new MissingFieldException(
                                $"Expected {fromDocument} to have a property named 'Query' of type 'String' but couldn't locate such property.");
                        }
                    }
                    query = await IndexQueryServerSide.CreateAsync(HttpContext, start, pageSize, context, tracker, overrideQuery: overrideQuery);
                    query.IsStream = true;
                }
                else
                {
                    var stream = RequestHandler.TryGetRequestFromStream("ExportOptions") ?? RequestHandler.RequestBodyStream();//TODO stav: dispose stream?
                    var queryJson = await context.ReadForMemoryAsync(stream, "index/query");
                    query = IndexQueryServerSide.Create(HttpContext, queryJson, GetQueryMetadataCache(), tracker);
                    query.IsStream = true;

                    if (TrafficWatchManager.HasRegisteredClients)
                    {
                        var sb = new StringBuilder();
                        // append stringBuilder with the query
                        sb.Append(query.Query);
                        // if query got parameters append with parameters
                        if (query.QueryParameters != null && query.QueryParameters.Count > 0)
                            sb.AppendLine().Append(query.QueryParameters);
                        RequestHandler.AddStringToHttpContext(sb.ToString(), TrafficWatchChangeType.Streams);
                    }
                }

                var propertiesArray = properties.Count == 0 ? null : properties.ToArray();
                // set the exported file name prefix
                var fileNamePrefix = query.Metadata.IsCollectionQuery ? query.Metadata.CollectionName + "_collection" : "query_result";
                fileNamePrefix = $"{RequestHandler.DatabaseName}_{fileNamePrefix}";

                var fromSharded = RequestHandler.GetBoolFromHeaders(Constants.Headers.Sharded) ?? false;

                if (string.IsNullOrWhiteSpace(debug) == false)
                {
                    if (string.Equals(debug, "entries", StringComparison.OrdinalIgnoreCase))
                    {
                        await ExecuteAndWriteIndexQueryStreamEntriesAsync(context, query, format, debug, propertiesArray, fileNamePrefix, ignoreLimit, fromSharded, token);
                    }
                    else
                    {
                        ThrowUnsupportedException($"You have selected {debug} debug mode, which is not supported.");
                    }
                }
                else
                {
                    await ExecuteAndWriteQueryStreamAsync(context, query, format, propertiesArray, fileNamePrefix, ignoreLimit, fromSharded, token);
                }
            }
        }

        protected static bool IsCsvFormat(string format)
        {
            return string.IsNullOrEmpty(format) == false && string.Equals(format, "csv", StringComparison.OrdinalIgnoreCase);
        }

        protected void ThrowUnsupportedException(string message)
        {
            throw new NotSupportedException(message);
        }
    }
}

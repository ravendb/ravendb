using System;
using System.Diagnostics.CodeAnalysis;
using System.IO;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http;
using Raven.Client;
using Raven.Client.Exceptions.Documents;
using Raven.Server.Documents.Queries;
using Raven.Server.Extensions;
using Raven.Server.NotificationCenter;
using Raven.Server.ServerWide;
using Raven.Server.TrafficWatch;
using Sparrow.Json;
using Sparrow.Logging;
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
            if (method != HttpMethod.Post && method != HttpMethod.Get)
                throw new ArgumentException($"Expected method 'POST' or 'GET' but got '{method.Method}'");

            _method = method;
        }

        protected abstract RequestTimeTracker GetTimeTracker();

        protected abstract ValueTask<BlittableJsonReaderObject> GetDocumentDataAsync(TOperationContext context, string fromDocument);

        protected abstract IDisposable AllocateContext(out TOperationContext context);

        protected abstract QueryMetadataCache GetQueryMetadataCache();
       
        protected abstract ValueTask ExecuteAndWriteQueryStreamAsync(TOperationContext context, IndexQueryServerSide query, string format,
            string[] propertiesArray, string fileNamePrefix, OperationCancelToken token);

        protected abstract ValueTask ExecuteAndWriteIndexQueryStreamEntriesAsync(TOperationContext context, IndexQueryServerSide query, string format,
            string[] propertiesArray, string fileNamePrefix, bool ignoreLimit, OperationCancelToken token);

        protected IStreamQueryResultWriter<TSource> GetResultWriter<TSource>(string format, JsonOperationContext context, HttpResponse response,  Stream responseBodyStream, string[] propertiesArray,
            string fileNamePrefix = null)
        {
            var t = typeof(TSource);
            if (t == typeof(BlittableJsonReaderObject))
                return (IStreamQueryResultWriter<TSource>)GetBlittableQueryResultWriter(format, response, context, responseBodyStream, propertiesArray, fileNamePrefix);
            if (t == typeof(Document))
                return (IStreamQueryResultWriter<TSource>)GetDocumentQueryResultWriter(format, response, context, responseBodyStream, propertiesArray, fileNamePrefix);

            throw new ArgumentException($"Unknown writer type {t.FullName}");
        }

         private IStreamQueryResultWriter<BlittableJsonReaderObject> GetBlittableQueryResultWriter(string format, HttpResponse response, JsonOperationContext context, Stream responseBodyStream,
            string[] propertiesArray, string fileNamePrefix = null)
        {
            var queryFormat = GetQueryResultFormat(format);
            switch (queryFormat)
            {
                case QueryResultFormat.Default:
                    return new StreamBlittableDocumentQueryResultWriter(responseBodyStream, context);
                case QueryResultFormat.Json:
                    //does not write query stats to stream
                    return new StreamJsonFileBlittableQueryResultWriter(response, responseBodyStream, context, propertiesArray, fileNamePrefix);
                case QueryResultFormat.Csv:
                    //does not write query stats to stream
                    return new StreamCsvBlittableQueryResultWriter(response, responseBodyStream, propertiesArray, fileNamePrefix);
                case QueryResultFormat.Jsonl:
                    return new StreamJsonlBlittableQueryResultWriter(responseBodyStream, context);
                default:
                    ThrowUnsupportedException($"You have selected \"{format}\" file format, for 'GetBlittableQueryResultWriter' which is not supported.");
                    return null;
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
                ThrowUnsupportedException($"Using '{queryFormat}' output format with custom fields is not supported.");
            }

            switch (queryFormat)
            {
                case QueryResultFormat.Default:
                    return new StreamJsonDocumentQueryResultWriter(responseBodyStream, context);
                case QueryResultFormat.Jsonl:
                    return new StreamJsonlDocumentQueryResultWriter(responseBodyStream, context);
                default:
                    ThrowUnsupportedException($"You have selected \"{format}\" file format for 'GetDocumentQueryResultWriter', which is not supported.");
                    return null;
            }
        }

        public override async ValueTask ExecuteAsync()
        {
            var format = RequestHandler.GetStringQueryString("format", false);
            var debug = RequestHandler.GetStringQueryString("debug", false);
            var ignoreLimit = RequestHandler.GetBoolValueQueryString("ignoreLimit", required: false) ?? false;
            var properties = RequestHandler.GetStringValuesQueryString("field", false);

            // ReSharper disable once ArgumentsStyleLiteral
            using (var tracker = GetTimeTracker())
            using (var token = RequestHandler.CreateHttpRequestBoundTimeLimitedOperationTokenForQuery())
            using (AllocateContext(out TOperationContext context))
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
                        var docData = await GetDocumentDataAsync(context, fromDocument).ConfigureAwait(false);
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
                }
                else
                {
                    await using var stream = RequestHandler.TryGetRequestFromStream("ExportOptions") ?? RequestHandler.RequestBodyStream();
                    var queryJson = await context.ReadForMemoryAsync(stream, "index/query")
                                                 .ConfigureAwait(false);
                    query = IndexQueryServerSide.Create(HttpContext, queryJson, GetQueryMetadataCache(), tracker);
                }
                query.IsStream = true;

                if (RequestHandler.HttpContext.Request.IsFromOrchestrator())
                    query.ReturnOptions = IndexQueryServerSide.QueryResultReturnOptions.CreateForSharding(query);

                if (TrafficWatchManager.HasRegisteredClients)
                    RequestHandler.TrafficWatchStreamQuery(query);

                var propertiesArray = properties.Count == 0 ? null : properties.ToArray();

                if (LoggingSource.AuditLog.IsInfoEnabled && query.Metadata.CollectionName == Constants.Documents.Collections.AllDocumentsCollection)
                    RequestHandler.LogAuditFor(RequestHandler.DatabaseName, "QUERY", $"Streaming all documents (query: {query}, format: {format}, debug: {debug}, ignore limit: {ignoreLimit})");

                // set the exported file name prefix
                var fileNamePrefix = query.Metadata.IsCollectionQuery ? query.Metadata.CollectionName + "_collection" : "query_result";
                fileNamePrefix = $"{RequestHandler.DatabaseName}_{ServerStore.NodeTag}_{fileNamePrefix}";

                if (string.IsNullOrWhiteSpace(debug) == false)
                {
                    if (string.Equals(debug, "entries", StringComparison.OrdinalIgnoreCase))
                    {
                        await ExecuteAndWriteIndexQueryStreamEntriesAsync(context, query, format, propertiesArray, fileNamePrefix, ignoreLimit, token)
                                    .ConfigureAwait(false);
                    }
                    else
                    {
                        ThrowUnsupportedException($"You have selected {debug} debug mode, which is not supported.");
                    }
                }
                else
                {
                    await ExecuteAndWriteQueryStreamAsync(context, query, format, propertiesArray, fileNamePrefix, token)
                                .ConfigureAwait(false);
                }
            }
        }
        protected static QueryResultFormat GetQueryResultFormat(string format)
        {
            return Enum.TryParse<QueryResultFormat>(format, ignoreCase: true, out var queryFormat)
                ? queryFormat
                : QueryResultFormat.Default;
        }

        [DoesNotReturn]
        protected static void ThrowUnsupportedException(string message)
        {
            throw new NotSupportedException(message);
        }

        protected enum QueryResultFormat
        {
            Default,
            Json,
            Jsonl,
            Csv
        }
    }
}

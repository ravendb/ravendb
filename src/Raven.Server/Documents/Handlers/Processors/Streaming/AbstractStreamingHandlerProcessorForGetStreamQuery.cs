using System;
using System.Text;
using System.Threading.Tasks;
using Elasticsearch.Net;
using JetBrains.Annotations;
using Microsoft.Extensions.Primitives;
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
    internal abstract class AbstractStreamingHandlerProcessorForGetStreamQuery<TRequestHandler, TOperationContext> : AbstractStreamingHandlerProcessor<TRequestHandler, TOperationContext>
        where TRequestHandler : AbstractDatabaseRequestHandler<TOperationContext>
        where TOperationContext : JsonOperationContext
    {
        protected readonly HttpMethod _method;

        protected AbstractStreamingHandlerProcessorForGetStreamQuery([NotNull] TRequestHandler requestHandler, HttpMethod method) : base(requestHandler)
        {
            _method = method;
        }
        
        protected abstract ValueTask ExecuteQueryAndWriteAsync(TOperationContext context, IndexQueryServerSide indexQuery, string format, string debug, bool ignoreLimit, StringValues properties, RequestTimeTracker tracker, OperationCancelToken token);

        protected abstract RequestTimeTracker GetTimeTracker();

        protected abstract ValueTask<BlittableJsonReaderObject> GetDocumentData(TOperationContext context, string fromDocument);

        protected abstract IDisposable AllocateContext(out TOperationContext context);

        protected abstract QueryMetadataCache GetQueryMetadataCache();

        public override async ValueTask ExecuteAsync()
        {
            var format = RequestHandler.GetStringQueryString("format", false);
            var debug = RequestHandler.GetStringQueryString("debug", false);
            var ignoreLimit = RequestHandler.GetBoolValueQueryString("ignoreLimit", required: false) ?? false;
            var properties = RequestHandler.GetStringValuesQueryString("field", false);

            using (var tracker = GetTimeTracker())
            using (var token = RequestHandler.CreateTimeLimitedOperationToken()) //TODO stav: original: CreateTimeLimitedQueryToken
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
                
                await ExecuteQueryAndWriteAsync(context, query, format, debug, ignoreLimit, properties, tracker, token);
            }
        }
    }
}

using System;
using System.IO;
using System.Net;
using System.Text;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http;
using Raven.Client.Documents.Changes;
using Raven.Client.Exceptions.Documents;
using Raven.Client.Exceptions.Documents.Indexes;
using Raven.Server.Documents.Handlers.Processors.Streaming;
using Raven.Server.Documents.Handlers.Processors.TimeSeries;
using Raven.Server.Documents.Indexes;
using Raven.Server.Documents.Queries;
using Raven.Server.Documents.TimeSeries;
using Raven.Server.NotificationCenter;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Context;
using Raven.Server.TrafficWatch;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.Streaming
{
    public class StreamingHandler : DatabaseRequestHandler
    {
        [RavenAction("/databases/*/streams/docs", "GET", AuthorizationStatus.ValidUser, EndpointType.Read, DisableOnCpuCreditsExhaustion = true)]
        public async Task StreamDocsGet()
        {
            using (var processor = new StreamingHandlerProcessorForGetDocs(this))
                await processor.ExecuteAsync();
        }

        [RavenAction("/databases/*/streams/timeseries", "GET", AuthorizationStatus.ValidUser, EndpointType.Read)]
        public async Task Stream()
        {
            var documentId = GetStringQueryString("docId");
            var name = GetStringQueryString("name");
            var fromStr = GetStringQueryString("from", required: false);
            var toStr = GetStringQueryString("to", required: false);
            var offset = GetTimeSpanQueryString("offset", required: false);

            var from = string.IsNullOrEmpty(fromStr)
                ? DateTime.MinValue
                : TimeSeriesHandlerProcessorForGetTimeSeries.ParseDate(fromStr, name);

            var to = string.IsNullOrEmpty(toStr)
                ? DateTime.MaxValue
                : TimeSeriesHandlerProcessorForGetTimeSeries.ParseDate(toStr, name);

            using (ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            using (context.OpenReadTransaction())
            {
                using (var token = CreateOperationToken())
                await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
                {
                    var reader = new TimeSeriesReader(context, documentId, name, from, to, offset, token.Token);

                    writer.WriteStartObject();
                    writer.WritePropertyName("Results");
                    writer.WriteStartArray();
                    
                    foreach (var entry in reader.AllValues())
                    {
                        context.Write(writer, entry.ToTimeSeriesEntryJson());
                        writer.WriteComma();
                        await writer.MaybeFlushAsync(token.Token);
                    }

                    writer.WriteEndArray();
                    writer.WriteEndObject();

                    await writer.MaybeFlushAsync(token.Token);
                }
            }
        }

        [RavenAction("/databases/*/streams/queries", "HEAD", AuthorizationStatus.ValidUser, EndpointType.Read)]
        public Task SteamQueryHead()
        {
            return Task.CompletedTask;
        }

        [RavenAction("/databases/*/streams/queries", "GET", AuthorizationStatus.ValidUser, EndpointType.Read, DisableOnCpuCreditsExhaustion = true)]
        public async Task StreamQueryGet()
        {
            // ReSharper disable once ArgumentsStyleLiteral
            using (var tracker = new RequestTimeTracker(HttpContext, Logger, Database, "StreamQuery", doPerformanceHintIfTooLong: false))
            using (var token = CreateTimeLimitedQueryToken())
            using (var queryContext = QueryOperationContext.Allocate(Database))
            {
                var documentId = GetStringQueryString("fromDocument", false);
                string overrideQuery = null;
                if (string.IsNullOrEmpty(documentId) == false)
                {
                    Document document;
                    using (queryContext.OpenReadTransaction())
                    {
                        document = Database.DocumentsStorage.Get(queryContext.Documents, documentId);
                        if (document == null)
                        {
                            throw new DocumentDoesNotExistException($"Was request to stream a query taken from {documentId} document, but it does not exist.");
                        }
                        if (document.Data.TryGet("Query", out overrideQuery) == false)
                        {
                            throw new MissingFieldException($"Expected {documentId} to have a property named 'Query' of type 'String' but couldn't locate such property.");
                        }
                    }
                }
                var query = await IndexQueryServerSide.CreateAsync(HttpContext, GetStart(), GetPageSize(), queryContext.Documents, tracker, overrideQuery: overrideQuery);
                query.IsStream = true;

                var format = GetStringQueryString("format", false);
                var debug = GetStringQueryString("debug", false);
                var ignoreLimit = GetBoolValueQueryString("ignoreLimit", required: false) ?? false;
                var properties = GetStringValuesQueryString("field", false);
                var propertiesArray = properties.Count == 0 ? null : properties.ToArray();
                // set the exported file name prefix
                var fileNamePrefix = query.Metadata.IsCollectionQuery ? query.Metadata.CollectionName + "_collection" : "query_result";
                fileNamePrefix = $"{Database.Name}_{fileNamePrefix}";
                if (string.IsNullOrWhiteSpace(debug) == false)
                {
                    if (string.Equals(debug, "entries", StringComparison.OrdinalIgnoreCase))
                    {
                        await using (var writer = GetIndexEntriesQueryResultWriter(format, HttpContext.Response, ResponseBodyStream(), propertiesArray, fileNamePrefix))
                        {
                            try
                            {
                                await Database.QueryRunner.ExecuteStreamIndexEntriesQuery(query, queryContext, HttpContext.Response, writer, ignoreLimit, token).ConfigureAwait(false);
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
                    await using (var writer = GetQueryResultWriter(format, HttpContext.Response, queryContext.Documents, ResponseBodyStream(), propertiesArray, fileNamePrefix))
                    {
                        try
                        {
                            await Database.QueryRunner.ExecuteStreamQuery(query, queryContext, HttpContext.Response, writer, token).ConfigureAwait(false);
                        }
                        catch (IndexDoesNotExistException)
                        {
                            HttpContext.Response.StatusCode = (int)HttpStatusCode.NotFound;
                            await writer.WriteErrorAsync($"Index {query.Metadata.IndexName} does not exist");
                        }
                        catch (Exception e)
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

        [RavenAction("/databases/*/streams/queries", "POST", AuthorizationStatus.ValidUser, EndpointType.Read, DisableOnCpuCreditsExhaustion = true)]
        public async Task StreamQueryPost()
        {
            // ReSharper disable once ArgumentsStyleLiteral
            using (var tracker = new RequestTimeTracker(HttpContext, Logger, Database, "StreamQuery", doPerformanceHintIfTooLong: false))
            using (var token = CreateTimeLimitedQueryToken())
            using (var queryContext = QueryOperationContext.Allocate(Database))
            {
                var stream = TryGetRequestFromStream("ExportOptions") ?? RequestBodyStream();
                var queryJson = await queryContext.Documents.ReadForMemoryAsync(stream, "index/query");
                var query = IndexQueryServerSide.Create(HttpContext, queryJson, Database.QueryMetadataCache, tracker);
                query.IsStream = true;

                if (TrafficWatchManager.HasRegisteredClients)
                {
                    var sb = new StringBuilder();
                    // append stringBuilder with the query
                    sb.Append(query.Query);
                    // if query got parameters append with parameters
                    if (query.QueryParameters != null && query.QueryParameters.Count > 0)
                        sb.AppendLine().Append(query.QueryParameters);
                    AddStringToHttpContext(sb.ToString(), TrafficWatchChangeType.Streams);
                }

                var format = GetStringQueryString("format", false);
                var debug = GetStringQueryString("debug", false);
                var ignoreLimit = GetBoolValueQueryString("ignoreLimit", required: false) ?? false;
                var properties = GetStringValuesQueryString("field", false);
                var propertiesArray = properties.Count == 0 ? null : properties.ToArray();

                // set the exported file name prefix
                var fileNamePrefix = query.Metadata.IsCollectionQuery ? query.Metadata.CollectionName + "_collection" : "query_result";
                fileNamePrefix = $"{Database.Name}_{fileNamePrefix}";
                if (string.IsNullOrWhiteSpace(debug) == false)
                {
                    if (string.Equals(debug, "entries", StringComparison.OrdinalIgnoreCase))
                    {
                        await using (var writer = GetIndexEntriesQueryResultWriter(format, HttpContext.Response, ResponseBodyStream(), propertiesArray, fileNamePrefix))
                        {
                            try
                            {
                                await Database.QueryRunner.ExecuteStreamIndexEntriesQuery(query, queryContext, HttpContext.Response, writer, ignoreLimit, token).ConfigureAwait(false);
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
                    await using (var writer = GetQueryResultWriter(format, HttpContext.Response, queryContext.Documents, ResponseBodyStream(), propertiesArray, fileNamePrefix))
                    {
                        try
                        {
                            await Database.QueryRunner.ExecuteStreamQuery(query, queryContext, HttpContext.Response, writer, token).ConfigureAwait(false);
                        }
                        catch (IndexDoesNotExistException)
                        {
                            HttpContext.Response.StatusCode = (int)HttpStatusCode.NotFound;
                            await writer.WriteErrorAsync($"Index {query.Metadata.IndexName} does not exist");
                        }
                    }
                }
            }
        }

        private StreamCsvBlittableQueryResultWriter GetIndexEntriesQueryResultWriter(string format, HttpResponse response, Stream responseBodyStream,
            string[] propertiesArray, string fileNamePrefix = null)
        {
            if (string.IsNullOrEmpty(format) || string.Equals(format, "csv", StringComparison.OrdinalIgnoreCase) == false)
                ThrowUnsupportedException($"You have selected \"{format}\" file format, which is not supported.");

            return new StreamCsvBlittableQueryResultWriter(response, responseBodyStream, propertiesArray, fileNamePrefix);
        }

        private IStreamQueryResultWriter<Document> GetQueryResultWriter(string format, HttpResponse response, DocumentsOperationContext context, Stream responseBodyStream,
            string[] propertiesArray, string fileNamePrefix = null)
        {
            if (string.IsNullOrEmpty(format) == false && string.Equals(format, "csv", StringComparison.OrdinalIgnoreCase))
            {
                return new StreamCsvDocumentQueryResultWriter(response, responseBodyStream, context, propertiesArray, fileNamePrefix);
            }

            if (propertiesArray != null)
            {
                ThrowUnsupportedException("Using json output format with custom fields is not supported.");
            }

            return new StreamJsonDocumentQueryResultWriter(responseBodyStream, context);
        }

        private void ThrowUnsupportedException(string message)
        {
            throw new NotSupportedException(message);
        }
    }
}

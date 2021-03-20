using System;
using System.IO;
using System.Net;
using System.Text;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http;
using Raven.Client.Documents.Changes;
using Raven.Client.Exceptions.Documents;
using Raven.Client.Exceptions.Documents.Indexes;
using Raven.Server.Documents.Indexes;
using Raven.Server.Documents.Queries;
using Raven.Server.Documents.TimeSeries;
using Raven.Server.Json;
using Raven.Server.NotificationCenter;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Context;
using Raven.Server.TrafficWatch;
using Raven.Server.Utils;
using Raven.Server.Utils.Enumerators;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.Streaming
{
    public class StreamingHandler : DatabaseRequestHandler
    {
        [RavenAction("/databases/*/streams/docs", "GET", AuthorizationStatus.ValidUser, DisableOnCpuCreditsExhaustion = true)]
        public async Task StreamDocsGet()
        {
            var start = GetStart();
            var pageSize = GetPageSize();

            using (Database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            using (context.OpenReadTransaction())
            {
                var initialState = new DocsStreamingIterationState(context, Database.Configuration.Databases.PulseReadTransactionLimit)
                {
                    Start = start,
                    Take = pageSize
                };

                if (HttpContext.Request.Query.ContainsKey("startsWith"))
                {
                    initialState.StartsWith = HttpContext.Request.Query["startsWith"];
                    initialState.Excludes = HttpContext.Request.Query["excludes"];
                    initialState.Matches = HttpContext.Request.Query["matches"];
                    initialState.StartAfter = HttpContext.Request.Query["startAfter"];
                    initialState.Skip = new Reference<long>();
                }

                var documentsEnumerator = new PulsedTransactionEnumerator<Document, DocsStreamingIterationState>(context, state =>
                    {
                        if (string.IsNullOrEmpty(state.StartsWith) == false)
                        {
                            return Database.DocumentsStorage.GetDocumentsStartingWith(context, state.StartsWith, state.Matches, state.Excludes, state.StartAfter,
                                state.LastIteratedEtag == null ? state.Start : 0, // if we iterated already some docs then we pass 0 as Start and rely on state.Skip
                                state.Take,
                                state.Skip);
                        }

                        if (state.LastIteratedEtag != null)
                            return Database.DocumentsStorage.GetDocumentsInReverseEtagOrderFrom(context, state.LastIteratedEtag.Value, state.Take, skip: 1); // we seek to LastIteratedEtag but skip 1 item because we iterated it already

                        return Database.DocumentsStorage.GetDocumentsInReverseEtagOrder(context, state.Start, state.Take);
                    },
                    initialState);

                using (var token = CreateOperationToken())
                await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
                {
                    writer.WriteStartObject();
                    writer.WritePropertyName("Results");

                    await writer.WriteDocumentsAsync(context, documentsEnumerator, metadataOnly: false, token.Token);

                    writer.WriteEndObject();
                }
            }
        }

        [RavenAction("/databases/*/streams/timeseries", "GET", AuthorizationStatus.ValidUser)]
        public async Task Stream()
        {
            var documentId = GetStringQueryString("docId");
            var name = GetStringQueryString("name");
            var fromStr = GetStringQueryString("from", required: false);
            var toStr = GetStringQueryString("to", required: false);
            var offset = GetTimeSpanQueryString("offset", required: false);

            var from = string.IsNullOrEmpty(fromStr)
                ? DateTime.MinValue
                : TimeSeriesHandler.ParseDate(fromStr, name);

            var to = string.IsNullOrEmpty(toStr)
                ? DateTime.MaxValue
                : TimeSeriesHandler.ParseDate(toStr, name);

            using (ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            using (context.OpenReadTransaction())
            {
                var reader = new TimeSeriesReader(context, documentId, name, from, to, offset);

                await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
                {
                    writer.WriteStartObject();
                    writer.WritePropertyName("Results");
                    writer.WriteStartArray();
                    foreach (var entry in reader.AllValues())
                    {
                        context.Write(writer, entry.ToTimeSeriesEntryJson());
                        writer.WriteComma();
                        await writer.MaybeFlushAsync(Database.DatabaseShutdown);
                    }
                    writer.WriteEndArray();
                    writer.WriteEndObject();

                    await writer.MaybeFlushAsync(Database.DatabaseShutdown);
                }
            }
        }

        [RavenAction("/databases/*/streams/queries", "HEAD", AuthorizationStatus.ValidUser)]
        public Task SteamQueryHead()
        {
            return Task.CompletedTask;
        }

        [RavenAction("/databases/*/streams/queries", "GET", AuthorizationStatus.ValidUser, DisableOnCpuCreditsExhaustion = true)]
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
                var query = await IndexQueryServerSide.CreateAsync(HttpContext, GetStart(), GetPageSize(), queryContext.Documents, tracker, overrideQuery);
                query.IsStream = true;

                var format = GetStringQueryString("format", false);
                var debug = GetStringQueryString("debug", false);
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
                                await Database.QueryRunner.ExecuteStreamIndexEntriesQuery(query, queryContext, HttpContext.Response, writer, token).ConfigureAwait(false);
                            }
                            catch (IndexDoesNotExistException)
                            {
                                HttpContext.Response.StatusCode = (int)HttpStatusCode.NotFound;
                                writer.WriteError($"Index {query.Metadata.IndexName} does not exist");
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
                            writer.WriteError($"Index {query.Metadata.IndexName} does not exist");
                        }
                        catch (Exception e)
                        {
                            try
                            {
                                writer.WriteError($"Failed to execute stream query. Error: {e}");
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

        [RavenAction("/databases/*/streams/queries", "POST", AuthorizationStatus.ValidUser, DisableOnCpuCreditsExhaustion = true)]
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
                                await Database.QueryRunner.ExecuteStreamIndexEntriesQuery(query, queryContext, HttpContext.Response, writer, token).ConfigureAwait(false);
                            }
                            catch (IndexDoesNotExistException)
                            {
                                HttpContext.Response.StatusCode = (int)HttpStatusCode.NotFound;
                                writer.WriteError($"Index {query.Metadata.IndexName} does not exist");
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
                            writer.WriteError($"Index {query.Metadata.IndexName} does not exist");
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

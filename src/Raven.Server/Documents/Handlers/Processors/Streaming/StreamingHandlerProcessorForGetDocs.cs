using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client;
using Raven.Server.Documents.Handlers.Streaming;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Raven.Server.Utils.Enumerators;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.Processors.Streaming
{
    internal class StreamingHandlerProcessorForGetDocs : AbstractStreamingHandlerProcessorForGetDocs<DatabaseRequestHandler, DocumentsOperationContext>
    {
        public StreamingHandlerProcessorForGetDocs([NotNull] DatabaseRequestHandler requestHandler) : base(requestHandler)
        {
        }

        protected override async ValueTask GetDocumentsAndWriteAsync(DocumentsOperationContext context, int start, int pageSize, string startsWith,
            string excludes, string matches, string startAfter, string format, OperationCancelToken token)
        {
            using (context.OpenReadTransaction())
            {
                var initialState =
                    new DocsStreamingIterationState(context, RequestHandler.Database.Configuration.Databases.PulseReadTransactionLimit)
                    {
                        Start = start, 
                        Take = pageSize
                    };

                if (startsWith != null) //startsWith can be an empty string
                {
                    initialState.StartsWith = startsWith;
                    initialState.Excludes = excludes;
                    initialState.Matches = matches;
                    initialState.StartAfter = startAfter;
                    initialState.Skip = new Reference<long>();
                }

                var documentsEnumerator = new PulsedTransactionEnumerator<Document, DocsStreamingIterationState>(context, state =>
                    {
                        if (string.IsNullOrEmpty(state.StartsWith) == false)
                        {
                            return RequestHandler.Database.DocumentsStorage.GetDocumentsStartingWith(context, state.StartsWith, state.Matches, state.Excludes,
                                state.StartAfter,
                                state.LastIteratedEtag == null ? state.Start : 0, // if we iterated already some docs then we pass 0 as Start and rely on state.Skip
                                state.Take,
                                state.Skip);
                        }

                        if (state.LastIteratedEtag != null)
                            return RequestHandler.Database.DocumentsStorage.GetDocumentsInReverseEtagOrderFrom(context, state.LastIteratedEtag.Value, state.Take,
                                skip: 1); // we seek to LastIteratedEtag but skip 1 item because we iterated it already

                        return RequestHandler.Database.DocumentsStorage.GetDocumentsInReverseEtagOrder(context, state.Start, state.Take);
                    },
                    initialState);

                var databaseChangeVector = DocumentsStorage.GetDatabaseChangeVector(context);
                HttpContext.Response.Headers[Constants.Headers.Etag] = "\"" + databaseChangeVector + "\"";

                await using (var writer = GetLoadDocumentsResultsWriter(format, context, RequestHandler.ResponseBodyStream(), token.Token))
                {
                    writer.StartResponse();
                    writer.StartResults();

                    foreach (var document in documentsEnumerator)
                        await writer.AddResultAsync(document, token.Token);

                    writer.EndResults();
                    writer.EndResponse();
                }
            }
        }

        private IStreamResultsWriter<Document> GetLoadDocumentsResultsWriter(string format, JsonOperationContext context, Stream responseBodyStream, CancellationToken token)
        {
            if (string.IsNullOrEmpty(format) == false && string.Equals(format, "jsonl", StringComparison.OrdinalIgnoreCase))
                return new StreamJsonlResultsWriter(responseBodyStream, context, token);
            return new StreamResultsWriter(responseBodyStream, context, token);
        }
    }
}

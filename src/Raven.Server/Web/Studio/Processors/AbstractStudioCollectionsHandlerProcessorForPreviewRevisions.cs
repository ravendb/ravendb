using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Amqp.Listener;
using Elastic.Clients.Elasticsearch;
using JetBrains.Annotations;
using Raven.Client.Documents.Operations.Revisions;
using Raven.Server.Documents;
using Raven.Server.Documents.Handlers.Processors;
using Raven.Server.Documents.Sharding.Handlers.ContinuationTokens;
using Raven.Server.Json;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;

namespace Raven.Server.Web.Studio.Processors;

internal abstract class AbstractStudioCollectionsHandlerProcessorForPreviewRevisions<TRequestHandler, TOperationContext> : AbstractDatabaseHandlerProcessor<TRequestHandler, TOperationContext>
    where TOperationContext : JsonOperationContext
    where TRequestHandler : AbstractDatabaseRequestHandler<TOperationContext>
{
    protected string _collection;

    public AbstractStudioCollectionsHandlerProcessorForPreviewRevisions([NotNull] TRequestHandler requestHandler) : base(requestHandler)
    {
    }

    public override async ValueTask ExecuteAsync()
    {
        using (ContextPool.AllocateOperationContext(out TOperationContext context))
        using (OpenReadTransaction(context))
        using (var token = RequestHandler.CreateHttpRequestBoundOperationToken())
        {
            await InitializeAsync(context);

            var count = await GetTotalCountAsync();

            var revisions = GetRevisionsAsync(context);

            await using (var writer = new AsyncBlittableJsonTextWriter(context, RequestHandler.ResponseBodyStream()))
            {
                writer.WriteStartObject();

                writer.WritePropertyName(nameof(PreviewRevisionsResult.TotalResults));
                writer.WriteInteger(count);
                writer.WriteComma();

                writer.WritePropertyName(nameof(PreviewRevisionsResult.Results));
                writer.WriteStartArray();

                var first = true;
                await foreach (var revision in revisions)
                {
                    token.ThrowIfCancellationRequested();

                    if (first)
                        first = false;
                    else
                        writer.WriteComma();

                    writer.WriteStartObject();

                    writer.WritePropertyName(nameof(Document.Id));
                    writer.WriteString(revision.Id);
                    writer.WriteComma();

                    writer.WritePropertyName(nameof(Document.Etag));
                    writer.WriteInteger(revision.Etag);
                    writer.WriteComma();

                    writer.WritePropertyName(nameof(Document.LastModified));
                    writer.WriteDateTime(revision.LastModified, true);
                    writer.WriteComma();

                    writer.WritePropertyName(nameof(Document.ChangeVector));
                    writer.WriteString(revision.ChangeVector);
                    writer.WriteComma();

                    writer.WritePropertyName(nameof(Document.Flags));
                    writer.WriteString(revision.Flags.ToString());

                    writer.WriteEndObject();
                }

                writer.WriteEndArray();

                WriteAdditionalField(context, writer);
                writer.WriteEndObject();
            }

        }
    }

    protected abstract IDisposable OpenReadTransaction(TOperationContext context);

    protected virtual void WriteAdditionalField(TOperationContext context, AsyncBlittableJsonTextWriter writer)
    {
    }

    protected sealed class PreviewRevisionsResult
    {
        public List<Document> Results;
        public long TotalResults;
    }

    protected abstract ValueTask<long> GetTotalCountAsync();

    protected abstract IAsyncEnumerable<Document> GetRevisionsAsync(TOperationContext context);

    protected virtual ValueTask InitializeAsync(TOperationContext context)
    {
        _collection = RequestHandler.GetStringQueryString("collection", required: false);

        if (NotModified(context, out var etag))
        {
            RequestHandler.HttpContext.Response.StatusCode = (int)HttpStatusCode.NotModified;
        }
        else if (etag != null)
            HttpContext.Response.Headers["ETag"] = "\"" + etag + "\"";

        return ValueTask.CompletedTask;
    }

    protected abstract bool NotModified(TOperationContext context, out string etag);
}


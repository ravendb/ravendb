using System;
using System.Collections.Generic;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client;
using Raven.Server.Documents;
using Raven.Server.Documents.Handlers.Processors;
using Sparrow.Json;

namespace Raven.Server.Web.Studio.Processors;

internal abstract class AbstractStudioCollectionsHandlerProcessorForPreviewRevisions<TRequestHandler, TOperationContext> : AbstractHandlerProcessor<TRequestHandler>
    where TOperationContext : JsonOperationContext
    where TRequestHandler : AbstractDatabaseRequestHandler<TOperationContext>
{

    protected readonly JsonContextPoolBase<TOperationContext> ContextPool;

    protected string Collection;

    protected AbstractStudioCollectionsHandlerProcessorForPreviewRevisions([NotNull] TRequestHandler requestHandler) : base(requestHandler)
    {
        ContextPool = RequestHandler.ContextPool;
    }

    public override async ValueTask ExecuteAsync()
    {
        using (ContextPool.AllocateOperationContext(out TOperationContext context))
        using (OpenReadTransaction(context))
        using (var token = RequestHandler.CreateHttpRequestBoundOperationToken())
        {
            await InitializeAsync(context, token.Token);

            if (NotModified(context, out var etag))
            {
                RequestHandler.HttpContext.Response.StatusCode = (int)HttpStatusCode.NotModified;
                return;
            }

            if (etag != null)
                HttpContext.Response.Headers[Constants.Headers.Etag] = "\"" + etag + "\"";

            var count = await GetTotalCountAsync();

            await using (var writer = new AsyncBlittableJsonTextWriter(context, RequestHandler.ResponseBodyStream()))
            {
                writer.WriteStartObject();

                writer.WritePropertyName(nameof(PreviewRevisionsResult.TotalResults));
                writer.WriteInteger(count);
                writer.WriteComma();

                writer.WritePropertyName(nameof(PreviewRevisionsResult.Results));
                await WriteItemsAsync(context, writer);

                writer.WriteEndObject();
            }

        }
    }

    protected abstract IDisposable OpenReadTransaction(TOperationContext context);

    protected abstract ValueTask<long> GetTotalCountAsync();

    protected abstract bool NotModified(TOperationContext context, out string etag);

    protected abstract Task WriteItemsAsync(TOperationContext context, AsyncBlittableJsonTextWriter writer);

    protected virtual Task InitializeAsync(TOperationContext context, CancellationToken token)
    {
        Collection = RequestHandler.GetStringQueryString("collection", required: false);

        return Task.CompletedTask;
    }

    protected sealed class PreviewRevisionsResult
    {
        public List<Document> Results;
        public long TotalResults;
    }
}


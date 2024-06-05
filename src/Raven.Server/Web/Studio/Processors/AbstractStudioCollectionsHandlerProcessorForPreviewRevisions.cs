using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Microsoft.AspNetCore.Http;
using Raven.Server.Documents;
using Raven.Server.Documents.Handlers.Processors;
using Sparrow.Json;

namespace Raven.Server.Web.Studio.Processors;

internal abstract class AbstractStudioCollectionsHandlerProcessorForPreviewRevisions<TRequestHandler, TOperationContext> : IDisposable
    where TOperationContext : JsonOperationContext
    where TRequestHandler : AbstractDatabaseRequestHandler<TOperationContext>
{
    protected readonly TRequestHandler RequestHandler;

    protected readonly HttpContext HttpContext;

    protected readonly JsonContextPoolBase<TOperationContext> ContextPool;

    protected string Collection;

    public AbstractStudioCollectionsHandlerProcessorForPreviewRevisions([NotNull] TRequestHandler requestHandler)
    {
        RequestHandler = requestHandler ?? throw new ArgumentNullException(nameof(requestHandler));
        HttpContext = requestHandler.HttpContext;
        ContextPool = RequestHandler.ContextPool;
    }

    public async ValueTask ExecuteAsync()
    {
        using (ContextPool.AllocateOperationContext(out TOperationContext context))
        using (OpenReadTransaction(context))
        using (var token = RequestHandler.CreateHttpRequestBoundOperationToken())
        {
            Collection = RequestHandler.GetStringQueryString("collection", required: false);

            if (NotModified(context, out var etag))
            {
                RequestHandler.HttpContext.Response.StatusCode = (int)HttpStatusCode.NotModified;
                return;
            }

            if (etag != null)
                HttpContext.Response.Headers["ETag"] = "\"" + etag + "\"";

            await InitializeAsync(context, token.Token);

            var count = await GetTotalCountAsync();

            await using (var writer = new AsyncBlittableJsonTextWriter(context, RequestHandler.ResponseBodyStream()))
            {
                writer.WriteStartObject();

                writer.WritePropertyName(nameof(PreviewRevisionsResult.TotalResults));
                writer.WriteInteger(count);
                writer.WriteComma();

                writer.WritePropertyName(nameof(PreviewRevisionsResult.Results));
                await WriteItems(context, writer);

                
                writer.WriteEndObject();
            }

        }
    }

    protected abstract IDisposable OpenReadTransaction(TOperationContext context);

    protected abstract ValueTask<long> GetTotalCountAsync();

    protected abstract bool NotModified(TOperationContext context, out string etag);

    protected abstract Task WriteItems(TOperationContext context, AsyncBlittableJsonTextWriter writer);

    protected virtual Task InitializeAsync(TOperationContext context, CancellationToken token)
    {
        Collection = RequestHandler.GetStringQueryString("collection", required: false);

        return Task.CompletedTask;
    }

    public virtual void Dispose()
    {
        GC.SuppressFinalize(this);
    }

    protected sealed class PreviewRevisionsResult
    {
        public List<Document> Results;
        public long TotalResults;
    }
}


using System;
using System.Collections.Generic;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client;
using Raven.Server.Documents;
using Raven.Server.Documents.Handlers.Processors;
using Raven.Server.Documents.Revisions;
using Sparrow.Json;
using Enum = System.Enum;

namespace Raven.Server.Web.Studio.Processors;

internal abstract class AbstractStudioCollectionsHandlerProcessorForPreviewRevisions<TRequestHandler, TOperationContext> : AbstractHandlerProcessor<TRequestHandler>
    where TOperationContext : JsonOperationContext
    where TRequestHandler : AbstractDatabaseRequestHandler<TOperationContext>
{
    protected const int TotalResultsUnsupported = -1;

    protected readonly JsonContextPoolBase<TOperationContext> ContextPool;

    protected readonly string Collection;

    protected readonly RevisionsStorage.RevisionType Type;

    protected AbstractStudioCollectionsHandlerProcessorForPreviewRevisions([NotNull] TRequestHandler requestHandler) : base(requestHandler)
    {
        ContextPool = RequestHandler.ContextPool;
        Collection = RequestHandler.GetStringQueryString("collection", required: false);
        var type = RequestHandler.GetStringQueryString("type", required: false);
        if (type == null)
        {
            Type = RevisionsStorage.RevisionType.All;
            return;
        }

        if (Enum.TryParse(type, true, out Type) == false)
        {
            throw new ArgumentException(
                $"Invalid value '{type}' provided for 'type'. Please use one of the following options: {string.Join(", ", Enum.GetNames(typeof(RevisionsStorage.RevisionType)))}.");
        }
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

                if (count >= 0)
                {
                    writer.WritePropertyName(nameof(PreviewRevisionsResult.TotalResults));
                    writer.WriteInteger(count);
                    writer.WriteComma();
                }

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

    protected abstract Task InitializeAsync(TOperationContext context, CancellationToken token);

    protected sealed class PreviewRevisionsResult
    {
        public List<Document> Results;
        public long TotalResults;
    }

}


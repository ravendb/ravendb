﻿using System.Net;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client;
using Raven.Server.Web;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.Processors.Documents;

internal abstract class AbstractDocumentHandlerProcessorForHead<TRequestHandler, TOperationContext> : AbstractHandlerProcessor<TRequestHandler, TOperationContext>
    where TRequestHandler : RequestHandler
    where TOperationContext : JsonOperationContext
{
    protected AbstractDocumentHandlerProcessorForHead([NotNull] TRequestHandler requestHandler, [NotNull] JsonContextPoolBase<TOperationContext> contextPool) : base(requestHandler, contextPool)
    {
    }

    protected abstract ValueTask<(HttpStatusCode StatusCode, string ChangeVector)> GetStatusCodeAndChangeVector(string docId, TOperationContext context);

    public override async ValueTask ExecuteAsync()
    {
        var id = RequestHandler.GetQueryStringValueAndAssertIfSingleAndNotEmpty("id");

        using (ContextPool.AllocateOperationContext(out TOperationContext context))
        {
            var result = await GetStatusCodeAndChangeVector(id, context);

            HttpContext.Response.StatusCode = (int)result.StatusCode;

            if (result.ChangeVector != null)
                HttpContext.Response.Headers[Constants.Headers.Etag] = result.ChangeVector;
        }

    }
}

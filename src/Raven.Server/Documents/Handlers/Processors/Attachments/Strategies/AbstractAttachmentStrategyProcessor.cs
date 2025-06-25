using System;
using JetBrains.Annotations;
using Microsoft.AspNetCore.Http;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Logging;
using Sparrow.Server.Logging;

namespace Raven.Server.Documents.Handlers.Processors.Attachments.Strategies;

internal abstract class AbstractAttachmentStrategyProcessor<TRequestHandler, TOperationContext> : IDisposable
    where TOperationContext : JsonOperationContext 
    where TRequestHandler : AbstractDatabaseRequestHandler<TOperationContext>
{
    protected readonly ClusterContextPool ClusterContextPool;
    protected readonly TRequestHandler RequestHandler;
    protected readonly HttpContext HttpContext;
    protected ServerStore ServerStore => RequestHandler.ServerStore;
    protected readonly JsonContextPoolBase<TOperationContext> ContextPool;
    protected readonly RavenLogger Logger;

    protected AbstractAttachmentStrategyProcessor([NotNull] TRequestHandler requestHandler)
    {
        RequestHandler = requestHandler ?? throw new ArgumentNullException(nameof(requestHandler));
        HttpContext = requestHandler.HttpContext;
        ClusterContextPool = requestHandler.ServerStore.Engine.ContextPool;
        ContextPool = requestHandler.ContextPool;
        Logger = requestHandler.Logger;
    }

    public virtual void Dispose()
    {
        GC.SuppressFinalize(this);
    }
}

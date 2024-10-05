using System;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Microsoft.AspNetCore.Http;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Raven.Server.Web;
using Sparrow.Json;
using Sparrow.Logging;
using Sparrow.Server.Logging;

namespace Raven.Server.Documents.Handlers.Processors
{
    internal abstract class AbstractHandlerProcessor<TRequestHandler> : IDisposable
        where TRequestHandler : RequestHandler
    {
        protected readonly ClusterContextPool ClusterContextPool;

        protected readonly TRequestHandler RequestHandler;

        protected readonly HttpContext HttpContext;
        protected ServerStore ServerStore => RequestHandler.ServerStore;

        protected AbstractHandlerProcessor([NotNull] TRequestHandler requestHandler)
        {
            RequestHandler = requestHandler ?? throw new ArgumentNullException(nameof(requestHandler));
            HttpContext = requestHandler.HttpContext;
            ClusterContextPool = requestHandler.ServerStore.Engine.ContextPool;
        }

        public abstract ValueTask ExecuteAsync();

        public virtual void Dispose()
        {
            GC.SuppressFinalize(this);
        }
    }

    internal abstract class AbstractDatabaseHandlerProcessor<TRequestHandler, TOperationContext> : AbstractHandlerProcessor<TRequestHandler>
        where TOperationContext : JsonOperationContext
        where TRequestHandler : AbstractDatabaseRequestHandler<TOperationContext>
    {
        protected readonly JsonContextPoolBase<TOperationContext> ContextPool;

        protected readonly RavenLogger Logger;

        protected AbstractDatabaseHandlerProcessor([NotNull] TRequestHandler requestHandler) : base(requestHandler)
        {
            ContextPool = requestHandler.ContextPool;
            Logger = requestHandler.Logger;
        }
    }

    internal abstract class AbstractDatabaseHandlerProcessor<TOperationContext> : AbstractDatabaseHandlerProcessor<AbstractDatabaseRequestHandler<TOperationContext>, TOperationContext>
        where TOperationContext : JsonOperationContext
    {
        protected AbstractDatabaseHandlerProcessor([NotNull] AbstractDatabaseRequestHandler<TOperationContext> requestHandler) : base(requestHandler)
        {
        }
    }
}

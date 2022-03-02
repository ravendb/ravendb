using System;
using JetBrains.Annotations;
using Microsoft.AspNetCore.Http;
using Sparrow.Json;

namespace Raven.Server.Web.Studio.Sharding.Processors
{
    internal abstract class AbstractHandlerProcessor<TRequestHandler, TOperationContext> : IDisposable
            where TRequestHandler : RequestHandler
            where TOperationContext : JsonOperationContext
    {
        protected readonly TRequestHandler RequestHandler;

        protected readonly HttpContext HttpContext;

        protected readonly JsonContextPoolBase<TOperationContext> ContextPool;

        protected AbstractHandlerProcessor([NotNull] TRequestHandler requestHandler, [NotNull] JsonContextPoolBase<TOperationContext> contextPool)
        {
            RequestHandler = requestHandler ?? throw new ArgumentNullException(nameof(requestHandler));
            HttpContext = requestHandler.HttpContext;
            ContextPool = contextPool;
        }

        public virtual void Dispose()
        {
            GC.SuppressFinalize(this);
        }
    }

}

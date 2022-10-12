using System.Threading.Tasks;
using JetBrains.Annotations;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.Processors.Stats
{
    internal abstract class AbstractStatsHandlerProcessorForDatabaseHealthCheck<TRequestHandler, TOperationContext> : AbstractDatabaseHandlerProcessor<TRequestHandler, TOperationContext>
        where TOperationContext : JsonOperationContext
        where TRequestHandler : AbstractDatabaseRequestHandler<TOperationContext>
    {
        protected AbstractStatsHandlerProcessorForDatabaseHealthCheck([NotNull] TRequestHandler requestHandler) : base(requestHandler)
        {
        }

        protected abstract Task GetNoContentStatusAsync();

        public override async ValueTask ExecuteAsync()
        {
            await GetNoContentStatusAsync();
        }
    }
}

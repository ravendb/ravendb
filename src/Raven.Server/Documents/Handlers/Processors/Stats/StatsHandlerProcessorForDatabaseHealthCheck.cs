using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Handlers.Processors.Stats
{
    internal sealed class StatsHandlerProcessorForDatabaseHealthCheck : AbstractStatsHandlerProcessorForDatabaseHealthCheck<DatabaseRequestHandler, DocumentsOperationContext>
    {
        public StatsHandlerProcessorForDatabaseHealthCheck([NotNull] DatabaseRequestHandler requestHandler) : base(requestHandler)
        {
        }

        protected override Task GetNoContentStatusAsync()
        {
            RequestHandler.NoContentStatus();
            return Task.CompletedTask;
        }
    }
}

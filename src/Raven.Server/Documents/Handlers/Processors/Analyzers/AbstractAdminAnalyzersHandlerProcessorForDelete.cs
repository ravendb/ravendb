using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Server.ServerWide.Commands.Analyzers;
using Sparrow.Json;
using Sparrow.Logging;

namespace Raven.Server.Documents.Handlers.Processors.Analyzers
{
    internal abstract class AbstractAdminAnalyzersHandlerProcessorForDelete<TRequestHandler, TOperationContext> : AbstractDatabaseHandlerProcessor<TRequestHandler, TOperationContext>
        where TRequestHandler : AbstractDatabaseRequestHandler<TOperationContext>
        where TOperationContext : JsonOperationContext 
    {
        protected AbstractAdminAnalyzersHandlerProcessorForDelete([NotNull] TRequestHandler requestHandler) : base(requestHandler)
        {
        }

        public override async ValueTask ExecuteAsync()
        {
            var name = RequestHandler.GetQueryStringValueAndAssertIfSingleAndNotEmpty("name");
            var databaseName = RequestHandler.DatabaseName;

            if (LoggingSource.AuditLog.IsInfoEnabled)
            {
                RequestHandler.LogAuditFor(databaseName, "DELETE", $"Analyzer '{name}'");
            }

            var command = new DeleteAnalyzerCommand(name, databaseName, RequestHandler.GetRaftRequestIdFromQuery());
            var index = (await RequestHandler.ServerStore.SendToLeaderAsync(command)).Index;

            await RequestHandler.WaitForIndexNotificationAsync(index);

            RequestHandler.NoContentStatus();
        }
    }
}

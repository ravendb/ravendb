using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Server.ServerWide.Commands.Analyzers;
using Raven.Server.Web;
using Sparrow.Json;
using Sparrow.Logging;

namespace Raven.Server.Documents.Handlers.Processors.Analyzers
{
    internal abstract class AbstractAdminAnalyzersHandlerProcessorForDelete<TRequestHandler, TOperationContext> : AbstractHandlerProcessor<TRequestHandler, TOperationContext>
        where TRequestHandler : RequestHandler
        where TOperationContext : JsonOperationContext
    {
        protected AbstractAdminAnalyzersHandlerProcessorForDelete([NotNull] TRequestHandler requestHandler, [NotNull] JsonContextPoolBase<TOperationContext> contextPool) : base(requestHandler, contextPool)
        {
        }

        protected abstract string GetDatabaseName();

        protected abstract ValueTask WaitForIndexNotificationAsync(long index);

        public override async ValueTask ExecuteAsync()
        {
            var name = RequestHandler.GetQueryStringValueAndAssertIfSingleAndNotEmpty("name");
            var databaseName = GetDatabaseName();

            if (LoggingSource.AuditLog.IsInfoEnabled)
            {
                var clientCert = RequestHandler.GetCurrentCertificate();

                var auditLog = LoggingSource.AuditLog.GetLogger(databaseName, "Audit");
                auditLog.Info($"Analyzer {name} DELETE by {clientCert?.Subject} {clientCert?.Thumbprint}");
            }

            var command = new DeleteAnalyzerCommand(name, databaseName, RequestHandler.GetRaftRequestIdFromQuery());
            var index = (await RequestHandler.ServerStore.SendToLeaderAsync(command)).Index;

            await WaitForIndexNotificationAsync(index);

            RequestHandler.NoContentStatus();
        }
    }
}

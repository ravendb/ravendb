using System.Net;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Server.Documents.Indexes.Analysis;
using Raven.Server.Json;
using Raven.Server.ServerWide.Commands.Analyzers;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Logging;

namespace Raven.Server.Documents.Handlers.Processors.Analyzers
{
    internal abstract class AbstractAdminAnalyzersHandlerProcessorForPut<TRequestHandler, TOperationContext> : AbstractDatabaseHandlerProcessor<TRequestHandler, TOperationContext>
        where TRequestHandler : AbstractDatabaseRequestHandler<TOperationContext>
        where TOperationContext : JsonOperationContext 
    {
        protected AbstractAdminAnalyzersHandlerProcessorForPut([NotNull] TRequestHandler requestHandler) : base(requestHandler)
        {
        }

        public override async ValueTask ExecuteAsync()
        {
            var databaseName = RequestHandler.DatabaseName;

            using (RequestHandler.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            {
                var input = await context.ReadForMemoryAsync(RequestHandler.RequestBodyStream(), "Analyzers");
                if (input.TryGet("Analyzers", out BlittableJsonReaderArray analyzers) == false)
                    Web.RequestHandler.ThrowRequiredPropertyNameInRequest("Analyzers");

                var command = new PutAnalyzersCommand(databaseName, RequestHandler.GetRaftRequestIdFromQuery());
                foreach (var analyzerToAdd in analyzers)
                {
                    var analyzerDefinition = JsonDeserializationServer.AnalyzerDefinition((BlittableJsonReaderObject)analyzerToAdd);
                    analyzerDefinition.Name = analyzerDefinition.Name?.Trim();

                    if (LoggingSource.AuditLog.IsInfoEnabled)
                    {
                        var clientCert = RequestHandler.GetCurrentCertificate();

                        var auditLog = LoggingSource.AuditLog.GetLogger(databaseName, "Audit");
                        auditLog.Info($"Analyzer {analyzerDefinition.Name} PUT by {clientCert?.Subject} {clientCert?.Thumbprint} with definition: {analyzerToAdd}");
                    }

                    analyzerDefinition.Validate();

                    // check if analyzer is compilable
                    AnalyzerCompiler.Compile(analyzerDefinition.Name, analyzerDefinition.Code);

                    command.Analyzers.Add(analyzerDefinition);
                }

                var index = (await RequestHandler.ServerStore.SendToLeaderAsync(command)).Index;

                await RequestHandler.WaitForIndexNotificationAsync(index);

                RequestHandler.NoContentStatus(HttpStatusCode.Created);
            }
        }
    }
}

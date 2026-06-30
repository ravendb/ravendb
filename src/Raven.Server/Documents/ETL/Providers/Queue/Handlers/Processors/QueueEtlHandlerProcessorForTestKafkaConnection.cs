using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Server.Documents.Handlers.Processors;
using Sparrow.Json;

namespace Raven.Server.Documents.ETL.Providers.Queue.Handlers.Processors
{
    internal sealed class QueueEtlHandlerProcessorForTestKafkaConnection<TRequestHandler, TOperationContext> : AbstractDatabaseHandlerProcessor<TRequestHandler, TOperationContext>
        where TOperationContext : JsonOperationContext
        where TRequestHandler : AbstractDatabaseRequestHandler<TOperationContext>
    {
        public QueueEtlHandlerProcessorForTestKafkaConnection([NotNull] TRequestHandler requestHandler) : base(requestHandler)
        {
        }

        public override ValueTask ExecuteAsync() => new(QueueEtlTestConnectionHelpers.TestKafkaAsync(RequestHandler));
    }
}

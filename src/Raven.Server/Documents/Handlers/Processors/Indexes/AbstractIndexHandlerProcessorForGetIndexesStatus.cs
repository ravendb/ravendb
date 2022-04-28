using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Client.Http;
using Raven.Server.Web;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.Processors.Indexes
{
    internal abstract class AbstractIndexHandlerProcessorForGetIndexesStatus<TRequestHandler, TOperationContext> : AbstractHandlerProxyReadProcessor<IndexingStatus, TRequestHandler,
            TOperationContext>
        where TOperationContext : JsonOperationContext 
        where TRequestHandler : AbstractDatabaseRequestHandler<TOperationContext>
    {
        protected AbstractIndexHandlerProcessorForGetIndexesStatus([NotNull] TRequestHandler requestHandler) : base(requestHandler)
        {
        }

        protected override RavenCommand<IndexingStatus> CreateCommandForNode(string nodeTag) => new GetIndexingStatusOperation.GetIndexingStatusCommand(nodeTag);
    }
}

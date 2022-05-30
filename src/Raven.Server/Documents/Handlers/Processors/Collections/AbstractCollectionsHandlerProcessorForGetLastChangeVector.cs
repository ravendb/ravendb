using JetBrains.Annotations;
using Raven.Client.Http;
using Raven.Server.Documents.Sharding.Operations;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.Processors.Collections
{
    internal abstract class AbstractCollectionsHandlerProcessorForGetLastChangeVector<TRequestHandler, TOperationContext> : AbstractHandlerProxyReadProcessor<LastChangeVectorForCollectionResult, TRequestHandler, TOperationContext>
        where TRequestHandler : AbstractDatabaseRequestHandler<TOperationContext>
        where TOperationContext : JsonOperationContext
    {
        public AbstractCollectionsHandlerProcessorForGetLastChangeVector([NotNull] TRequestHandler requestHandler) : base(requestHandler)
        {
        }

        protected string GetCollectionName()
        {
            return RequestHandler.GetStringQueryString("name");
        }

        protected override RavenCommand<LastChangeVectorForCollectionResult> CreateCommandForNode(string nodeTag)
        {
            return new ShardedLastChangeVectorForCollectionOperation.LastChangeVectorForCollectionCommand(GetCollectionName());
        }
    }
}

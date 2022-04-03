using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Server.Documents.Handlers.Processors.DocumentsCompression;

namespace Raven.Server.Documents.Sharding.Handlers.Processors.DocumentsCompression
{
    internal class ShardedDocumentsCompressionHandlerProcessorForPost : AbstractDocumentsCompressionHandlerProcessorForPost<ShardedDatabaseRequestHandler>
    {
        public ShardedDocumentsCompressionHandlerProcessorForPost([NotNull] ShardedDatabaseRequestHandler requestHandler) : base(requestHandler)
        {
        }

        protected override string GetDatabaseName() => RequestHandler.DatabaseContext.DatabaseName;

        protected override ValueTask WaitForIndexNotificationAsync(long index) => RequestHandler.DatabaseContext.Cluster.WaitForExecutionOnAllNodesAsync(index);
    }
}

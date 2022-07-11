using Raven.Server.Documents.Commands.Streaming;
using Raven.Server.Documents.Queries;
using Raven.Server.Documents.Sharding.Handlers;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;

namespace Raven.Server.Documents.Sharding.Queries
{
    internal class ShardedQueryStreamProcessor : ShardedQueryProcessor
    {
        public ShardedQueryStreamProcessor(TransactionOperationContext context, ShardedDatabaseRequestHandler parent, IndexQueryServerSide query) : base(context, parent, query)
        {
        }

        public override void CreateQueryCommands(BlittableJsonReaderObject queryTemplate)
        {
            //this is done in the EP processor
        }
    }
}

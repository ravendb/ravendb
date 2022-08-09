using System.Collections.Generic;
using System.Threading;
using Raven.Server.Documents.Queries;
using Raven.Server.Documents.Sharding.Handlers;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;

namespace Raven.Server.Documents.Sharding.Queries
{
    internal class ShardedQueryStreamProcessor : ShardedQueryProcessor
    {
        public ShardedQueryStreamProcessor(TransactionOperationContext context, ShardedDatabaseRequestHandler requestHandler, IndexQueryServerSide query, CancellationToken token) : base(context, requestHandler, query, false, false, token)
        {
        }

        public override void CreateQueryCommands(Dictionary<int, BlittableJsonReaderObject> queryTemplates, string indexName)
        {
            //this is done in the EP processor
        }
    }
}

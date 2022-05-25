using System.Collections.Generic;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Server.Documents.Handlers.Processors.Studio;
using Raven.Server.Documents.Sharding.Operations;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Utils;

namespace Raven.Server.Documents.Sharding.Handlers.Processors.Studio
{
    internal class ShardedStudioCollectionFieldsHandlerProcessorForGetCollectionFields : AbstractStudioCollectionFieldsHandlerProcessorForGetCollectionFields<ShardedDatabaseRequestHandler, TransactionOperationContext>
    {
        public ShardedStudioCollectionFieldsHandlerProcessorForGetCollectionFields([NotNull] ShardedDatabaseRequestHandler requestHandler) : base(requestHandler)
        {
        }

        protected override async ValueTask<Dictionary<LazyStringValue, FieldType>> GetFieldsAsync(TransactionOperationContext context, string collection, string prefix)
        {
            DevelopmentHelper.ShardingToDo(DevelopmentHelper.TeamMember.Stav, DevelopmentHelper.Severity.Normal, "Handle NotModified");

            using (var token = RequestHandler.CreateOperationToken())
            {
                var op = new ShardedGetCollectionFieldsOperation(context, HttpContext, collection, prefix);
                var collectionFields = await RequestHandler.ShardExecutor.ExecuteParallelForAllAsync(op, token.Token);
                
                return collectionFields;
            }
        }
    }
}

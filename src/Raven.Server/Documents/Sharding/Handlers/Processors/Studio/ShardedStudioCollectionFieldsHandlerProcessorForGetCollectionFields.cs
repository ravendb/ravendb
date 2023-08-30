using System.Collections.Generic;
using System.Net;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client;
using Raven.Server.Documents.Handlers.Processors.Studio;
using Raven.Server.Documents.Sharding.Operations;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;

namespace Raven.Server.Documents.Sharding.Handlers.Processors.Studio
{
    internal sealed class ShardedStudioCollectionFieldsHandlerProcessorForGetCollectionFields : AbstractStudioCollectionFieldsHandlerProcessorForGetCollectionFields<ShardedDatabaseRequestHandler, TransactionOperationContext>
    {
        public ShardedStudioCollectionFieldsHandlerProcessorForGetCollectionFields([NotNull] ShardedDatabaseRequestHandler requestHandler) : base(requestHandler)
        {
        }

        protected override async ValueTask<Dictionary<LazyStringValue, FieldType>> GetFieldsAsync(TransactionOperationContext context, string collection, string prefix)
        {
            using (var token = RequestHandler.CreateHttpRequestBoundOperationToken())
            {
                var etag = RequestHandler.GetStringFromHeaders(Constants.Headers.IfNoneMatch);
                var op = new ShardedGetCollectionFieldsOperation(context, HttpContext, collection, prefix, etag);
                var collectionFields = await RequestHandler.ShardExecutor.ExecuteParallelForAllAsync(op, token.Token);

                if (collectionFields.StatusCode == (int)HttpStatusCode.NotModified)
                {
                    HttpContext.Response.StatusCode = (int)HttpStatusCode.NotModified;
                    return null;
                }

                HttpContext.Response.Headers["ETag"] = "\"" + collectionFields.CombinedEtag + "\"";

                return collectionFields.Result;
            }
        }

        protected override DocumentsTransaction OpenReadTransaction(TransactionOperationContext context)
        {
            return null;
        }
    }
}

using System.Collections.Generic;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Http;
using Raven.Server.Documents;
using Raven.Server.Documents.Handlers.Processors;
using Raven.Server.Json;
using Raven.Server.Utils;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using BucketStats = Raven.Server.Documents.Sharding.BucketStats;

namespace Raven.Server.Web.Studio.Processors
{
    internal abstract class AbstractBucketsHandlerProcessorForGetBucket<TRequestHandler, TOperationContext> : AbstractDatabaseHandlerProcessor<TRequestHandler, TOperationContext>
        where TOperationContext : JsonOperationContext
        where TRequestHandler : AbstractDatabaseRequestHandler<TOperationContext>
    {
        protected AbstractBucketsHandlerProcessorForGetBucket([NotNull] TRequestHandler requestHandler) : base(requestHandler)
        {
        }

        public int GetBucket()
        {
            return RequestHandler.GetIntValueQueryString("bucket", required: true) ?? -1;
        }
        
        protected abstract ValueTask<BucketInfo> GetBucketInfo(TOperationContext context, int bucket);

        public override async ValueTask ExecuteAsync()
        {
            using(ContextPool.AllocateOperationContext(out TOperationContext context))
            await using (var writer = new AsyncBlittableJsonTextWriterForDebug(context, ServerStore, RequestHandler.ResponseBodyStream()))
            {
                var bucketInfo = await GetBucketInfo(context, GetBucket());
                writer.WriteObject(context.ReadObject(bucketInfo.ToJson(), "bucket/results"));
            }
        }
    }

    public sealed class GetBucketInfoCommand : RavenCommand<BucketInfo>
    {
        private readonly int _bucket;
        
        internal GetBucketInfoCommand(int bucket)
        {
            _bucket = bucket;
        }

        public override bool IsReadRequest => true;

        public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
        {
            var pathBuilder = new StringBuilder(node.Url);
            pathBuilder.Append($"/databases/{node.Database}/debug/sharding/bucket?");
            pathBuilder.Append($"&bucket={_bucket}");
            
            var request = new HttpRequestMessage {Method = HttpMethod.Get,};

            url = pathBuilder.ToString();
            return request;
        }

        public override void SetResponse(JsonOperationContext context, BlittableJsonReaderObject response, bool fromCache)
        {
            if (response == null)
            {
                Result = null;
                return;
            }

            Result = JsonDeserializationServer.BucketInfo(response);
        }
    }

    public sealed class BucketInfo : BucketStats
    {
        public List<string> Items;

        public override DynamicJsonValue ToJson()
        {
            var json = base.ToJson();
            json[nameof(Items)] = new DynamicJsonArray(Items);
            return json;
        }
    }
}

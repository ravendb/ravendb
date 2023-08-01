using System.Net.Http;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client;
using Raven.Client.Http;
using Raven.Server.Documents;
using Raven.Server.Documents.Handlers.Processors;
using Raven.Server.Extensions;
using Raven.Server.Json;
using Raven.Server.Utils;
using Sparrow.Json;

namespace Raven.Server.Web.Studio.Processors
{
    internal abstract class AbstractBucketsHandlerProcessorForGetBuckets<TRequestHandler, TOperationContext> : AbstractDatabaseHandlerProcessor<TRequestHandler, TOperationContext>
        where TOperationContext : JsonOperationContext
        where TRequestHandler : AbstractDatabaseRequestHandler<TOperationContext>
    {
        protected AbstractBucketsHandlerProcessorForGetBuckets([NotNull] TRequestHandler requestHandler) : base(requestHandler)
        {
        }

        protected (int FromBucket, int ToBucket, int Range) GetParameters()
        {
            var fromBucket = RequestHandler.GetIntValueQueryString("fromBucket", required: false) ?? 0;
            var toBucket = RequestHandler.GetIntValueQueryString("toBucket", required: false) ?? int.MaxValue;
            var range = RequestHandler.GetIntValueQueryString("range", required: false) ?? (32 * 1024);
            
            return (fromBucket, toBucket, range);
        }

        public override async ValueTask ExecuteAsync()
        {
            (int fromBucket, int toBucket, int range) = GetParameters();
            
            using(ContextPool.AllocateOperationContext(out TOperationContext context))
            using (var token = RequestHandler.CreateHttpRequestBoundOperationToken())
            {
                var bucketsResults = await GetBucketsResults(context, fromBucket, toBucket, range,token.Token);

                await using (var writer = new AsyncBlittableJsonTextWriterForDebug(context, ServerStore, RequestHandler.ResponseBodyStream()))
                {
                    writer.WriteObject(context.ReadObject(bucketsResults.ToJson(), "buckets/results"));
                }
            }
        }

        protected abstract ValueTask<BucketsResults> GetBucketsResults(TOperationContext context, int fromBucket, int toBucket, int range, CancellationToken token);
    }

    public sealed class GetBucketsCommand : RavenCommand<BucketsResults>
    {
        private readonly int _fromBucket;
        private readonly int _toBucket;
        private readonly int _range;
        private readonly int? _shardNumber;

        internal GetBucketsCommand(int fromBucket, int toBucket, int range, int? shardNumber = null)
        {
            _fromBucket = fromBucket;
            _toBucket = toBucket;
            _range = range;
            _shardNumber = shardNumber;
        }

        public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
        {
            var pathBuilder = new StringBuilder(node.Url);
            pathBuilder.Append($"/databases/{node.Database}/debug/sharding/buckets?");
            pathBuilder.Append($"&fromBucket={_fromBucket}");
            pathBuilder.Append($"&toBucket={_toBucket}");
            pathBuilder.Append($"&range={_range}");

            if (_shardNumber.HasValue)
                pathBuilder.Append($"&{Constants.QueryString.ShardNumber}={_shardNumber}");

            var request = new HttpRequestMessage
            {
                Method = HttpMethod.Get,
            };

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

            Result = JsonDeserializationServer.BucketsResults(response);
        }

        public override bool IsReadRequest => true;
    }
}

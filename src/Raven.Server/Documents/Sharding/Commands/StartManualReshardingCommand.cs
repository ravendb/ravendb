using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Operations;
using Raven.Client.Http;
using Sparrow.Json;

namespace Raven.Server.Documents.Sharding.Commands
{
    public sealed class StartManualReshardingOperation : IMaintenanceOperation
    {
        private readonly long _fromBucket;
        private readonly long _toBucket;
        private readonly int _toShard;

        public StartManualReshardingOperation(long fromBucket, long toBucket, int toShard)
        {
            _fromBucket = fromBucket;
            _toBucket = toBucket;
            _toShard = toShard;
        }

        public RavenCommand GetCommand(DocumentConventions conventions, JsonOperationContext context)
        {
            return new StartManualReshardingCommand(_fromBucket, _toBucket, _toShard);
        }

        public sealed class StartManualReshardingCommand : RavenCommand
        {
            private readonly long _fromBucket;
            private readonly long _toBucket;
            private readonly int _toShard;

            public StartManualReshardingCommand(long fromBucket, long toBucket, int toShard)
            {
                _fromBucket = fromBucket;
                _toBucket = toBucket;
                _toShard = toShard;
            }

            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                url = $"{node.Url}/admin/resharding/start?fromBucket={_fromBucket}&toBucket={_toBucket}&toShard={_toShard}&database={node.Database}";

                return new HttpRequestMessage
                {
                    Method = HttpMethod.Post
                };
            }
        }
    }
}

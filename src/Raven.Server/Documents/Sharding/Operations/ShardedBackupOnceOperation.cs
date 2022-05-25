using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.Http;
using Raven.Client.Json;
using Raven.Server.Documents.Sharding.Handlers;
using Sparrow.Json;

namespace Raven.Server.Documents.Sharding.Operations
{
    public class ShardedBackupOnceOperation : ShardedBackupOperationBase, IShardedOperation
    {
        private readonly BackupConfiguration _configuration;

        public ShardedBackupOnceOperation(ShardedDatabaseRequestHandler handler, BackupConfiguration configuration) : base(handler)
        {
            _configuration = configuration;
        }

        public RavenCommand<object> CreateCommandForShard(int shardNumber)
        {
            AddOperationFor(shardNumber);
            return new ShardedBackupOnceCommand(_configuration, QueryString);
        }

        private class ShardedBackupOnceCommand : ShardedBackupNowCommand
        {
            private readonly BackupConfiguration _configuration;

            public ShardedBackupOnceCommand(BackupConfiguration configuration, string queryString) : base(queryString)
            {
                _configuration = configuration;
            }

            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                var request = base.CreateRequest(ctx, node, out url);

                var blittable = DocumentConventions.Default.Serialization.DefaultConverter.ToBlittable(_configuration, ctx);
                request.Content = new BlittableJsonContent(async stream => await blittable.WriteJsonToAsync(stream));

                return request;
            }
        }
    }
}

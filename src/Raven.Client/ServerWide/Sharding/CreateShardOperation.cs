using System;
using System.Net.Http;
using System.Text;
using Raven.Client.Documents.Conventions;
using Raven.Client.Http;
using Raven.Client.Json.Serialization;
using Raven.Client.ServerWide.Operations;
using Raven.Client.Util;
using Sparrow.Json;

namespace Raven.Client.ServerWide.Sharding
{
    internal class CreateShardOperation : IServerOperation<CreateShardResult>
    {
        private readonly string _databaseName;
        private readonly int? _shardNumber;
        private readonly string[] _nodes;
        private readonly int? _replicationFactor;

        public CreateShardOperation(string databaseName, int? shardNumber = null, string[] nodes = null, int? replicationFactor = null)
        {
            ResourceNameValidator.AssertValidDatabaseName(databaseName);
            _databaseName = databaseName;
            _shardNumber = shardNumber;
            _nodes = nodes;
            _replicationFactor = replicationFactor;
        }

        public RavenCommand<CreateShardResult> GetCommand(DocumentConventions conventions, JsonOperationContext context)
        {
            return new CreateShardCommand(_databaseName, _shardNumber, _nodes, _replicationFactor);
        }

        internal class CreateShardCommand : RavenCommand<CreateShardResult>, IRaftCommand
        {
            private readonly string _databaseName;
            private readonly int? _shardNumber;
            private readonly string[] _nodes;
            private readonly int? _replicationFactor;

            public CreateShardCommand(string databaseName, int? shardNumber = null, string[] nodes = null, int? replicationFactor = null)
            {
                _databaseName = databaseName;
                _shardNumber = shardNumber;
                _nodes = nodes;
                _replicationFactor = replicationFactor;
            }

            public override bool IsReadRequest => false;

            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                var sb = new StringBuilder($"{node.Url}/admin/databases/shard?databaseName={Uri.EscapeDataString(_databaseName)}");

                if (_shardNumber.HasValue)
                    sb = sb.Append($"&shardNumber={_shardNumber}");

                if (_replicationFactor.HasValue)
                    sb.Append($"&replicationFactor={_replicationFactor}");

                if (_nodes?.Length > 0)
                {
                    foreach (var nodeStr in _nodes)
                    {
                        sb.Append("&node=").Append(Uri.EscapeDataString(nodeStr));
                    }
                }
                var request = new HttpRequestMessage
                {
                    Method = HttpMethod.Put
                };

                url = sb.ToString();
                return request;
            }

            public override void SetResponse(JsonOperationContext context, BlittableJsonReaderObject response, bool fromCache)
            {
                if (response == null)
                    ThrowInvalidResponse();

                Result = JsonDeserializationClient.CreateShardResult(response);
            }

            public string RaftUniqueRequestId { get; } = RaftIdGenerator.NewId();
        }
    }

    public class CreateShardResult
    {
        public string DatabaseName { get; set; }
        public int NewShardNumber { get; set; }
        public DatabaseTopology NewShardTopology { get; set; }
        public long RaftCommandIndex { get; set; }
    }
}

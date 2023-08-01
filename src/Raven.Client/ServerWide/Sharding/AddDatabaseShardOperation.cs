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
    internal sealed class AddDatabaseShardOperation : IServerOperation<AddDatabaseShardResult>
    {
        private readonly string _databaseName;
        private readonly int? _shardNumber;
        private readonly string[] _nodes;
        private readonly int? _replicationFactor;

        public AddDatabaseShardOperation(string databaseName, int? shardNumber = null)
        {
            ResourceNameValidator.AssertValidDatabaseName(databaseName);
            _databaseName = databaseName;
            _shardNumber = shardNumber;
        }

        public AddDatabaseShardOperation(string databaseName, string[] nodes, int? shardNumber = null) : this(databaseName, shardNumber)
        {
            _nodes = nodes;
        }

        public AddDatabaseShardOperation(string databaseName, int? replicationFactor, int? shardNumber = null) : this(databaseName, shardNumber)
        {
            _replicationFactor = replicationFactor;
        }

        public RavenCommand<AddDatabaseShardResult> GetCommand(DocumentConventions conventions, JsonOperationContext context)
        {
            return new AddDatabaseShardCommand(_databaseName, _shardNumber, _nodes, _replicationFactor);
        }

        internal sealed class AddDatabaseShardCommand : RavenCommand<AddDatabaseShardResult>, IRaftCommand
        {
            private readonly string _databaseName;
            private readonly int? _shardNumber;
            private readonly string[] _nodes;
            private readonly int? _replicationFactor;

            public AddDatabaseShardCommand(string databaseName, int? shardNumber = null, string[] nodes = null, int? replicationFactor = null)
            {
                _databaseName = databaseName;
                _shardNumber = shardNumber;
                _nodes = nodes;
                _replicationFactor = replicationFactor;
            }

            public override bool IsReadRequest => false;

            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                var sb = new StringBuilder($"{node.Url}/admin/databases/shard?name={Uri.EscapeDataString(_databaseName)}");

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

                Result = JsonDeserializationClient.AddDatabaseShardResult(response);
            }

            public string RaftUniqueRequestId { get; } = RaftIdGenerator.NewId();
        }
    }

    public sealed class AddDatabaseShardResult
    {
        public string Name { get; set; }
        public int ShardNumber { get; set; }
        public DatabaseTopology ShardTopology { get; set; }
        public long RaftCommandIndex { get; set; }
    }
}

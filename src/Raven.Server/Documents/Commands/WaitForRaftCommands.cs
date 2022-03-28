using System.Collections.Generic;
using System.Net.Http;
using Raven.Client.Http;
using Raven.Client.Json;
using Raven.Server.Monitoring.Snmp.Objects.Database;
using Raven.Server.Utils;
using Sparrow.Json;

namespace Raven.Server.Documents.Commands
{
    public class WaitForRaftCommands : RavenCommand
    {
        private readonly List<long> _indexes;
        private readonly int? _shardNumber;

        public WaitForRaftCommands(List<long> indexes, int? shardNumber = null)
        {
            _indexes = indexes;
            _shardNumber = shardNumber;
        }

        public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
        {
            var databaseName = node.Database;
            if (_shardNumber.HasValue)
                databaseName = ShardHelper.ToShardName(node.Database, _shardNumber.Value);

            url = $"{node.Url}/databases/{databaseName}/admin/rachis/wait-for-raft-commands";

            var request = new HttpRequestMessage
            {
                Method = HttpMethod.Post,
                Content = new BlittableJsonContent(async stream =>
                {
                    await using (var writer = new AsyncBlittableJsonTextWriter(ctx, stream))
                    {
                        writer.WriteStartObject();
                        writer.WriteArray(nameof(WaitForCommandsRequest.RaftCommandIndexes), _indexes);
                        writer.WriteEndObject();
                    }
                })
            };

            return request;
        }
    }

    public class WaitForCommandsRequest
    {
        public List<long> RaftCommandIndexes { get; set; }
    }
}

using System.Net.Http;
using Raven.Client.Http;
using Raven.Client.Json.Converters;
using Raven.Client.ServerWide.Commands;
using Sparrow.Json;

namespace Raven.Client.Documents.Commands
{
    public class GetPullReplicationInfoCommand : RavenCommand<TcpConnectionInfo[]>
    {
        private readonly string _remoteDatabase;
        private readonly string _databaseGroupId;
        private readonly string _name;

        public GetPullReplicationInfoCommand(string remoteDatabase, string databaseGroupId, string name)
        {
            _remoteDatabase = remoteDatabase;
            _databaseGroupId = databaseGroupId;
            _name = name;
        }

        public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
        {
            url = $"{node.Url}/info/tcp/pull?databaseName={_remoteDatabase}&definitionName={_name}&databaseGroupId={_databaseGroupId}";

            RequestedNode = node;
            var request = new HttpRequestMessage
            {
                Method = HttpMethod.Get
            };
            return request;
        }

        public override void SetResponse(JsonOperationContext context, BlittableJsonReaderObject response, bool fromCache)
        {
            BlittableJsonReaderArray array = null;
            if (response == null || response.TryGet("Results", out array) == false)
                ThrowInvalidResponse();
            if (array == null)
                ThrowInvalidResponse();

            Result = new TcpConnectionInfo[array.Length];
            for (var index = 0; index < array.Length; index++)
            {
                var item = (BlittableJsonReaderObject)array[index];
                Result[index] = JsonDeserializationClient.TcpConnectionInfo(item);
            }
        }

        public ServerNode RequestedNode { get; private set; }

        public override bool IsReadRequest => true;
    }

}

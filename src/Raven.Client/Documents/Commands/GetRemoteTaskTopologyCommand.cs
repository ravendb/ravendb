using System;
using System.Net.Http;
using Raven.Client.Http;
using Sparrow.Json;

namespace Raven.Client.Documents.Commands
{
    internal class GetRemoteTaskTopologyCommand : RavenCommand<string[]>
    {
        private readonly string _remoteDatabase;
        private readonly string _databaseGroupId;
        private readonly string _remoteTask;

        public GetRemoteTaskTopologyCommand(string remoteDatabase, string databaseGroupId, string remoteTask)
        {
            _remoteDatabase = remoteDatabase ?? throw new ArgumentNullException(nameof(remoteDatabase));
            _databaseGroupId = databaseGroupId ?? throw new ArgumentNullException(nameof(databaseGroupId));
            _remoteTask = remoteTask ?? throw new ArgumentNullException(nameof(remoteTask));
        }

        public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
        {
            url = $"{node.Url}/info/remote-task/topology?database={_remoteDatabase}&remote-task={_remoteTask}&groupId={_databaseGroupId}";

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
            if (response == null || response.TryGet("Results", out array) == false || array == null)
                ThrowInvalidResponse();

            Result = new string[array.Length];
            for (var index = 0; index < array.Length; index++)
            {
                Result[index] = array[index].ToString();
            }
        }

        public ServerNode RequestedNode { get; private set; }

        public override bool IsReadRequest => true;
    }

}

using System.Net.Http;
using Raven.Client.Http;
using Raven.Client.Json.Converters;
using Raven.Client.Util;
using Sparrow.Json;

namespace Raven.Client.Server.Commands
{   
    public class IsDatabaseLoadedCommand : RavenCommand<IsDatabaseLoadedCommand.CommandResult>
    {
        public class CommandResult
        {
            public string DatabaseName;
            public bool IsLoaded;
        }

        public override bool IsReadRequest => true;

        public override HttpRequestMessage CreateRequest(ServerNode node, out string url)
        {
            //not sure if we need to escape database name here
            url = $"{node.Url}/admin/databases/is-loaded?name={node.Database}";
            var request = new HttpRequestMessage
            {
                Method = HttpMethod.Get,
            };
            return request;
        }

        public override void SetResponse(BlittableJsonReaderObject response, bool fromCache)
        {
            if (response == null)
                ThrowInvalidResponse();

            Result = JsonDeserializationClient.IsDatabaseLoadedCommandResult(response);
        }
    }
}

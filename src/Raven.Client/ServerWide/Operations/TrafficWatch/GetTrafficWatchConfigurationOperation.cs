using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Http;
using Raven.Client.Json.Serialization;
using Sparrow.Json;

namespace Raven.Client.ServerWide.Operations.TrafficWatch;

public class GetTrafficWatchConfigurationOperation : IServerOperation<PutTrafficWatchConfigurationOperation.Parameters>
{
    public RavenCommand<PutTrafficWatchConfigurationOperation.Parameters> GetCommand(DocumentConventions conventions, JsonOperationContext context)
    {
        return new GetTrafficWatchConfigurationCommand();
    }

    public class GetTrafficWatchConfigurationCommand : RavenCommand<PutTrafficWatchConfigurationOperation.Parameters>
    {
        public override bool IsReadRequest => true;
        public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
        {
            url = $"{node.Url}/admin/traffic-watch/configuration";

            return new HttpRequestMessage(HttpMethod.Get, url);
        }

        public override void SetResponse(JsonOperationContext context, BlittableJsonReaderObject response, bool fromCache)
        {
            if (response == null)
                ThrowInvalidResponse();

            Result = JsonDeserializationClient.GetTrafficWatchConfigurationResult(response);
        }
    }
}
public enum TrafficWatchMode
{
    None = 0,
    Off,
    ToLogFile
}

using System.Net.Http;
using Raven.Client.Http;
using Sparrow.Json;

namespace Tests.Infrastructure.Commands;

public class GetSnmpOidsCommand : RavenCommand<object>
{
    public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
    {
        url = $"{node.Url}/monitoring/snmp/oids";

        return new HttpRequestMessage
        {
            Method = HttpMethod.Get
        };
    }

    public override void SetResponse(JsonOperationContext context, BlittableJsonReaderObject response, bool fromCache)
    {
        if (response == null)
            ThrowInvalidResponse();

        Result = response;
    }

    public override bool IsReadRequest => true;
}

public class SnmpEntry
{
    public string OID { get; set; }
    public string Description { get; set; }
}

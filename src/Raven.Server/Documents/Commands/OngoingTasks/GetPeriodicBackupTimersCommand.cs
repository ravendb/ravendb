using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using Raven.Client.Http;
using Raven.Server.Documents.PeriodicBackup;
using Raven.Server.Json;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.Commands.OngoingTasks;

internal class GetPeriodicBackupTimersCommand : RavenCommand<GetPeriodicBackupTimersCommand.PeriodicBackupTimersResponse>
{
    public class PeriodicBackupTimersResponse
    {
        public List<PeriodicBackupInfo> Timers { get; set; } = new();

        public int Count { get; set; }

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(Timers)] = new DynamicJsonArray(Timers.Select(x => x.ToJson())),
                [nameof(Count)] = Count
            };
        }
    }

    public GetPeriodicBackupTimersCommand(string nodeTag)
    {
        SelectedNodeTag = nodeTag;
    }

    public override bool IsReadRequest => true;

    public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
    {
        url = $"{node.Url}/databases/{node.Database}/admin/debug/periodic-backup/timers";

        return new HttpRequestMessage
        {
            Method = HttpMethod.Get
        };
    }

    public override void SetResponse(JsonOperationContext context, BlittableJsonReaderObject response, bool fromCache)
    {
        if (response == null)
            ThrowInvalidResponse();

        Result = JsonDeserializationServer.GetPeriodicBackupTimersCommandResponse(response);
    }
}

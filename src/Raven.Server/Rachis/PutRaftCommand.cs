using System;
using System.Linq;
using System.Net.Http;
using Raven.Client.Http;
using Raven.Client.Json;
using Raven.Client.Util;
using Sparrow.Json;

namespace Raven.Server.Rachis;

public class PutRaftCommand : RavenCommand<PutRaftCommand.PutRaftCommandResult>, IRaftCommand
{
    private readonly BlittableJsonReaderObject _command;
    private bool _reachedLeader;
    public override bool IsReadRequest => false;

    public bool HasReachLeader() => _reachedLeader;

    private readonly string _source;
    private readonly string _commandType;

    public PutRaftCommand(BlittableJsonReaderObject command, string source, string commandType)
    {
        _command = command;
        _source = source;
        _commandType = commandType;
    }

    public override void OnResponseFailure(HttpResponseMessage response)
    {
        if (response.Headers.Contains("Reached-Leader") == false)
            return;
        _reachedLeader = response.Headers.GetValues("Reached-Leader").Contains("true");
    }

    public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
    {
        url = $"{node.Url}/admin/rachis/send?source={_source}&commandType={_commandType}";
        var request = new HttpRequestMessage
        {
            Method = HttpMethod.Post,
            Content = new BlittableJsonContent(async stream =>
            {
                await using (var writer = new AsyncBlittableJsonTextWriter(ctx, stream))
                {
                    writer.WriteObject(_command);
                }
            })
        };

        return request;
    }

    public override void SetResponse(JsonOperationContext context, BlittableJsonReaderObject response, bool fromCache)
    {
        Result = PutRaftCommandResultFunc(response);
    }

    public string RaftUniqueRequestId { get; } = RaftIdGenerator.NewId();

    public class PutRaftCommandResult
    {
        public long RaftCommandIndex { get; set; }

        public object Data { get; set; }
    }

    private static readonly Func<BlittableJsonReaderObject, PutRaftCommandResult> PutRaftCommandResultFunc = JsonDeserializationBase.GenerateJsonDeserializationRoutine<PutRaftCommandResult>();
}

using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Http;
using Raven.Client.Json.Converters;
using Sparrow.Json;

namespace Raven.Client.ServerWide.Operations
{
    public class ToggleTaskStateOperation : IServerOperation
    {
        private readonly long _taskId;
        private readonly OngoingTaskType _type;
        private readonly bool _disable;

        public ToggleTaskStateOperation(long taskId, OngoingTaskType type, bool disable)
        {
            _taskId = taskId;
            _type = type;
            _disable = disable;
        }

        public RavenCommand GetCommand(DocumentConventions conventions, JsonOperationContext ctx)
        {
            return new ToggleTaskStateCommand(_taskId, _type, _disable);
        }

        private class ToggleTaskStateCommand : RavenCommand
        {
            private readonly long _taskId;
            private readonly OngoingTaskType _type;
            private readonly bool _disable;

            public ToggleTaskStateCommand(long taskId, OngoingTaskType type, bool disable)
            {
                _taskId = taskId;
                _type = type;
                _disable = disable;
            }

            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                url = $"{node.Url}/databases/{node.Database}/admin/tasks/state?key={_taskId}&type={_type}&disable={_disable}";

                var request = new HttpRequestMessage
                {
                    Method = HttpMethod.Post
                };

                return request;
            }

            public override void SetResponse(BlittableJsonReaderObject response, bool fromCache)
            {
                if (response != null)
                    Result = JsonDeserializationClient.ModifyOngoingTaskResult(response);
            }

            public override bool IsReadRequest => false;
        }
    }

}

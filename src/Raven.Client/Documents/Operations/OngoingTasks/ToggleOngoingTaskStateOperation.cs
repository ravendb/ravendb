using System;
using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Http;
using Raven.Client.Json.Converters;
using Sparrow.Json;

namespace Raven.Client.Documents.Operations.OngoingTasks
{
    public class ToggleOngoingTaskStateOperation : IMaintenanceOperation
    {
        private readonly long _taskId;
        private readonly string _taskName;
        private readonly OngoingTaskType _type;
        private readonly bool _disable;

        public ToggleOngoingTaskStateOperation(long taskId, OngoingTaskType type, bool disable)
        {
            _taskId = taskId;
            _type = type;
            _disable = disable;
        }

        internal ToggleOngoingTaskStateOperation(string taskName, OngoingTaskType type, bool disable)
        {
            if (string.IsNullOrWhiteSpace(taskName)) 
                throw new ArgumentException("Value cannot be null or whitespace.", nameof(taskName));

            _taskName = taskName;
            _type = type;
            _disable = disable;
        }

        public RavenCommand GetCommand(DocumentConventions conventions, JsonOperationContext ctx)
        {
            return new ToggleTaskStateCommand(_taskId, _taskName, _type, _disable);
        }

        private class ToggleTaskStateCommand : RavenCommand
        {
            private readonly long _taskId;
            private readonly string _taskName;
            private readonly OngoingTaskType _type;
            private readonly bool _disable;

            public ToggleTaskStateCommand(long taskId, string taskName, OngoingTaskType type, bool disable)
            {
                _taskId = taskId;
                _taskName = taskName;
                _type = type;
                _disable = disable;
            }

            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                url = $"{node.Url}/databases/{node.Database}/admin/tasks/state?key={_taskId}&type={_type}&disable={_disable}";

                if (_taskName != null)
                    url += $"&taskName={_taskName}";

                var request = new HttpRequestMessage
                {
                    Method = HttpMethod.Post
                };

                return request;
            }

            public override void SetResponse(JsonOperationContext context, BlittableJsonReaderObject response, bool fromCache)
            {
                if (response != null)
                    Result = JsonDeserializationClient.ModifyOngoingTaskResult(response);
            }

            public override bool IsReadRequest => false;
        }
    }

}

using System;
using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Operations.OngoingTasks;
using Raven.Client.Http;
using Raven.Client.Util;
using Sparrow.Json;

namespace Raven.Client.ServerWide.Operations.OngoingTasks
{
    public class DeleteServerWideTaskOperation : IServerOperation
    {
        private readonly string _name;
        private readonly OngoingTaskType _type;

        public DeleteServerWideTaskOperation(string name, OngoingTaskType type)
        {
            _name = name ?? throw new ArgumentNullException(nameof(name));
            _type = type;
        }

        public RavenCommand GetCommand(DocumentConventions conventions, JsonOperationContext context)
        {
            return new DeleteServerWideTaskCommand(_name, _type);
        }

        private class DeleteServerWideTaskCommand : RavenCommand, IRaftCommand
        {
            private readonly string _name;
            private readonly OngoingTaskType _type;

            public DeleteServerWideTaskCommand(string name, OngoingTaskType type)
            {
                _name = name ?? throw new ArgumentNullException(nameof(name));
                _type = type;
            }

            public override bool IsReadRequest => false;

            public string RaftUniqueRequestId { get; } = RaftIdGenerator.NewId();

            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                url = $"{node.Url}/admin/configuration/server-wide/task?type={_type}&name={Uri.EscapeDataString(_name)}";

                return new HttpRequestMessage
                {
                    Method = HttpMethod.Delete
                };
            }
        }
    }
}

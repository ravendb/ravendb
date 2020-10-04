using System;
using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Http;
using Raven.Client.ServerWide.Operations.OngoingTasks;
using Raven.Client.Util;
using Sparrow.Json;

namespace Raven.Client.ServerWide.Operations.Configuration
{
    [Obsolete("Please use " + nameof(DeleteServerWideTaskOperation) + " instead")]
    public class DeleteServerWideBackupConfigurationOperation : IServerOperation
    {
        private readonly string _name;

        public DeleteServerWideBackupConfigurationOperation(string name)
        {
            _name = name ?? throw new ArgumentNullException(nameof(name));
        }

        public RavenCommand GetCommand(DocumentConventions conventions, JsonOperationContext context)
        {
            return new DeleteServerWideBackupConfigurationCommand(_name);
        }

        private class DeleteServerWideBackupConfigurationCommand : RavenCommand, IRaftCommand
        {
            private readonly string _name;

            public DeleteServerWideBackupConfigurationCommand(string name)
            {
                _name = name ?? throw new ArgumentNullException(nameof(name));
            }

            public override bool IsReadRequest => false;

            public string RaftUniqueRequestId { get; } = RaftIdGenerator.NewId();

            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                url = $"{node.Url}/admin/configuration/server-wide/backup?name={Uri.EscapeDataString(_name)}";

                return new HttpRequestMessage
                {
                    Method = HttpMethod.Delete
                };
            }
        }
    }
}

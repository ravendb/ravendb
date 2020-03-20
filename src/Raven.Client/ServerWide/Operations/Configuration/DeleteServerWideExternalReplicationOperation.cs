using System;
using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Http;
using Raven.Client.Util;
using Sparrow.Json;

namespace Raven.Client.ServerWide.Operations.Configuration
{
    public class DeleteServerWideExternalReplicationOperation : IServerOperation
    {
        private readonly string _name;

        public DeleteServerWideExternalReplicationOperation(string name)
        {
            _name = name ?? throw new ArgumentNullException(nameof(name));
        }

        public RavenCommand GetCommand(DocumentConventions conventions, JsonOperationContext context)
        {
            return new DeleteServerWideExternalReplicationCommand(_name);
        }

        private class DeleteServerWideExternalReplicationCommand : RavenCommand, IRaftCommand
        {
            private readonly string _name;

            public DeleteServerWideExternalReplicationCommand(string name)
            {
                _name = name ?? throw new ArgumentNullException(nameof(name));
            }

            public override bool IsReadRequest => false;

            public string RaftUniqueRequestId { get; } = RaftIdGenerator.NewId();

            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                url = $"{node.Url}/admin/configuration/server-wide/external-replication?name={Uri.EscapeDataString(_name)}";

                return new HttpRequestMessage
                {
                    Method = HttpMethod.Delete
                };
            }
        }
    }
}

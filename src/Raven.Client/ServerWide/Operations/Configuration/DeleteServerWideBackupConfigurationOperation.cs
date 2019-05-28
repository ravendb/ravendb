using System;
using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Http;
using Sparrow.Json;

namespace Raven.Client.ServerWide.Operations.Configuration
{
    public class DeleteServerWideBackupConfigurationOperation : IServerOperation
    {
        private readonly string _name;

        public DeleteServerWideBackupConfigurationOperation(string name)
        {
            _name = name ?? throw new ArgumentNullException(nameof(name));
        }

        public RavenCommand GetCommand(DocumentConventions conventions, JsonOperationContext context)
        {
            return new PutServerWideClientConfigurationCommand(_name);
        }

        private class PutServerWideClientConfigurationCommand : RavenCommand
        {
            private readonly string _name;

            public PutServerWideClientConfigurationCommand(string name)
            {
                _name = name ?? throw new ArgumentNullException(nameof(name));
            }

            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                url = $"{node.Url}/admin/configuration/server-wide/backup?name={_name}";

                return new HttpRequestMessage
                {
                    Method = HttpMethod.Delete
                };
            }
        }
    }
}

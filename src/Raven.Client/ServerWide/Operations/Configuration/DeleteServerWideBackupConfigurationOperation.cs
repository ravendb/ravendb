using System;
using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Http;
using Sparrow.Json;

namespace Raven.Client.ServerWide.Operations.Configuration
{
    public class DeleteServerWideBackupConfigurationOperation : IServerOperation
    {
        public DeleteServerWideBackupConfigurationOperation()
        {
        }

        public RavenCommand GetCommand(DocumentConventions conventions, JsonOperationContext context)
        {
            return new PutServerWideClientConfigurationCommand(conventions, context);
        }

        private class PutServerWideClientConfigurationCommand : RavenCommand
        {
            public PutServerWideClientConfigurationCommand(DocumentConventions conventions, JsonOperationContext context)
            {
                if (conventions == null)
                    throw new ArgumentNullException(nameof(conventions));
                if (context == null)
                    throw new ArgumentNullException(nameof(context));
            }

            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                url = $"{node.Url}/admin/configuration/server-wide/backup";

                return new HttpRequestMessage
                {
                    Method = HttpMethod.Delete
                };
            }
        }
    }
}

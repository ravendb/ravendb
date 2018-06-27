using System;
using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Session;
using Raven.Client.Http;
using Raven.Client.Json;
using Sparrow.Json;
using Sparrow.Logging;

namespace Raven.Client.ServerWide.Operations.Logs
{
    public class SetLogsConfigurationOperation : IServerOperation
    {
        private readonly Parameters _parameters;

        public class Parameters
        {
            public LogMode Mode { get; set; }
        }

        public SetLogsConfigurationOperation(Parameters parameters)
        {
            _parameters = parameters ?? throw new ArgumentNullException(nameof(parameters));
        }

        public RavenCommand GetCommand(DocumentConventions conventions, JsonOperationContext context)
        {
            return new SetLogsConfigurationCommand(_parameters);
        }

        private class SetLogsConfigurationCommand : RavenCommand
        {
            private readonly Parameters _parameters;

            public SetLogsConfigurationCommand(Parameters parameters)
            {
                _parameters = parameters ?? throw new ArgumentNullException(nameof(parameters));
            }

            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                url = $"{node.Url}/admin/logs/configuration";

                return new HttpRequestMessage(HttpMethod.Post, url)
                {
                    Content = new BlittableJsonContent(stream =>
                    {
                        ctx.Write(stream, EntityToBlittable.ConvertCommandToBlittable(_parameters, ctx));
                    })
                };
            }
        }
    }
}

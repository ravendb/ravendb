using System;
using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Http;
using Raven.Client.Json;
using Sparrow;
using Sparrow.Json;
using Sparrow.Logging;

namespace Raven.Client.ServerWide.Operations.Logs
{
    public sealed class SetLogsConfigurationOperation : IServerOperation
    {
        private readonly Parameters _parameters;

        public sealed class Parameters
        {
            public LogMode Mode { get; set; }
            public TimeSpan? RetentionTime { get; set; }
            public Size? RetentionSize { get; set; }
            public bool Compress { get; set; }

            public Parameters()
            {
            }

            public Parameters(GetLogsConfigurationResult currentLogsConfiguration)
            {
                Mode = currentLogsConfiguration.Mode;
                RetentionTime = currentLogsConfiguration.RetentionTime;
                RetentionSize = currentLogsConfiguration.RetentionSize;
                Compress = currentLogsConfiguration.Compress;
            }
        }

        public SetLogsConfigurationOperation(Parameters parameters)
        {
            _parameters = parameters ?? throw new ArgumentNullException(nameof(parameters));
        }

        public RavenCommand GetCommand(DocumentConventions conventions, JsonOperationContext context)
        {
            return new SetLogsConfigurationCommand(conventions, _parameters);
        }

        private class SetLogsConfigurationCommand : RavenCommand
        {
            private readonly DocumentConventions _conventions;
            private readonly Parameters _parameters;

            public SetLogsConfigurationCommand(DocumentConventions conventions, Parameters parameters)
            {
                _conventions = conventions ?? throw new ArgumentNullException(nameof(conventions));
                _parameters = parameters ?? throw new ArgumentNullException(nameof(parameters));
            }

            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                url = $"{node.Url}/admin/logs/configuration";

                return new HttpRequestMessage(HttpMethod.Post, url)
                {
                    Content = new BlittableJsonContent(async stream => await ctx.WriteAsync(stream, DocumentConventions.Default.Serialization.DefaultConverter.ToBlittable(_parameters, ctx)).ConfigureAwait(false), _conventions)
                };
            }
        }
    }
}

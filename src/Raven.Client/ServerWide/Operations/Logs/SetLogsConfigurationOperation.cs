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
    public class SetLogsConfigurationOperation : IServerOperation
    {
        private readonly Parameters _parameters;
        private readonly bool _persist;

        public class Parameters
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
            : this(parameters, false)
        {
        }
        
        public SetLogsConfigurationOperation(Parameters parameters, bool persist)
        {
            _parameters = parameters ?? throw new ArgumentNullException(nameof(parameters));
            _persist = persist;
        }

        public RavenCommand GetCommand(DocumentConventions conventions, JsonOperationContext context)
        {
            return new SetLogsConfigurationCommand(_parameters, _persist);
        }

        private class SetLogsConfigurationCommand : RavenCommand
        {
            private readonly Parameters _parameters;
            private readonly bool? _persist;

            public SetLogsConfigurationCommand(Parameters parameters, bool persist)
            {
                _parameters = parameters ?? throw new ArgumentNullException(nameof(parameters));
                _persist = persist;
            }

            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                url = $"{node.Url}/admin/logs/configuration?persist={_persist}";

                return new HttpRequestMessage(HttpMethod.Post, url)
                {
                    Content = new BlittableJsonContent(async stream => await ctx.WriteAsync(stream, DocumentConventions.Default.Serialization.DefaultConverter.ToBlittable(_parameters, ctx)).ConfigureAwait(false))
                };
            }
        }
    }
}

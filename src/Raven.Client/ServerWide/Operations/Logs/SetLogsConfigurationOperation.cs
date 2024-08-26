using System;
using System.Collections.Generic;
using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Http;
using Raven.Client.Json;
using Sparrow.Json;
using Sparrow.Logging;

namespace Raven.Client.ServerWide.Operations.Logs
{
    public sealed class SetLogsConfigurationOperation : IServerOperation
    {
        private readonly Parameters _parameters;

        public sealed class LogsConfiguration
        {
            public LogLevel MinLevel { get; set; }

            public LogLevel MaxLevel { get; set; }

            public List<LogFilter> Filters { get; set; }

            public LogFilterAction LogFilterDefaultAction { get; set; }
        }

        public sealed class MicrosoftLogsConfiguration
        {
            public LogLevel MinLevel { get; set; }
        }

        public sealed class AdminLogsConfiguration
        {
            public LogLevel MinLevel { get; set; }

            public LogLevel MaxLevel { get; set; }

            public List<LogFilter> Filters { get; set; }

            public LogFilterAction LogFilterDefaultAction { get; set; }
        }

        internal sealed class Parameters
        {
            public LogsConfiguration Logs { get; set; }

            public MicrosoftLogsConfiguration MicrosoftLogs { get; set; }

            public AdminLogsConfiguration AdminLogs { get; set; }

            public bool Persist { get; set; }
        }

        public SetLogsConfigurationOperation(LogsConfiguration configuration)
        {
            if (configuration == null)
                throw new ArgumentNullException(nameof(configuration));

            _parameters = new Parameters { Logs = configuration };
        }

        public SetLogsConfigurationOperation(MicrosoftLogsConfiguration configuration)
        {
            if (configuration == null)
                throw new ArgumentNullException(nameof(configuration));

            _parameters = new Parameters { MicrosoftLogs = configuration };
        }

        public SetLogsConfigurationOperation(AdminLogsConfiguration configuration)
        {
            if (configuration == null)
                throw new ArgumentNullException(nameof(configuration));

            _parameters = new Parameters { AdminLogs = configuration };
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

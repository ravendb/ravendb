using System;
using System.Collections.Generic;
using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Operations;
using Raven.Client.Http;
using Raven.Client.Json;
using Raven.Client.Util;
using Sparrow.Json;

namespace Raven.Client.ServerWide.Operations.Configuration
{
    public sealed class PutDatabaseSettingsOperation : IMaintenanceOperation
    {
        private readonly string _databaseName;
        private readonly Dictionary<string, string> _configurationSettings;

        public PutDatabaseSettingsOperation(string databaseName, Dictionary<string, string> configurationSettings)
        {
            _databaseName = databaseName ?? throw new ArgumentNullException(nameof(configurationSettings));
            _configurationSettings = configurationSettings ?? throw new ArgumentNullException(nameof(configurationSettings));
        }

        public RavenCommand GetCommand(DocumentConventions conventions, JsonOperationContext context)
        {
            return new PutDatabaseConfigurationSettingsCommand(conventions, context, _configurationSettings, _databaseName);
        }

        private sealed class PutDatabaseConfigurationSettingsCommand : RavenCommand, IRaftCommand
        {
            public override bool IsReadRequest => false;
            public string RaftUniqueRequestId { get; } = RaftIdGenerator.NewId();
            private readonly BlittableJsonReaderObject _configurationSettings;
            private readonly DocumentConventions _conventions;
            private readonly string _databaseName;

            public PutDatabaseConfigurationSettingsCommand(DocumentConventions conventions, JsonOperationContext context, Dictionary<string, string> configurationSettings, string databaseName)
            {
                if (context is null)
                    throw new ArgumentNullException(nameof(context));
                if (configurationSettings is null)
                    throw new ArgumentNullException(nameof(configurationSettings));
                _conventions = conventions ?? throw new ArgumentNullException(nameof(conventions));
                _databaseName = databaseName ?? throw new ArgumentNullException(nameof(databaseName));
                _configurationSettings = DocumentConventions.Default.Serialization.DefaultConverter.ToBlittable(configurationSettings, context);
            }

            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                url = $"{node.Url}/databases/{_databaseName}/admin/configuration/settings";
                var request = new HttpRequestMessage
                {
                    Method = HttpMethod.Put,
                    Content = new BlittableJsonContent(async stream => await ctx.WriteAsync(stream, _configurationSettings).ConfigureAwait(false), _conventions)
                };
                return request;
            }
        }
    }
}

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
    /// <summary>
    /// Modifies the default database configuration using the PutDatabaseSettingsOperation.
    /// <para><strong>Notes:</strong> </para>
    /// <list type="bullet">
    /// <item>
    /// <description>Only database-level settings can be customized with this operation.</description>
    /// </item>
    /// <item>
    /// <description>For changes to take effect, the database must be reloaded. Reloading can be done by disabling and enabling the database using the ToggleDatabasesStateOperation.</description>
    /// </item>
    /// </list>
    /// </summary>
    public sealed class PutDatabaseSettingsOperation : IMaintenanceOperation
    {
        private readonly string _databaseName;
        private readonly Dictionary<string, string> _configurationSettings;

        /// <inheritdoc cref="PutDatabaseSettingsOperation" />
        /// <param name="databaseName">The name of the database for which the settings are being modified.</param>
        /// <param name="configurationSettings">A dictionary of configuration settings to be applied to the database.</param>
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

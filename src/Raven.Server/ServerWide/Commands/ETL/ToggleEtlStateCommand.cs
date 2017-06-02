using System;
using System.Collections.Generic;
using Raven.Client.Server;
using Raven.Client.Server.ETL;
using Sparrow.Json.Parsing;

namespace Raven.Server.ServerWide.Commands.ETL
{
    public class ToggleEtlStateCommand : UpdateDatabaseCommand
    {
        private readonly string ConfigurationName;

        private readonly EtlType EtlType;

        public ToggleEtlStateCommand() : base(null)
        {
            // for deserialization
        }

        public ToggleEtlStateCommand(string configurationName, EtlType etlType, string databaseName) : base(databaseName)
        {
            ConfigurationName = configurationName;
            EtlType = etlType;
        }

        public override string UpdateDatabaseRecord(DatabaseRecord record, long etag)
        {
            switch (EtlType)
            {
                case EtlType.Raven:
                    ToggleState(record.RavenEtls);
                    return null;
                case EtlType.Sql:
                    ToggleState(record.SqlEtls);
                    return null;
                default:
                    throw new NotSupportedException($"Unknown ETL configuration type: {EtlType}");
            }
        }

        private void ToggleState<T>(List<EtlConfiguration<T>> etls) where T : EtlDestination
        {
            if (etls == null)
                ThrowNoEtlsDefined(EtlType, ConfigurationName);

            var config = etls.Find(x => x.Destination.Name.Equals(ConfigurationName, StringComparison.OrdinalIgnoreCase));

            if (config == null)
                ThrowConfigurationNotFound(ConfigurationName);

            config.Disabled = !config.Disabled;
        }

        private static void ThrowConfigurationNotFound(string configurationName)
        {
            throw new InvalidOperationException($"Configuration was not found: '{configurationName}'");
        }

        private static void ThrowNoEtlsDefined(EtlType type, string configurationName)
        {
            throw new InvalidOperationException($"There is no {type} ETL defined so we cannot delete '{configurationName}' configuration");
        }

        public override void FillJson(DynamicJsonValue json)
        {
            json[nameof(ConfigurationName)] = ConfigurationName;
            json[nameof(EtlType)] = EtlType;
        }
    }
}
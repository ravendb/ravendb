using System;
using System.Collections.Generic;
using Raven.Client.Server;
using Raven.Client.Server.ETL;
using Sparrow.Json.Parsing;

namespace Raven.Server.ServerWide.Commands.ETL
{
    public class DeleteEtlCommand : UpdateDatabaseCommand
    {
        public readonly string ConfigurationName;

        public readonly EtlType EtlType;

        public DeleteEtlCommand() : base(null)
        {
            // for deserialization
        }

        public DeleteEtlCommand(string configurationName, EtlType etlType, string databaseName) : base(databaseName)
        {
            ConfigurationName = configurationName;
            EtlType = etlType;
        }

        public override string UpdateDatabaseRecord(DatabaseRecord record, long etag)
        {
            switch (EtlType)
            {
                case EtlType.Raven:
                    Delete(record.RavenEtls, ConfigurationName);
                    return null;
                case EtlType.Sql:
                    Delete(record.SqlEtls, ConfigurationName);
                    return null;
                default:
                    throw new NotSupportedException($"Unknown ETL configuration type: {EtlType}");
            }
        }

        private void Delete<T>(List<EtlConfiguration<T>> etls, string configurationName) where T : EtlDestination
        {
            if (etls == null)
                ThrowNoEtlsDefined(EtlType, configurationName);
            
            var index = etls.FindIndex(x => x.Destination.Name.Equals(configurationName, StringComparison.OrdinalIgnoreCase));

            if (index == -1)
                ThrowConfigurationNotFound(configurationName);

            etls.RemoveAt(index);
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
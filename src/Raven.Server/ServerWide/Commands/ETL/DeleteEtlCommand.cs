using System;
using System.Collections.Generic;
using Raven.Client.Server;
using Raven.Client.Server.ETL;
using Sparrow.Json.Parsing;

namespace Raven.Server.ServerWide.Commands.ETL
{
    public class DeleteEtlCommand : UpdateDatabaseCommand
    {
        public readonly long Id;

        public readonly EtlType EtlType;

        public DeleteEtlCommand() : base(null)
        {
            // for deserialization
        }

        public DeleteEtlCommand(long id, EtlType etlType, string databaseName) : base(databaseName)
        {
            Id = id;
            EtlType = etlType;
        }

        public override string UpdateDatabaseRecord(DatabaseRecord record, long etag)
        {
            switch (EtlType)
            {
                case EtlType.Raven:
                    Delete(record.RavenEtls);
                    return null;
                case EtlType.Sql:
                    Delete(record.SqlEtls);
                    return null;
                default:
                    throw new NotSupportedException($"Unknown ETL configuration type: {EtlType}");
            }
        }

        private void Delete<T>(List<EtlConfiguration<T>> etls) where T : EtlDestination
        {
            if (etls == null)
                ThrowNoEtlsDefined(EtlType);
            
            var index = etls.FindIndex(x => x.Id == Id);

            if (index == -1)
                ThrowConfigurationNotFound(Id);

            etls.RemoveAt(index);
        }

        private static void ThrowConfigurationNotFound(long taskId)
        {
            throw new InvalidOperationException($"Configuration was not found for a give task id: {taskId}");
        }
        
        private static void ThrowNoEtlsDefined(EtlType type)
        {
            throw new InvalidOperationException($"There is no {type} ETL defined so we cannot delete from it");
        }

        public override void FillJson(DynamicJsonValue json)
        {
            json[nameof(Id)] = Id;
            json[nameof(EtlType)] = EtlType;
        }
    }
}
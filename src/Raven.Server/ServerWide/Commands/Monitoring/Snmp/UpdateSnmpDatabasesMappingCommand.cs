using System.Collections.Generic;
using Raven.Client;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.ServerWide.Commands.Monitoring.Snmp
{
    public class UpdateSnmpDatabasesMappingCommand : UpdateValueCommand<List<string>>
    {
        public UpdateSnmpDatabasesMappingCommand()
        {
            Name = Constants.Monitoring.Snmp.DatabasesMappingKey;
        }

        public UpdateSnmpDatabasesMappingCommand(List<string> databases, string uniqueRequestId) : base(uniqueRequestId)
        {
            Name = Constants.Monitoring.Snmp.DatabasesMappingKey;
            Value = databases;
        }

        public override object ValueToJson()
        {
            if (Value == null)
                return null;

            return new DynamicJsonArray(Value);
        }

        public override BlittableJsonReaderObject GetUpdatedValue(JsonOperationContext context, BlittableJsonReaderObject previousValue, long index)
        {
            if (previousValue != null)
            {
                if (previousValue.Modifications == null)
                    previousValue.Modifications = new DynamicJsonValue();

                AddDatabasesIfNecessary(previousValue.Modifications, previousValue, Value);

                if (previousValue.Modifications.Properties.Count == 0)
                    return null;

                return context.ReadObject(previousValue, Name);
            }

            var djv = new DynamicJsonValue();

            AddDatabasesIfNecessary(djv, null, Value);

            return context.ReadObject(djv, Name);
        }

        public override void AssertLicenseLimits(ServerStore serverStore, ClusterOperationContext context)
        {
        }

        private static void AddDatabasesIfNecessary(DynamicJsonValue djv, BlittableJsonReaderObject previousValue, List<string> databases)
        {
            if (databases == null)
                return;

            var propertiesCount = previousValue?.Count ?? 0;
            foreach (var database in databases)
            {
                if (previousValue == null || previousValue.TryGet(database, out long _) == false)
                    djv[database] = propertiesCount + djv.Properties.Count + 1;
            }
        }
    }
}

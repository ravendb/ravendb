using System.Collections.Generic;
using Raven.Client;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.ServerWide.Commands.Monitoring.Snmp
{
    public class AddDatabasesToSnmpMappingCommand : UpdateValueCommand<List<string>>
    {
        public AddDatabasesToSnmpMappingCommand()
        {
            Name = Constants.Monitoring.Snmp.MappingKey;
        }

        public AddDatabasesToSnmpMappingCommand(List<string> databases)
            : this()
        {
            Value = databases;
        }

        public override object ValueToJson()
        {
            if (Value == null)
                return null;

            return new DynamicJsonArray(Value);
        }

        public override BlittableJsonReaderObject OnUpdate(JsonOperationContext context, BlittableJsonReaderObject previousValue)
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
            if (Value != null)
                AddDatabasesIfNecessary(djv, null, Value);

            return context.ReadObject(djv, Name);
        }

        private static void AddDatabasesIfNecessary(DynamicJsonValue djv, BlittableJsonReaderObject previousValue, List<string> databases)
        {
            if (databases == null)
                return;

            foreach (var database in databases)
            {
                if (previousValue == null || previousValue.TryGet(database, out long _) == false)
                    djv[database] = djv.Properties.Count + 1;
            }
        }
    }
}

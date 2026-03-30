using System.Linq;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Client.ServerWide.Operations.ConnectionStrings;
using Raven.Server.Rachis;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.ServerWide.Commands
{
    public sealed class PutServerWideConnectionStringCommand : UpdateValueCommand<ServerWideConnectionString>
    {
        public PutServerWideConnectionStringCommand()
        {
            // for deserialization
        }

        public PutServerWideConnectionStringCommand(ServerWideConnectionString connectionString, string uniqueRequestId) : base(uniqueRequestId)
        {
            Name = ClusterStateMachine.ServerWideConfigurationKey.GetConnectionStringKeyByType(connectionString.Type);
            Value = connectionString;
        }

        public override object ValueToJson()
        {
            return Value.ToJson();
        }

        public override BlittableJsonReaderObject GetUpdatedValue(JsonOperationContext context, BlittableJsonReaderObject previousValue, long index)
        {
            if (string.IsNullOrWhiteSpace(Value.Name))
                throw new RachisApplyException($"Server-wide connection string name cannot be null or empty");

            if (Value.ExcludedDatabases != null &&
                Value.ExcludedDatabases.Any(string.IsNullOrWhiteSpace))
                throw new RachisApplyException($"{nameof(ServerWideConnectionString.ExcludedDatabases)} cannot contain null or empty database names");

            if (previousValue != null)
            {
                previousValue.Modifications ??= new DynamicJsonValue();

                var modifications = new DynamicJsonValue(previousValue);
                modifications[Value.Name] = Value.ToJson();
                return context.ReadObject(previousValue, Name);
            }

            var djv = new DynamicJsonValue
            {
                [Value.Name] = Value.ToJson()
            };

            return context.ReadObject(djv, Name);
        }

        public override void VerifyCanExecuteCommand(ServerStore store, TransactionOperationContext context, bool isClusterAdmin)
        {
            AssertClusterAdmin(isClusterAdmin);
        }

        public static string GetDatabaseRecordConnectionStringName(string name)
        {
            return ServerWideConnectionString.GetDatabaseRecordConnectionStringName(name);
        }

        internal static string GetConnectionStringDictionaryPropertyName(ConnectionStringType type)
        {
            switch (type)
            {
                case ConnectionStringType.Raven:
                    return nameof(Client.ServerWide.DatabaseRecord.RavenConnectionStrings);
                case ConnectionStringType.Sql:
                    return nameof(Client.ServerWide.DatabaseRecord.SqlConnectionStrings);
                case ConnectionStringType.Olap:
                    return nameof(Client.ServerWide.DatabaseRecord.OlapConnectionStrings);
                case ConnectionStringType.ElasticSearch:
                    return nameof(Client.ServerWide.DatabaseRecord.ElasticSearchConnectionStrings);
                case ConnectionStringType.Queue:
                    return nameof(Client.ServerWide.DatabaseRecord.QueueConnectionStrings);
                case ConnectionStringType.Snowflake:
                    return nameof(Client.ServerWide.DatabaseRecord.SnowflakeConnectionStrings);
                case ConnectionStringType.Ai:
                    return nameof(Client.ServerWide.DatabaseRecord.AiConnectionStrings);
                default:
                    throw new System.NotSupportedException($"Unknown connection string type: {type}");
            }
        }
    }
}

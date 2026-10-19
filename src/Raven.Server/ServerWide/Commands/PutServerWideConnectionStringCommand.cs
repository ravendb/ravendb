using System;
using System.Collections.Generic;
using System.Linq;
using Raven.Client.Documents.Operations.AI;
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

            ResolveAiIdentifier(Value, previousValue);

            if (previousValue != null)
            {
                if (Value.ConnectionString is AiConnectionString aiConnectionString)
                    AssertAiIdentifierNotInUse(previousValue, aiConnectionString);

                previousValue.Modifications = new DynamicJsonValue(previousValue);
                previousValue.Modifications[Value.Name] = Value.ToJson();
                return context.ReadObject(previousValue, Name);
            }

            var djv = new DynamicJsonValue
            {
                [Value.Name] = Value.ToJson()
            };

            return context.ReadObject(djv, Name);
        }

        private static void AssertAiIdentifierNotInUse(BlittableJsonReaderObject serverWideConnectionStrings, AiConnectionString aiConnectionString)
        {
            var identifier = GetEffectiveAiIdentifier(aiConnectionString.Identifier, aiConnectionString.Name);
            if (identifier == null)
                return;

            foreach (var name in serverWideConnectionStrings.GetPropertyNames())
            {
                if (name == aiConnectionString.Name)
                    continue;

                if (serverWideConnectionStrings.TryGet(name, out BlittableJsonReaderObject existing) == false || existing == null)
                    continue;

                if (GetEffectiveAiIdentifier(existing, name) == identifier)
                    throw new RachisApplyException(
                        $"Can't put server-wide connection string '{aiConnectionString.Name}'. " +
                        $"The identifier '{identifier}' is already used by server-wide connection string '{name}'");
            }
        }

        internal static void ResolveAiIdentifier(ServerWideConnectionString connectionString, BlittableJsonReaderObject previousValue)
        {
            if (connectionString.ConnectionString is not AiConnectionString aiConnectionString)
                return;

            if (string.IsNullOrWhiteSpace(aiConnectionString.Identifier) &&
                previousValue != null &&
                previousValue.TryGet(connectionString.Name, out BlittableJsonReaderObject stored) &&
                stored != null)
            {
                stored.TryGet(nameof(AiConnectionString.Identifier), out string storedIdentifier);
                aiConnectionString.Identifier = storedIdentifier;
                return;
            }

            EnsureAiIdentifier(aiConnectionString);
        }

        internal static string GetEffectiveAiIdentifier(BlittableJsonReaderObject aiConnectionString, string name)
        {
            aiConnectionString.TryGet(nameof(AiConnectionString.Identifier), out string identifier);
            return GetEffectiveAiIdentifier(identifier, name);
        }

        internal static string GetEffectiveAiIdentifier(string identifier, string name)
        {
            if (string.IsNullOrWhiteSpace(identifier) == false)
                return identifier;

            return AiTaskIdentifierHelper.TryGenerateIdentifier(ServerWideConnectionString.GetNameFromDatabaseRecordConnectionStringName(name), out var generated) ? generated : null;
        }

        internal static void EnsureAiIdentifier(AiConnectionString aiConnectionString)
        {
            bool isValid;
            List<string> errors;
            try
            {
                isValid = aiConnectionString.EnsureIdentifier(out errors);
            }
            catch (Exception e)
            {
                throw new RachisApplyException($"Failed to generate or validate the identifier of AI connection string '{aiConnectionString.Name}'", e);
            }

            if (isValid == false)
                throw new RachisApplyException($"Invalid identifier format. Validation errors:{Environment.NewLine} - {string.Join($"{Environment.NewLine} - ", errors)}");
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

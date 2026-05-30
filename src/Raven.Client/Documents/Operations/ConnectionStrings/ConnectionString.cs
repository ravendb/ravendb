using System;
using System.Collections.Generic;
using System.Linq;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Operations.ConnectionStrings
{
    public abstract class ConnectionString : IDynamicJson
    {
        public string Name { get; set; }

        [ForceJsonSerialization]
        internal List<ConnectionStringUsage> UsedBy { get; set; } = new List<ConnectionStringUsage>();

        public bool Validate(List<string> errors)
        {
            if (errors == null)
                throw new ArgumentNullException(nameof(errors));

            var count = errors.Count;

            ValidateImpl(errors);

            return count == errors.Count;
        }

        public abstract ConnectionStringType Type { get; }

        protected abstract void ValidateImpl(List<string> errors);

        public virtual DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(Name)] = Name,
                [nameof(UsedBy)] = new DynamicJsonArray(UsedBy.Select(x => x.ToJson())),
            };
        }

        public virtual DynamicJsonValue ToAuditJson()
        {
            return new DynamicJsonValue
            {
                [nameof(Name)] = Name
            };
        }

        public virtual bool IsEqual(ConnectionString connectionString)
        {
            if (connectionString == null)
                return false;

            return Name == connectionString.Name && Type == connectionString.Type;
        }

        internal static ConnectionStringType GetConnectionStringType(BlittableJsonReaderObject connectionStringConfiguration)
        {
            if (connectionStringConfiguration.TryGet("Type", out string type) == false)
                throw new InvalidOperationException($"ConnectionString configuration must have {nameof(ConnectionStringType)} field");

            if (Enum.TryParse<ConnectionStringType>(type, true, out var connectionStringType) == false)
                throw new NotSupportedException($"Unknown Connection string type: {connectionStringType}");

            return connectionStringType;
        }
    }

    internal class ConnectionStringUsage : IDynamicJson
    {
        public ConnectionStringUsageKind Kind { get; set; }

        /// <summary>
        /// The numeric task id, for ongoing tasks (ETL, replication, sinks). <c>null</c> for AI agents.
        /// </summary>
        public long? Id { get; set; }

        /// <summary>
        /// The string identifier, for AI agents. <c>null</c> for ongoing tasks.
        /// </summary>
        public string Identifier { get; set; }

        public string Name { get; set; }

        public virtual DynamicJsonValue ToJson() => new DynamicJsonValue
        {
            [nameof(Kind)] = Kind.ToString(),
            [nameof(Id)] = Id,
            [nameof(Identifier)] = Identifier,
            [nameof(Name)] = Name,
        };
    }

    internal enum ConnectionStringUsageKind
    {
        RavenEtl,
        SqlEtl,
        OlapEtl,
        ElasticSearchEtl,
        QueueEtl,
        SnowflakeEtl,
        QueueSink,
        ExternalReplication,
        PullReplicationAsSink,
        EmbeddingsGeneration,
        GenAi,
        AiAgent
    }

    public enum ConnectionStringType
    {
        None,
        Raven,
        Sql,
        Olap,
        ElasticSearch,
        Queue,
        Snowflake,
        Ai
    }
}

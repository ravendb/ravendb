using System;
using System.Collections.Generic;
using System.Linq;
using Newtonsoft.Json;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Operations.ConnectionStrings
{
    public abstract class ConnectionString : IDynamicJson
    {
        public string Name { get; set; }

        public List<ConnectionStringTaskUsage> UsedByTasks { get; set; } = new List<ConnectionStringTaskUsage>();

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
                [nameof(UsedByTasks)] = new DynamicJsonArray(UsedByTasks.Select(x => x.ToJson())),
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

    public sealed class ConnectionStringTaskUsage : IDynamicJson
    {
        public long TaskId { get; set; }
        public string TaskName { get; set; }

        public DynamicJsonValue ToJson() => new DynamicJsonValue
        {
            [nameof(TaskId)] = TaskId,
            [nameof(TaskName)] = TaskName,
        };
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

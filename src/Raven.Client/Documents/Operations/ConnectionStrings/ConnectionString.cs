using System;
using System.Collections.Generic;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Operations.ConnectionStrings
{
    public abstract class ConnectionString : IDynamicJsonValueConvertible
    {
        public string Name { get; set; }

        public bool Validate(ref List<string> errors)
        {
            if (errors == null)
                throw new ArgumentNullException(nameof(errors));

            var count = errors.Count;

            ValidateImpl(ref errors);

            return count == errors.Count;
        }

        public abstract ConnectionStringType Type { get; }

        protected abstract void ValidateImpl(ref List<string> errors);

        public virtual DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(Name)] = Name
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

    public enum ConnectionStringType
    {
        None,
        Raven,
        Sql,
        Olap,
        ElasticSearch,
        Queue
    }
}

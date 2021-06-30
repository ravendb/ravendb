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

        public virtual bool IsEqual(ConnectionString connectionString)
        {
            if (connectionString == null)
                return false;

            return Name == connectionString.Name && Type == connectionString.Type;
        }
    }

    public enum ConnectionStringType
    {
        None,
        Raven,
        Sql,
        Olap,
        Elasticsearch
    }
}

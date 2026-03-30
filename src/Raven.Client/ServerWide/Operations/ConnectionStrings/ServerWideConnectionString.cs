using System;
using System.Linq;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Client.ServerWide.Operations.ConnectionStrings
{
    [CreateFromBlittableJson]
    public sealed class ServerWideConnectionString : IDynamicJson
    {
        internal const string NamePrefix = "Server Wide Connection String";

        public ConnectionString ConnectionString { get; set; }

        public string[] ExcludedDatabases { get; set; }

        public string Name => ConnectionString?.Name;

        public ConnectionStringType Type => ConnectionString?.Type ?? ConnectionStringType.None;

        public bool IsExcluded(string databaseName)
        {
            if (ExcludedDatabases == null)
                return false;

            return ExcludedDatabases.Contains(databaseName, StringComparer.OrdinalIgnoreCase);
        }

        internal static string GetDatabaseRecordConnectionStringName(string name)
        {
            return $"{NamePrefix}, {name}";
        }

        public DynamicJsonValue ToJson()
        {
            var json = ConnectionString?.ToJson() ?? new DynamicJsonValue();
            json["Type"] = Type.ToString();
            json[nameof(ExcludedDatabases)] = ExcludedDatabases;
            return json;
        }

        internal static ServerWideConnectionString FromBlittable(BlittableJsonReaderObject blittable) => CreateFromBlittableJson(blittable);

        internal static ServerWideConnectionString CreateFromBlittableJson(BlittableJsonReaderObject blittable)
        {
            if (blittable == null)
                return null;

            var type = ConnectionString.GetConnectionStringType(blittable);
            var connectionString = DeserializeConnectionString(blittable, type);

            blittable.TryGet(nameof(ExcludedDatabases), out BlittableJsonReaderArray excludedArray);
            string[] excludedDatabases = null;
            if (excludedArray != null)
            {
                excludedDatabases = new string[excludedArray.Length];
                for (int i = 0; i < excludedArray.Length; i++)
                    excludedDatabases[i] = excludedArray[i]?.ToString();
            }

            return new ServerWideConnectionString
            {
                ConnectionString = connectionString,
                ExcludedDatabases = excludedDatabases
            };
        }

        private static ConnectionString DeserializeConnectionString(BlittableJsonReaderObject blittable, ConnectionStringType type)
        {
            switch (type)
            {
                case ConnectionStringType.Raven:
                    return JsonDeserializationBase.GenerateJsonDeserializationRoutine<Documents.Operations.ETL.RavenConnectionString>()(blittable);
                case ConnectionStringType.Sql:
                    return JsonDeserializationBase.GenerateJsonDeserializationRoutine<Documents.Operations.ETL.SQL.SqlConnectionString>()(blittable);
                case ConnectionStringType.Olap:
                    return JsonDeserializationBase.GenerateJsonDeserializationRoutine<Documents.Operations.ETL.OLAP.OlapConnectionString>()(blittable);
                case ConnectionStringType.ElasticSearch:
                    return JsonDeserializationBase.GenerateJsonDeserializationRoutine<Documents.Operations.ETL.ElasticSearch.ElasticSearchConnectionString>()(blittable);
                case ConnectionStringType.Queue:
                    return JsonDeserializationBase.GenerateJsonDeserializationRoutine<Documents.Operations.ETL.Queue.QueueConnectionString>()(blittable);
                case ConnectionStringType.Snowflake:
                    return JsonDeserializationBase.GenerateJsonDeserializationRoutine<Documents.Operations.ETL.Snowflake.SnowflakeConnectionString>()(blittable);
                case ConnectionStringType.Ai:
                    return JsonDeserializationBase.GenerateJsonDeserializationRoutine<Documents.Operations.AI.AiConnectionString>()(blittable);
                default:
                    throw new NotSupportedException($"Unknown connection string type: {type}");
            }
        }
    }
}

using System;
using System.Linq;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Client.ServerWide.Operations.ConnectionStrings
{
    /// <summary>
    /// Represents a server-wide connection string that is automatically propagated to all databases in the cluster
    /// (unless explicitly excluded). Wraps a standard <see cref="Documents.Operations.ConnectionStrings.ConnectionString"/>
    /// with additional server-wide configuration such as <see cref="ExcludedDatabases"/>.
    /// </summary>
    [CreateFromBlittableJson]
    public sealed class ServerWideConnectionString : IDynamicJson
    {
        internal const string NamePrefix = "Server Wide Connection String";

        /// <summary>
        /// The underlying connection string definition (e.g., <c>RavenConnectionString</c>, <c>SqlConnectionString</c>, etc.).
        /// </summary>
        public ConnectionString ConnectionString { get; set; }

        /// <summary>
        /// An optional list of database names that should not receive this server-wide connection string.
        /// When <c>null</c> or empty, the connection string is propagated to all databases.
        /// </summary>
        public string[] ExcludedDatabases { get; set; }

        /// <summary>
        /// The name of the connection string, delegated from the underlying <see cref="ConnectionString"/>.
        /// </summary>
        public string Name => ConnectionString?.Name;

        /// <summary>
        /// The type of the connection string (Raven, Sql, Olap, etc.), delegated from the underlying <see cref="ConnectionString"/>.
        /// </summary>
        public ConnectionStringType Type => ConnectionString?.Type ?? ConnectionStringType.None;

        /// <summary>
        /// Determines whether the specified database is excluded from receiving this server-wide connection string.
        /// </summary>
        /// <param name="databaseName">The name of the database to check.</param>
        /// <returns><c>true</c> if the database is in the <see cref="ExcludedDatabases"/> list; otherwise, <c>false</c>.</returns>
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

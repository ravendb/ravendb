using System;
using System.Collections.Generic;
using System.Linq;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Client.Json.Serialization;
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
        /// The list of ETL tasks that are currently using this server-wide connection string.
        /// </summary>
        public List<ConnectionStringTaskUsage> UsedByTasks { get; set; } = new List<ConnectionStringTaskUsage>();

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
        internal bool IsExcluded(string databaseName)
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
            json[nameof(Type)] = Type.ToString();
            json[nameof(ExcludedDatabases)] = ExcludedDatabases;
            json[nameof(UsedByTasks)] = new DynamicJsonArray(UsedByTasks.Select(x => x.ToJson()));
            return json;
        }

        internal static ServerWideConnectionString FromBlittable(BlittableJsonReaderObject blittable) => CreateFromBlittableJson(blittable);

        internal static ServerWideConnectionString CreateFromBlittableJson(BlittableJsonReaderObject blittable)
        {
            if (blittable == null)
                return null;

            if (blittable.TryGet(nameof(Type), out string _) == false)
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

            var result = new ServerWideConnectionString
            {
                ConnectionString = connectionString,
                ExcludedDatabases = excludedDatabases
            };

            if (blittable.TryGet(nameof(UsedByTasks), out BlittableJsonReaderArray usedByTasksArray) && usedByTasksArray != null)
            {
                foreach (BlittableJsonReaderObject taskBlittable in usedByTasksArray)
                {
                    taskBlittable.TryGet(nameof(ConnectionStringTaskUsage.TaskId), out long taskId);
                    taskBlittable.TryGet(nameof(ConnectionStringTaskUsage.TaskName), out string taskName);
                    result.UsedByTasks.Add(new ConnectionStringTaskUsage { TaskId = taskId, TaskName = taskName });
                }
            }

            return result;
        }

        private static ConnectionString DeserializeConnectionString(BlittableJsonReaderObject blittable, ConnectionStringType type)
        {
            switch (type)
            {
                case ConnectionStringType.Raven:
                    return JsonDeserializationClient.RavenConnectionString(blittable);
                case ConnectionStringType.Sql:
                    return JsonDeserializationClient.SqlConnectionString(blittable);
                case ConnectionStringType.Olap:
                    return JsonDeserializationClient.OlapConnectionString(blittable);
                case ConnectionStringType.ElasticSearch:
                    return JsonDeserializationClient.ElasticSearchConnectionString(blittable);
                case ConnectionStringType.Queue:
                    return JsonDeserializationClient.QueueConnectionString(blittable);
                case ConnectionStringType.Snowflake:
                    return JsonDeserializationClient.SnowflakeConnectionString(blittable);
                case ConnectionStringType.Ai:
                    return JsonDeserializationClient.AiConnectionString(blittable);
                default:
                    throw new NotSupportedException($"Unknown connection string type: {type}");
            }
        }
    }
}

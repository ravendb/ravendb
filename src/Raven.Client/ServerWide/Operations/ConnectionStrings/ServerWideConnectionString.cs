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
        /// The list of usages (ETL, replication, sinks, AI agents) that currently reference this server-wide connection string.
        /// This is computed server-side when reading; any value provided by a client is ignored.
        /// </summary>
        internal List<ServerWideConnectionStringUsage> UsedBy { get; set; } = new List<ServerWideConnectionStringUsage>();

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
            json[nameof(UsedBy)] = new DynamicJsonArray(UsedBy.Select(x => x.ToJson()));
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

            // UsedBy is computed server-side and returned in GET responses; parse it back so the
            // (typed) client and tests can read it. On write it is ignored (recomputed on the next read).
            if (blittable.TryGet(nameof(UsedBy), out BlittableJsonReaderArray usedByArray) && usedByArray != null)
            {
                foreach (BlittableJsonReaderObject usageBlittable in usedByArray)
                {
                    var usage = new ServerWideConnectionStringUsage();

                    if (usageBlittable.TryGet(nameof(ConnectionStringUsage.Kind), out string kindString) &&
                        Enum.TryParse(kindString, out ConnectionStringUsageKind kind))
                        usage.Kind = kind;

                    if (usageBlittable.TryGet(nameof(ConnectionStringUsage.Id), out long id))
                        usage.Id = id;

                    usageBlittable.TryGet(nameof(ConnectionStringUsage.Identifier), out string identifier);
                    usage.Identifier = identifier;

                    usageBlittable.TryGet(nameof(ConnectionStringUsage.Name), out string usageName);
                    usage.Name = usageName;

                    usageBlittable.TryGet(nameof(ServerWideConnectionStringUsage.DatabaseName), out string databaseName);
                    usage.DatabaseName = databaseName;

                    result.UsedBy.Add(usage);
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

    /// <summary>
    /// A usage of a server-wide connection string. Extends <see cref="ConnectionStringUsage"/> with the
    /// <see cref="DatabaseName"/> of the database where the referencing task/agent lives, since server-wide
    /// usages are aggregated across all databases.
    /// </summary>
    internal sealed class ServerWideConnectionStringUsage : ConnectionStringUsage
    {
        public string DatabaseName { get; set; }

        public override DynamicJsonValue ToJson()
        {
            var json = base.ToJson();
            json[nameof(DatabaseName)] = DatabaseName;
            return json;
        }
    }
}

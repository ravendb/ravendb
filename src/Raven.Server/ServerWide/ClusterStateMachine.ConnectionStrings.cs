using System;
using System.Collections.Generic;
using Raven.Client;
using Raven.Client.Documents.Operations.AI;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Client.Documents.Operations.ETL;
using Raven.Client.Documents.Operations.ETL.ElasticSearch;
using Raven.Client.Documents.Operations.ETL.OLAP;
using Raven.Client.Documents.Operations.ETL.Queue;
using Raven.Client.Documents.Operations.ETL.Snowflake;
using Raven.Client.Documents.Operations.ETL.SQL;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations.ConnectionStrings;
using Raven.Server.Rachis;
using Raven.Server.ServerWide.Commands;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Voron;
using Voron.Data.Tables;

namespace Raven.Server.ServerWide;

public sealed partial class ClusterStateMachine
{
    public sealed partial class ServerWideConfigurationKey
    {
        public const string ConnectionStringRaven = "server-wide/connection-strings/raven";
        public const string ConnectionStringSql = "server-wide/connection-strings/sql";
        public const string ConnectionStringOlap = "server-wide/connection-strings/olap";
        public const string ConnectionStringElasticSearch = "server-wide/connection-strings/elasticsearch";
        public const string ConnectionStringQueue = "server-wide/connection-strings/queue";
        public const string ConnectionStringSnowflake = "server-wide/connection-strings/snowflake";
        public const string ConnectionStringAi = "server-wide/connection-strings/ai";

        public static readonly string[] AllConnectionStringKeys = new[]
        {
            ConnectionStringRaven,
            ConnectionStringSql,
            ConnectionStringOlap,
            ConnectionStringElasticSearch,
            ConnectionStringQueue,
            ConnectionStringSnowflake,
            ConnectionStringAi
        };

        public static string GetConnectionStringKeyByType(ConnectionStringType type)
        {
            switch (type)
            {
                case ConnectionStringType.Raven:
                    return ConnectionStringRaven;
                case ConnectionStringType.Sql:
                    return ConnectionStringSql;
                case ConnectionStringType.Olap:
                    return ConnectionStringOlap;
                case ConnectionStringType.ElasticSearch:
                    return ConnectionStringElasticSearch;
                case ConnectionStringType.Queue:
                    return ConnectionStringQueue;
                case ConnectionStringType.Snowflake:
                    return ConnectionStringSnowflake;
                case ConnectionStringType.Ai:
                    return ConnectionStringAi;
                default:
                    throw new ArgumentOutOfRangeException(nameof(type), type, null);
            }
        }
    }

    private void PutServerWideConnectionString(ClusterOperationContext context, string type, BlittableJsonReaderObject cmd, long index)
    {
        var command = (PutServerWideConnectionStringCommand)JsonDeserializationCluster.Commands[type](cmd);
        var serverWideConnectionString = command.Value;

        if (serverWideConnectionString == null)
            throw new RachisInvalidOperationException($"Server-wide connection string is null for command type: {type}");

        if (string.IsNullOrWhiteSpace(serverWideConnectionString.Name))
            throw new RachisInvalidOperationException($"Server-wide connection string name is null or empty for command type: {type}");

        PutServerWideConnectionStringCommand.ResolveAiIdentifier(serverWideConnectionString, Read(context, command.Name));

        var items = context.Transaction.InnerTransaction.OpenTable(ItemsSchema, Items);
        var toUpdate = CollectDatabaseUpdatesForServerWideConnectionString(context, serverWideConnectionString, items);

        UpdateValue<ServerWideConnectionString>(context, type, cmd, index, skipNotifyValueChanged: true);
        ApplyDatabaseRecordUpdates(toUpdate, type, index, items, context);
    }

    private List<(string Key, BlittableJsonReaderObject DatabaseRecord, string DatabaseName, object)> CollectDatabaseUpdatesForServerWideConnectionString(ClusterOperationContext context, ServerWideConnectionString serverWideConnectionString, Table items)
    {
        var dbKey = Constants.Documents.Prefix;
        var toUpdate = new List<(string Key, BlittableJsonReaderObject DatabaseRecord, string DatabaseName, object)>();
        var databaseRecordCSName = ServerWideConnectionString.GetDatabaseRecordConnectionStringName(serverWideConnectionString.Name);
        var propertyName = PutServerWideConnectionStringCommand.GetConnectionStringDictionaryPropertyName(serverWideConnectionString.Type);

        using (Slice.From(context.Allocator, dbKey, out var loweredPrefix))
        {
            foreach (var result in items.SeekByPrimaryKeyPrefix(loweredPrefix, Slices.Empty, 0))
            {
                var (key, _, oldDatabaseRecord) = GetCurrentItem(context, result.Value);
                var databaseName = key.Substring(dbKey.Length);

                var hasConnectionStrings = oldDatabaseRecord.TryGet(propertyName, out BlittableJsonReaderObject connectionStrings);

                if (serverWideConnectionString.IsExcluded(databaseName) == false)
                {
                    var csJson = serverWideConnectionString.ConnectionString.ToJson();
                    csJson[nameof(ConnectionString.Name)] = databaseRecordCSName;

                    if (hasConnectionStrings)
                    {
                        if (serverWideConnectionString.ConnectionString is AiConnectionString aiConnectionString)
                        {
                            var identifier = PutServerWideConnectionStringCommand.GetEffectiveAiIdentifier(aiConnectionString.Identifier, serverWideConnectionString.Name);
                            AssertAiIdentifierNotInUse(connectionStrings, databaseRecordCSName, identifier, databaseName);
                        }

                        connectionStrings.Modifications = new DynamicJsonValue(connectionStrings)
                        {
                            [databaseRecordCSName] = csJson
                        };

                        connectionStrings = context.ReadObject(connectionStrings, propertyName);
                    }
                    else
                    {
                        var djv = new DynamicJsonValue
                        {
                            [databaseRecordCSName] = csJson
                        };

                        connectionStrings = context.ReadObject(djv, propertyName);
                    }
                }
                else if (hasConnectionStrings)
                {
                    // remove previously created connection string from excluded database
                    var csPropertyIndex = connectionStrings.GetPropertyIndex(databaseRecordCSName);
                    if (csPropertyIndex != -1)
                    {
                        // excluding a database must not orphan a task that uses the propagated connection string
                        AssertServerWideConnectionStringNotInUse(context, oldDatabaseRecord, databaseRecordCSName, serverWideConnectionString.Type, databaseName);

                        connectionStrings.Modifications ??= new DynamicJsonValue();
                        connectionStrings.Modifications.Removals = new HashSet<int> { csPropertyIndex };
                        connectionStrings = context.ReadObject(connectionStrings, propertyName);
                    }
                    else
                    {
                        using (oldDatabaseRecord)
                            continue;
                    }
                }
                else
                {
                    using (oldDatabaseRecord)
                        continue;
                }

                using (oldDatabaseRecord)
                using (connectionStrings)
                {
                    oldDatabaseRecord.Modifications = new DynamicJsonValue(oldDatabaseRecord)
                    {
                        [propertyName] = connectionStrings
                    };

                    var updatedDatabaseRecord = context.ReadObject(oldDatabaseRecord, "updated-database-record");
                    toUpdate.Add((Key: key, DatabaseRecord: updatedDatabaseRecord, DatabaseName: databaseName, null));
                }
            }
        }

        return toUpdate;
    }

    private void RemoveServerWideConnectionStringFromAllDatabases(RemoveServerWideConnectionStringCommand.DeleteConfiguration deleteConfiguration, ClusterOperationContext context, string type, long index)
    {
        var items = context.Transaction.InnerTransaction.OpenTable(ItemsSchema, Items);
        var toUpdate = new List<(string Key, BlittableJsonReaderObject DatabaseRecord, string DatabaseName, object)>();

        // deleteConfiguration is null when the connection string didn't exist (UpdateValue was a no-op).
        // There's nothing to remove from databases, but we still call ApplyDatabaseRecordUpdates so the raft index is notified.
        if (deleteConfiguration == null)
        {
            ApplyDatabaseRecordUpdates(toUpdate, type, index, items, context);
            return;
        }

        if (string.IsNullOrWhiteSpace(deleteConfiguration.ConnectionStringName))
            throw new RachisInvalidOperationException($"Connection string name to delete cannot be null or white space for command type {type}: raftIndex {index}");

        var dbKey = Constants.Documents.Prefix;
        var databaseRecordCSName = ServerWideConnectionString.GetDatabaseRecordConnectionStringName(deleteConfiguration.ConnectionStringName);
        var propertyName = PutServerWideConnectionStringCommand.GetConnectionStringDictionaryPropertyName(deleteConfiguration.Type);

        using (Slice.From(context.Allocator, dbKey, out var loweredPrefix))
        {
            foreach (var result in items.SeekByPrimaryKeyPrefix(loweredPrefix, Slices.Empty, 0))
            {
                var (key, _, oldDatabaseRecord) = GetCurrentItem(context, result.Value);

                if (oldDatabaseRecord.TryGet(propertyName, out BlittableJsonReaderObject connectionStrings) == false)
                {
                    using (oldDatabaseRecord)
                        continue;
                }

                var csPropertyIndex = connectionStrings.GetPropertyIndex(databaseRecordCSName);
                if (csPropertyIndex == -1)
                {
                    using (oldDatabaseRecord)
                        continue;
                }

                // check if any ongoing task in this database uses the connection string
                var databaseName = key.Substring(dbKey.Length);
                AssertServerWideConnectionStringNotInUse(context, oldDatabaseRecord, databaseRecordCSName, deleteConfiguration.Type, databaseName);

                connectionStrings.Modifications ??= new DynamicJsonValue();
                connectionStrings.Modifications.Removals = new HashSet<int> { csPropertyIndex };
                connectionStrings = context.ReadObject(connectionStrings, propertyName);

                using (oldDatabaseRecord)
                using (connectionStrings)
                {
                    oldDatabaseRecord.Modifications = new DynamicJsonValue(oldDatabaseRecord)
                    {
                        [propertyName] = connectionStrings
                    };

                    var updatedDatabaseRecord = context.ReadObject(oldDatabaseRecord, "updated-database-record");
                    toUpdate.Add((Key: key, DatabaseRecord: updatedDatabaseRecord, DatabaseName: databaseName, null));
                }
            }
        }

        ApplyDatabaseRecordUpdates(toUpdate, type, index, items, context);
    }

    private static void AssertAiIdentifierNotInUse(BlittableJsonReaderObject aiConnectionStrings, string connectionStringName, string identifier, string databaseName)
    {
        if (identifier == null)
            return;

        foreach (var propertyName in aiConnectionStrings.GetPropertyNames())
        {
            if (propertyName == connectionStringName)
                continue;

            if (aiConnectionStrings.TryGet(propertyName, out BlittableJsonReaderObject existing) == false || existing == null)
                continue;

            if (PutServerWideConnectionStringCommand.GetEffectiveAiIdentifier(existing, propertyName) == identifier)
                ThrowAiIdentifierInUse(connectionStringName, identifier, propertyName, databaseName);
        }
    }

    private static void AssertNewDatabaseAiIdentifierNotInUse(Dictionary<string, AiConnectionString> aiConnectionStrings, string serverWideName, string identifier, string databaseName)
    {
        if (identifier == null)
            return;

        var connectionStringName = ServerWideConnectionString.GetDatabaseRecordConnectionStringName(serverWideName);

        foreach (var (name, existing) in aiConnectionStrings)
        {
            if (name == connectionStringName)
                continue;

            if (existing != null && PutServerWideConnectionStringCommand.GetEffectiveAiIdentifier(existing.Identifier, name) == identifier)
                throw new RachisApplyException(
                    $"Can't create database '{databaseName}'. " +
                    $"Its AI connection string '{name}' uses identifier '{identifier}', which is also used by server-wide connection string '{serverWideName}'. " +
                    $"Exclude the database from '{serverWideName}' or change one of the identifiers.");
        }
    }

    private static void ThrowAiIdentifierInUse(string connectionStringName, string identifier, string existingName, string databaseName)
    {
        throw new RachisApplyException(
            $"Can't put server-wide connection string '{connectionStringName}'. " +
            $"The identifier '{identifier}' is already used by connection string '{existingName}' in database '{databaseName}'");
    }

    private static void AssertServerWideConnectionStringNotInUse(ClusterOperationContext context, BlittableJsonReaderObject databaseRecord, string connectionStringName, ConnectionStringType type, string databaseName)
    {
        var usage = ConnectionStringConsumers.FindUsage(new RawDatabaseRecord(context, databaseRecord), type, connectionStringName);
        if (usage == null)
            return;

        throw new RachisApplyException(
            $"Can't delete server-wide connection string '{connectionStringName}'. " +
            $"It is used by {usage.Kind} '{usage.Name}' in database '{databaseName}'");
    }

    internal IEnumerable<BlittableJsonReaderObject> GetServerWideConnectionStrings(TransactionOperationContext context, ConnectionStringType? type, string name)
    {
        if (type.HasValue && type.Value != ConnectionStringType.None)
        {
            var key = ServerWideConfigurationKey.GetConnectionStringKeyByType(type.Value);
            foreach (var item in GetServerWideConnectionStringsFromKey(context, key, name))
                yield return item;
        }
        else
        {
            foreach (var key in ServerWideConfigurationKey.AllConnectionStringKeys)
            {
                foreach (var item in GetServerWideConnectionStringsFromKey(context, key, name))
                    yield return item;
            }
        }
    }

    private IEnumerable<BlittableJsonReaderObject> GetServerWideConnectionStringsFromKey(TransactionOperationContext context, string key, string name)
    {
        var configurationsBlittable = Read(context, key);
        if (configurationsBlittable == null)
            yield break;

        foreach (var propertyName in configurationsBlittable.GetPropertyNames())
        {
            if (name != null && string.Equals(propertyName, name, StringComparison.OrdinalIgnoreCase) == false)
                continue;

            if (configurationsBlittable.TryGet(propertyName, out BlittableJsonReaderObject connectionStringBlittable))
                yield return connectionStringBlittable;
        }
    }

    /// <summary>
    /// Populates a new database's record with all applicable server-wide connection strings.
    /// Called during AddDatabase processing.
    /// </summary>
    private bool PopulateServerWideConnectionStrings(ClusterOperationContext context, AddDatabaseCommand addDatabaseCommand)
    {
        var hasChanges = false;
        var databaseOwnedAiConnectionStrings = new Dictionary<string, AiConnectionString>(addDatabaseCommand.Record.AiConnectionStrings);

        foreach (var key in ServerWideConfigurationKey.AllConnectionStringKeys)
        {
            var serverWideConnectionStrings = Read(context, key);
            if (serverWideConnectionStrings == null)
                continue;

            var propertyNames = serverWideConnectionStrings.GetPropertyNames();
            if (propertyNames.Length == 0)
                continue;

            foreach (var propertyName in propertyNames)
            {
                if (serverWideConnectionStrings.TryGet(propertyName, out BlittableJsonReaderObject configurationBlittable) == false)
                    continue;

                if (IsExcluded(configurationBlittable, addDatabaseCommand.Name, nameof(ServerWideConnectionString.ExcludedDatabases)))
                    continue;

                var serverWideCS = ServerWideConnectionString.FromBlittable(configurationBlittable);
                if (serverWideCS?.ConnectionString == null)
                    continue;

                var serverWideName = serverWideCS.Name;
                var databaseRecordCSName = ServerWideConnectionString.GetDatabaseRecordConnectionStringName(serverWideName);
                serverWideCS.ConnectionString.Name = databaseRecordCSName;

                switch (serverWideCS.Type)
                {
                    case ConnectionStringType.Raven:
                        addDatabaseCommand.Record.RavenConnectionStrings[databaseRecordCSName] = (RavenConnectionString)serverWideCS.ConnectionString;
                        break;
                    case ConnectionStringType.Sql:
                        addDatabaseCommand.Record.SqlConnectionStrings[databaseRecordCSName] = (SqlConnectionString)serverWideCS.ConnectionString;
                        break;
                    case ConnectionStringType.Olap:
                        addDatabaseCommand.Record.OlapConnectionStrings[databaseRecordCSName] = (OlapConnectionString)serverWideCS.ConnectionString;
                        break;
                    case ConnectionStringType.ElasticSearch:
                        addDatabaseCommand.Record.ElasticSearchConnectionStrings[databaseRecordCSName] = (ElasticSearchConnectionString)serverWideCS.ConnectionString;
                        break;
                    case ConnectionStringType.Queue:
                        addDatabaseCommand.Record.QueueConnectionStrings[databaseRecordCSName] = (QueueConnectionString)serverWideCS.ConnectionString;
                        break;
                    case ConnectionStringType.Snowflake:
                        addDatabaseCommand.Record.SnowflakeConnectionStrings[databaseRecordCSName] = (SnowflakeConnectionString)serverWideCS.ConnectionString;
                        break;
                    case ConnectionStringType.Ai:
                        var aiConnectionString = (AiConnectionString)serverWideCS.ConnectionString;
                        var identifier = PutServerWideConnectionStringCommand.GetEffectiveAiIdentifier(aiConnectionString.Identifier, serverWideName);
                        AssertNewDatabaseAiIdentifierNotInUse(databaseOwnedAiConnectionStrings, serverWideName, identifier, addDatabaseCommand.Name);
                        addDatabaseCommand.Record.AiConnectionStrings[databaseRecordCSName] = aiConnectionString;
                        break;
                }

                hasChanges = true;
            }
        }

        return hasChanges;
    }
}

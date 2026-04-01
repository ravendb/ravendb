using System;
using System.Collections.Generic;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations.ConnectionStrings;
using Raven.Client.ServerWide.Operations.OngoingTasks;
using Raven.Server.Rachis;
using Raven.Server.ServerWide.Commands;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Voron;
using Voron.Data;

namespace Raven.Server.ServerWide;

public sealed partial class ClusterStateMachine
{
    private void UpdateDatabasesWithServerWideConnectionString(ClusterOperationContext context, string type, ServerWideConnectionString serverWideConnectionString, long index)
    {
        if (serverWideConnectionString == null)
            throw new RachisInvalidOperationException($"Server-wide connection string is null for command type: {type}");

        if (string.IsNullOrWhiteSpace(serverWideConnectionString.Name))
            throw new RachisInvalidOperationException($"Server-wide connection string name is null or empty for command type: {type}");

        var items = context.Transaction.InnerTransaction.OpenTable(ItemsSchema, Items);

        const string dbKey = "db/";
        var toUpdate = new List<(string Key, BlittableJsonReaderObject DatabaseRecord, string DatabaseName, object)>();
        var databaseRecordCSName = PutServerWideConnectionStringCommand.GetDatabaseRecordConnectionStringName(serverWideConnectionString.Name);
        var propertyName = PutServerWideConnectionStringCommand.GetConnectionStringDictionaryPropertyName(serverWideConnectionString.Type);

        // create the connection string JSON with the prefixed name
        var csJson = serverWideConnectionString.ConnectionString.ToJson();
        csJson[nameof(ConnectionString.Name)] = databaseRecordCSName;

        using (Slice.From(context.Allocator, dbKey, out var loweredPrefix))
        {
            foreach (var result in items.SeekByPrimaryKeyPrefix(loweredPrefix, Slices.Empty, 0))
            {
                var (key, _, oldDatabaseRecord) = GetCurrentItem(context, result.Value);
                var databaseName = key.Substring(dbKey.Length);

                var hasConnectionStrings = oldDatabaseRecord.TryGet(propertyName, out BlittableJsonReaderObject connectionStrings);

                if (serverWideConnectionString.IsExcluded(databaseName) == false)
                {
                    if (hasConnectionStrings)
                    {
                        connectionStrings.Modifications ??= new DynamicJsonValue();
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

        ApplyDatabaseRecordUpdates(toUpdate, type, index, items, context);
    }

    private void DeleteServerWideConnectionStringFromAllDatabases(DeleteServerWideConnectionStringCommand.DeleteConfiguration deleteConfiguration, ClusterOperationContext context, string type, long index)
    {
        if (deleteConfiguration == null)
            throw new RachisInvalidOperationException($"No configuration was supplied to {type}: raftIndex {index}");

        if (string.IsNullOrWhiteSpace(deleteConfiguration.ConnectionStringName))
            throw new RachisInvalidOperationException($"Connection string name to delete cannot be null or white space for command type {type}: raftIndex {index}");

        var items = context.Transaction.InnerTransaction.OpenTable(ItemsSchema, Items);

        const string dbKey = "db/";
        var toUpdate = new List<(string Key, BlittableJsonReaderObject DatabaseRecord, string DatabaseName, object)>();
        var databaseRecordCSName = PutServerWideConnectionStringCommand.GetDatabaseRecordConnectionStringName(deleteConfiguration.ConnectionStringName);
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
                AssertServerWideConnectionStringNotInUse(oldDatabaseRecord, databaseRecordCSName, deleteConfiguration.Type, databaseName);

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

    private static void AssertServerWideConnectionStringNotInUse(BlittableJsonReaderObject databaseRecord, string connectionStringName, ConnectionStringType type, string databaseName)
    {
        switch (type)
        {
            case ConnectionStringType.Raven:
                CheckTasksUseConnectionString(databaseRecord, nameof(DatabaseRecord.RavenEtls), connectionStringName, databaseName);
                CheckTasksUseConnectionString(databaseRecord, nameof(DatabaseRecord.ExternalReplications), connectionStringName, databaseName);
                break;
            case ConnectionStringType.Sql:
                CheckTasksUseConnectionString(databaseRecord, nameof(DatabaseRecord.SqlEtls), connectionStringName, databaseName);
                break;
            case ConnectionStringType.Olap:
                CheckTasksUseConnectionString(databaseRecord, nameof(DatabaseRecord.OlapEtls), connectionStringName, databaseName);
                break;
            case ConnectionStringType.ElasticSearch:
                CheckTasksUseConnectionString(databaseRecord, nameof(DatabaseRecord.ElasticSearchEtls), connectionStringName, databaseName);
                break;
            case ConnectionStringType.Queue:
                CheckTasksUseConnectionString(databaseRecord, nameof(DatabaseRecord.QueueEtls), connectionStringName, databaseName);
                CheckTasksUseConnectionString(databaseRecord, nameof(DatabaseRecord.QueueSinks), connectionStringName, databaseName);
                break;
            case ConnectionStringType.Snowflake:
                CheckTasksUseConnectionString(databaseRecord, nameof(DatabaseRecord.SnowflakeEtls), connectionStringName, databaseName);
                break;
            case ConnectionStringType.Ai:
                CheckTasksUseConnectionString(databaseRecord, nameof(DatabaseRecord.EmbeddingsGenerations), connectionStringName, databaseName);
                CheckTasksUseConnectionString(databaseRecord, nameof(DatabaseRecord.GenAis), connectionStringName, databaseName);
                break;
        }
    }

    private static void CheckTasksUseConnectionString(BlittableJsonReaderObject databaseRecord, string tasksPropertyName, string connectionStringName, string databaseName)
    {
        if (databaseRecord.TryGet(tasksPropertyName, out BlittableJsonReaderArray tasks) == false || tasks == null)
            return;

        foreach (BlittableJsonReaderObject task in tasks)
        {
            if (task.TryGet("ConnectionStringName", out string taskConnectionStringName) &&
                string.Equals(taskConnectionStringName, connectionStringName, StringComparison.OrdinalIgnoreCase))
            {
                task.TryGet("Name", out string taskName);
                throw new RachisApplyException(
                    $"Can't delete server-wide connection string '{connectionStringName}'. " +
                    $"It is used by task '{taskName}' in database '{databaseName}'");
            }
        }
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

                if (IsExcluded(configurationBlittable, addDatabaseCommand.Name))
                    continue;

                var serverWideCS = ServerWideConnectionString.FromBlittable(configurationBlittable);
                if (serverWideCS?.ConnectionString == null)
                    continue;

                var databaseRecordCSName = PutServerWideConnectionStringCommand.GetDatabaseRecordConnectionStringName(serverWideCS.Name);
                serverWideCS.ConnectionString.Name = databaseRecordCSName;

                switch (serverWideCS.Type)
                {
                    case ConnectionStringType.Raven:
                        addDatabaseCommand.Record.RavenConnectionStrings[databaseRecordCSName] = (Raven.Client.Documents.Operations.ETL.RavenConnectionString)serverWideCS.ConnectionString;
                        break;
                    case ConnectionStringType.Sql:
                        addDatabaseCommand.Record.SqlConnectionStrings[databaseRecordCSName] = (Raven.Client.Documents.Operations.ETL.SQL.SqlConnectionString)serverWideCS.ConnectionString;
                        break;
                    case ConnectionStringType.Olap:
                        addDatabaseCommand.Record.OlapConnectionStrings[databaseRecordCSName] = (Raven.Client.Documents.Operations.ETL.OLAP.OlapConnectionString)serverWideCS.ConnectionString;
                        break;
                    case ConnectionStringType.ElasticSearch:
                        addDatabaseCommand.Record.ElasticSearchConnectionStrings[databaseRecordCSName] = (Raven.Client.Documents.Operations.ETL.ElasticSearch.ElasticSearchConnectionString)serverWideCS.ConnectionString;
                        break;
                    case ConnectionStringType.Queue:
                        addDatabaseCommand.Record.QueueConnectionStrings[databaseRecordCSName] = (Raven.Client.Documents.Operations.ETL.Queue.QueueConnectionString)serverWideCS.ConnectionString;
                        break;
                    case ConnectionStringType.Snowflake:
                        addDatabaseCommand.Record.SnowflakeConnectionStrings[databaseRecordCSName] = (Raven.Client.Documents.Operations.ETL.Snowflake.SnowflakeConnectionString)serverWideCS.ConnectionString;
                        break;
                    case ConnectionStringType.Ai:
                        addDatabaseCommand.Record.AiConnectionStrings[databaseRecordCSName] = (Raven.Client.Documents.Operations.AI.AiConnectionString)serverWideCS.ConnectionString;
                        break;
                }

                hasChanges = true;
            }
        }

        return hasChanges;
    }
}

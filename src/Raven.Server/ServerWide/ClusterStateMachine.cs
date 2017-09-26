using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Dynamic;
using System.IO;
using System.Linq;
using System.Net.Sockets;
using System.Runtime.ExceptionServices;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client;
using Raven.Client.Documents;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Session;
using Raven.Client.Exceptions.Cluster;
using Raven.Client.Exceptions.Database;
using Raven.Client.Exceptions.Security;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations.Certificates;
using Raven.Client.ServerWide.Tcp;
using Raven.Server.Commercial;
using Raven.Server.Json;
using Raven.Server.Rachis;
using Raven.Server.ServerWide.Commands;
using Raven.Server.ServerWide.Commands.ConnectionStrings;
using Raven.Server.ServerWide.Commands.ETL;
using Raven.Server.ServerWide.Commands.Indexes;
using Raven.Server.ServerWide.Commands.Monitoring.Snmp;
using Raven.Server.ServerWide.Commands.PeriodicBackup;
using Raven.Server.ServerWide.Commands.Subscriptions;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow;
using Sparrow.Binary;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Utils;
using Voron;
using Voron.Data;
using Voron.Data.Tables;
using Voron.Exceptions;
using Voron.Impl;

namespace Raven.Server.ServerWide
{
    public class ClusterStateMachine : RachisStateMachine
    {
        private const string LocalNodeStateTreeName = "LocalNodeState";
        private static readonly StringSegment DatabaseName = new StringSegment("DatabaseName");

        private static readonly TableSchema ItemsSchema;

        private static readonly TableSchema CmpXchgItemsSchema;
        private enum UniqueItems
        {
            Key,
            Index,
            Value
        }

        private static readonly Slice EtagIndexName;
        private static readonly Slice Items;
        private static readonly Slice CmpXchg;
        public static readonly Slice Identities;

        static ClusterStateMachine()
        {
            Slice.From(StorageEnvironment.LabelsContext, "Items", out Items);
            Slice.From(StorageEnvironment.LabelsContext, "CmpXchg", out CmpXchg);
            Slice.From(StorageEnvironment.LabelsContext, "EtagIndexName", out EtagIndexName);
            Slice.From(StorageEnvironment.LabelsContext, "Identities", out Identities);

            ItemsSchema = new TableSchema();

            // We use the follow format for the items data
            // { lowered key, key, data, etag }
            ItemsSchema.DefineKey(new TableSchema.SchemaIndexDef
            {
                StartIndex = 0,
                Count = 1
            });

            ItemsSchema.DefineFixedSizeIndex(new TableSchema.FixedSizeSchemaIndexDef
            {
                Name = EtagIndexName,
                IsGlobal = true,
                StartIndex = 3
            });

            CmpXchgItemsSchema = new TableSchema();
            CmpXchgItemsSchema.DefineKey(new TableSchema.SchemaIndexDef
            {
                StartIndex = 0,
                Count = 1
            });
        }

        public event EventHandler<(string DatabaseName, long Index, string Type)> DatabaseChanged;

        public event EventHandler<(string DatabaseName, long Index, string Type)> DatabaseValueChanged;

        public event EventHandler<(long Index, string Type)> ValueChanged;

        private readonly RachisLogIndexNotifications _rachisLogIndexNotifications = new RachisLogIndexNotifications(CancellationToken.None);

        protected override void Apply(TransactionOperationContext context, BlittableJsonReaderObject cmd, long index, Leader leader, ServerStore serverStore)
        {
            if (cmd.TryGet("Type", out string type) == false)
            {
                NotifyLeaderAboutError(index, leader, new CommandExecutionException("Cannot execute command, wrong format"));
                return;
            }

            var sw = Stopwatch.StartNew();
            try
            {
                string errorMessage;
                switch (type)
                {
                    //The reason we have a separate case for removing node from database is because we must 
                    //actually delete the database before we notify about changes to the record otherwise we 
                    //don't know that it was us who needed to delete the database.
                    case nameof(RemoveNodeFromDatabaseCommand):
                        RemoveNodeFromDatabase(context, cmd, index, leader);
                        break;
                    case nameof(RemoveNodeFromClusterCommand):
                        RemoveNodeFromCluster(context, cmd, index, leader);
                        break;
                    case nameof(DeleteValueCommand):
                    case nameof(DeactivateLicenseCommand):
                    case nameof(DeleteCertificateFromClusterCommand):
                        DeleteValue(context, type, cmd, index, leader);
                        break;
                    case nameof(IncrementClusterIdentityCommand):
                        if (ValidatePropertyExistence(cmd, nameof(IncrementClusterIdentityCommand), nameof(IncrementClusterIdentityCommand.Prefix), out errorMessage) == false)
                        {
                            NotifyLeaderAboutError(index, leader, new InvalidDataException(errorMessage));
                            return;
                        }

                        SetValueForTypedDatabaseCommand(context, type, cmd, index, leader, out object result);
                        leader?.SetStateOf(index, result);
                        break;
                    case nameof(UpdateClusterIdentityCommand):
                        if (ValidatePropertyExistence(cmd, nameof(UpdateClusterIdentityCommand), nameof(UpdateClusterIdentityCommand.Identities), out errorMessage) == false)
                        {
                            NotifyLeaderAboutError(index, leader, new InvalidDataException(errorMessage));
                            return;
                        }

                        SetValueForTypedDatabaseCommand(context, type, cmd, index, leader, out _);
                        break;
                    case nameof(PutIndexCommand):
                    case nameof(PutAutoIndexCommand):
                    case nameof(DeleteIndexCommand):
                    case nameof(SetIndexLockCommand):
                    case nameof(SetIndexPriorityCommand):
                    case nameof(EditRevisionsConfigurationCommand):
                    case nameof(UpdatePeriodicBackupCommand):
                    case nameof(EditExpirationCommand):
                    case nameof(ModifyConflictSolverCommand):
                    case nameof(UpdateTopologyCommand):
                    case nameof(DeleteDatabaseCommand):
                    case nameof(UpdateExternalReplicationCommand):
                    case nameof(PromoteDatabaseNodeCommand):
                    case nameof(ToggleTaskStateCommand):
                    case nameof(AddRavenEtlCommand):
                    case nameof(AddSqlEtlCommand):
                    case nameof(UpdateRavenEtlCommand):
                    case nameof(UpdateSqlEtlCommand):
                    case nameof(DeleteOngoingTaskCommand):
                    case nameof(PutRavenConnectionString):
                    case nameof(PutSqlConnectionString):
                    case nameof(RemoveRavenConnectionString):
                    case nameof(RemoveSqlConnectionString):
                        UpdateDatabase(context, type, cmd, index, leader, serverStore);
                        break;
                    case nameof(UpdatePeriodicBackupStatusCommand):
                    case nameof(AcknowledgeSubscriptionBatchCommand):
                    case nameof(PutSubscriptionCommand):
                    case nameof(DeleteSubscriptionCommand):
                    case nameof(UpdateEtlProcessStateCommand):
                    case nameof(ToggleSubscriptionStateCommand):
                    case nameof(UpdateSubscriptionClientConnectionTime):
                    case nameof(UpdateSnmpDatabaseIndexesMappingCommand):
                        SetValueForTypedDatabaseCommand(context, type, cmd, index, leader, out _);
                        break;
                    case nameof(CompareExchangeCommand):
                        CompareExchange(context, type, cmd, index);
                        break;
                    case nameof(UpdateSnmpDatabasesMappingCommand):
                        UpdateValue<List<string>>(context, type, cmd, index, leader);
                        break;
                    case nameof(PutLicenseCommand):
                        PutValue<License>(context, type, cmd, index, leader);
                        break;
                    case nameof(PutLicenseLimitsCommand):
                        PutValue<LicenseLimits>(context, type, cmd, index, leader);
                        break;
                    case nameof(PutCertificateCommand):
                        PutValue<CertificateDefinition>(context, type, cmd, index, leader);
                        // Once the certificate is in the cluster, no need to keep it locally so we delete it.
                        if (cmd.TryGet(nameof(PutCertificateCommand.Name), out string key))
                            DeleteLocalState(context, key);
                        break;
                    case nameof(PutClientConfigurationCommand):
                        PutValue<ClientConfiguration>(context, type, cmd, index, leader);
                        break;
                    case nameof(AddDatabaseCommand):
                        AddDatabase(context, cmd, index, leader);
                        break;
                }
            }
            catch (Exception e)
            {
                NotifyLeaderAboutError(index, leader, new CommandExecutionException($"Cannot execute command of type {type}", e));
            }
            finally
            {
                var executionTime = sw.Elapsed;
                _rachisLogIndexNotifications.RecordNotification(new RecentLogIndexNotification
                {
                    Type = type,
                    ExecutionTime = executionTime,
                    Index = index,
                    LeaderErrorCount = leader?.ErrorsList.Count,
                    Term = leader?.Term,
                    LeaderShipDuration = leader?.LeaderShipDuration,
                });
            }
        }

        private void RemoveNodeFromCluster(TransactionOperationContext context, BlittableJsonReaderObject cmd, long index, Leader leader)
        {
            var removed = JsonDeserializationCluster.RemoveNodeFromClusterCommand(cmd).RemovedNode;
            var items = context.Transaction.InnerTransaction.OpenTable(ItemsSchema, Items);

            foreach (var record in ReadAllDatabases(context))
            {
                try
                {
                    using (Slice.From(context.Allocator, "db/" + record.DatabaseName.ToLowerInvariant(), out Slice lowerKey))
                    using (Slice.From(context.Allocator, "db/" + record.DatabaseName, out Slice key))
                    {
                        if (record.DeletionInProgress != null)
                        {
                            record.DeletionInProgress.Remove(removed);
                            if (record.DeletionInProgress.Count == 0 && record.Topology.Count == 0)
                            {
                                DeleteDatabaseRecord(context, index, items, lowerKey, record.DatabaseName);
                                continue;
                            }
                        }

                        var updated = EntityToBlittable.ConvertEntityToBlittable(record, DocumentConventions.Default, context);

                        UpdateValue(index, items, lowerKey, key, updated);
                    }
                    NotifyDatabaseChanged(context, record.DatabaseName, index, nameof(RemoveNodeFromCluster));
                }
                catch (Exception e)
                {
                    NotifyLeaderAboutError(index, leader, e);
                }
            }
        }

        protected void NotifyLeaderAboutError(long index, Leader leader, Exception e)
        {
            _rachisLogIndexNotifications.RecordNotification(new RecentLogIndexNotification
            {
                Type = "Error",
                ExecutionTime = TimeSpan.Zero,
                Index = index,
                LeaderErrorCount = leader?.ErrorsList.Count,
                Term = leader?.Term,
                LeaderShipDuration = leader?.LeaderShipDuration,
                Exception = e,
            });

            // ReSharper disable once UseNullPropagation
            if (leader == null)
                return;

            leader.SetStateOf(index, tcs => { tcs.TrySetException(e); });
        }

        private static bool ValidatePropertyExistence(BlittableJsonReaderObject cmd, string propertyTypeName, string propertyName, out string errorMessage)
        {
            errorMessage = null;
            if (cmd.TryGet(propertyName, out object _) == false)
            {
                errorMessage = $"Expected to find {propertyTypeName}.{propertyName} property in the Raft command but didn't find it...";
                return false;
            }
            return true;
        }

        private void SetValueForTypedDatabaseCommand(TransactionOperationContext context, string type, BlittableJsonReaderObject cmd, long index, Leader leader, out object result)
        {
            result = null;
            UpdateValueForDatabaseCommand updateCommand = null;
            try
            {
                var items = context.Transaction.InnerTransaction.OpenTable(ItemsSchema, Items);

                updateCommand = (UpdateValueForDatabaseCommand)JsonDeserializationCluster.Commands[type](cmd);

                var record = ReadDatabase(context, updateCommand.DatabaseName);
                if (record == null)
                {
                    NotifyLeaderAboutError(index, leader,
                        new CommandExecutionException($"Cannot set typed value of type {type} for database {updateCommand.DatabaseName}, because does not exist"));
                    return;
                }

                try
                {
                    updateCommand.Execute(context, items, index, record, _parent.CurrentState == RachisConsensus.State.Passive, out result);
                }
                catch (Exception e)
                {
                    NotifyLeaderAboutError(index, leader,
                        new CommandExecutionException($"Cannot set typed value of type {type} for database {updateCommand.DatabaseName}, because does not exist", e));
                }
            }
            finally
            {
                NotifyDatabaseValueChanged(context, updateCommand?.DatabaseName, index, type);
            }
        }

        public async Task WaitForIndexNotification(long index, TimeSpan? timeout = null)
        {
            await _rachisLogIndexNotifications.WaitForIndexNotification(index, timeout ?? _parent.OperationTimeout);
        }

        private unsafe void RemoveNodeFromDatabase(TransactionOperationContext context, BlittableJsonReaderObject cmd, long index, Leader leader)
        {
            var items = context.Transaction.InnerTransaction.OpenTable(ItemsSchema, Items);
            var remove = JsonDeserializationCluster.RemoveNodeFromDatabaseCommand(cmd);
            var databaseName = remove.DatabaseName;
            var databaseNameLowered = databaseName.ToLowerInvariant();
            using (Slice.From(context.Allocator, "db/" + databaseNameLowered, out Slice lowerKey))
            using (Slice.From(context.Allocator, "db/" + databaseName, out Slice key))
            {
                if (items.ReadByKey(lowerKey, out TableValueReader reader) == false)
                {
                    NotifyLeaderAboutError(index, leader, new InvalidOperationException($"The database {databaseName} does not exists"));
                    return;
                }
                var doc = new BlittableJsonReaderObject(reader.Read(2, out int size), size, context);

                var databaseRecord = JsonDeserializationCluster.DatabaseRecord(doc);

                if (doc.TryGet(nameof(DatabaseRecord.Topology), out BlittableJsonReaderObject _) == false)
                {
                    items.DeleteByKey(lowerKey);
                    NotifyDatabaseChanged(context, databaseName, index, nameof(RemoveNodeFromDatabaseCommand));
                    return;
                }
                remove.UpdateDatabaseRecord(databaseRecord, index);

                if (databaseRecord.DeletionInProgress.Count == 0 && databaseRecord.Topology.Count == 0)
                {
                    DeleteDatabaseRecord(context, index, items, lowerKey, databaseName);
                    return;
                }

                var updated = EntityToBlittable.ConvertEntityToBlittable(databaseRecord, DocumentConventions.Default, context);

                UpdateValue(index, items, lowerKey, key, updated);

                NotifyDatabaseChanged(context, databaseName, index, nameof(RemoveNodeFromDatabaseCommand));
            }
        }

        private void DeleteDatabaseRecord(TransactionOperationContext context, long index, Table items, Slice lowerKey, string databaseName)
        {
            // delete database record
            items.DeleteByKey(lowerKey);

            // delete all values linked to database record - for subscription, etl etc.
            CleanupDatabaseRelatedValues(context, items, databaseName);

            NotifyDatabaseChanged(context, databaseName, index, nameof(RemoveNodeFromDatabaseCommand));
        }

        private void CleanupDatabaseRelatedValues(TransactionOperationContext context, Table items, string databaseName)
        {
            var dbValuesPrefix = Helpers.ClusterStateMachineValuesPrefix(databaseName).ToLowerInvariant();
            using (Slice.From(context.Allocator, dbValuesPrefix, out var loweredKey))
            {
                items.DeleteByPrimaryKeyPrefix(loweredKey);
            }

            DeleteIdentities(context, databaseName);
        }

        internal static unsafe void UpdateValue(long index, Table items, Slice lowerKey, Slice key, BlittableJsonReaderObject updated)
        {
            using (items.Allocate(out TableValueBuilder builder))
            {
                builder.Add(lowerKey);
                builder.Add(key);
                builder.Add(updated.BasePointer, updated.Size);
                builder.Add(Bits.SwapBytes(index));

                items.Set(builder);
            }
        }

        private unsafe void AddDatabase(TransactionOperationContext context, BlittableJsonReaderObject cmd, long index, Leader leader)
        {
            var addDatabaseCommand = JsonDeserializationCluster.AddDatabaseCommand(cmd);
            try
            {
                var items = context.Transaction.InnerTransaction.OpenTable(ItemsSchema, Items);
                using (Slice.From(context.Allocator, "db/" + addDatabaseCommand.Name, out Slice valueName))
                using (Slice.From(context.Allocator, "db/" + addDatabaseCommand.Name.ToLowerInvariant(), out Slice valueNameLowered))
                using (var databaseRecordAsJson = EntityToBlittable.ConvertEntityToBlittable(addDatabaseCommand.Record, DocumentConventions.Default, context))
                {
                    if (addDatabaseCommand.RaftCommandIndex != null)
                    {
                        if (items.ReadByKey(valueNameLowered, out TableValueReader reader) == false && addDatabaseCommand.RaftCommandIndex != 0)
                        {
                            NotifyLeaderAboutError(index, leader,
                                new ConcurrencyException("Concurrency violation, the database " + addDatabaseCommand.Name + " does not exists, but had a non zero etag"));
                            return;
                        }

                        var actualEtag = Bits.SwapBytes(*(long*)reader.Read(3, out int size));
                        Debug.Assert(size == sizeof(long));

                        if (actualEtag != addDatabaseCommand.RaftCommandIndex.Value)
                        {
                            NotifyLeaderAboutError(index, leader,
                                new ConcurrencyException("Concurrency violation, the database " + addDatabaseCommand.Name + " has etag " + actualEtag +
                                                         " but was expecting " + addDatabaseCommand.RaftCommandIndex));
                            return;
                        }
                    }

                    UpdateValue(index, items, valueNameLowered, valueName, databaseRecordAsJson);
                    SetDatabaseValues(addDatabaseCommand.DatabaseValues, context, index, items);
                }
            }
            finally
            {
                NotifyDatabaseChanged(context, addDatabaseCommand.Name, index, nameof(AddDatabaseCommand));
            }
        }

        private static void SetDatabaseValues(
            Dictionary<string, ExpandoObject> databaseValues,
            TransactionOperationContext context,
            long index,
            Table items)
        {
            if (databaseValues == null)
                return;

            foreach (var keyValue in databaseValues)
            {
                using (Slice.From(context.Allocator, keyValue.Key, out Slice databaseValueName))
                using (Slice.From(context.Allocator, keyValue.Key.ToLowerInvariant(), out Slice databaseValueNameLowered))
                using (var value = EntityToBlittable.ConvertEntityToBlittable(keyValue.Value, DocumentConventions.Default, context))
                {
                    UpdateValue(index, items, databaseValueNameLowered, databaseValueName, value);
                }
            }
        }

        private void DeleteValue(TransactionOperationContext context, string type, BlittableJsonReaderObject cmd, long index, Leader leader)
        {
            try
            {
                var items = context.Transaction.InnerTransaction.OpenTable(ItemsSchema, Items);
                var delCmd = JsonDeserializationCluster.DeleteValueCommand(cmd);
                if (delCmd.Name.StartsWith("db/"))
                {
                    NotifyLeaderAboutError(index, leader,
                        new InvalidOperationException("Cannot delete " + delCmd.Name + " using DeleteValueCommand, only via dedicated database calls"));
                    return;
                }

                using (Slice.From(context.Allocator, delCmd.Name, out Slice _))
                using (Slice.From(context.Allocator, delCmd.Name.ToLowerInvariant(), out Slice keyNameLowered))
                {
                    items.DeleteByKey(keyNameLowered);
                }
            }
            finally
            {
                NotifyValueChanged(context, type, index);
            }
        }

        private unsafe void UpdateValue<T>(TransactionOperationContext context, string type, BlittableJsonReaderObject cmd, long index, Leader leader)
        {
            try
            {
                var items = context.Transaction.InnerTransaction.OpenTable(ItemsSchema, Items);
                var command = (UpdateValueCommand<T>)CommandBase.CreateFrom(cmd);
                if (command.Name.StartsWith(Constants.Documents.Prefix))
                {
                    NotifyLeaderAboutError(index, leader,
                        new InvalidOperationException("Cannot set " + command.Name + " using PutValueCommand, only via dedicated database calls"));
                    return;
                }

                using (Slice.From(context.Allocator, command.Name, out Slice valueName))
                using (Slice.From(context.Allocator, command.Name.ToLowerInvariant(), out Slice valueNameLowered))
                {
                    BlittableJsonReaderObject previousValue = null;
                    if (items.ReadByKey(valueNameLowered, out var tvr))
                    {
                        var ptr = tvr.Read(2, out int size);
                        previousValue = new BlittableJsonReaderObject(ptr, size, context);
                    }

                    var newValue = command.GetUpdatedValue(context, previousValue);
                    if (newValue == null)
                        return;

                    UpdateValue(index, items, valueNameLowered, valueName, newValue);
                }
            }
            finally
            {
                NotifyValueChanged(context, type, index);
            }
        }

        private void PutValue<T>(TransactionOperationContext context, string type, BlittableJsonReaderObject cmd, long index, Leader leader)
        {
            try
            {
                var items = context.Transaction.InnerTransaction.OpenTable(ItemsSchema, Items);
                var command = (PutValueCommand<T>)CommandBase.CreateFrom(cmd);
                if (command.Name.StartsWith(Constants.Documents.Prefix))
                {
                    NotifyLeaderAboutError(index, leader,
                        new InvalidOperationException("Cannot set " + command.Name + " using PutValueCommand, only via dedicated database calls"));
                    return;
                }

                using (Slice.From(context.Allocator, command.Name, out Slice valueName))
                using (Slice.From(context.Allocator, command.Name.ToLowerInvariant(), out Slice valueNameLowered))
                using (var rec = context.ReadObject(command.ValueToJson(), "inner-val"))
                {
                    UpdateValue(index, items, valueNameLowered, valueName, rec);
                }
            }
            finally
            {
                NotifyValueChanged(context, type, index);
            }
        }

        public override void EnsureNodeRemovalOnDeletion(TransactionOperationContext context, string nodeTag)
        {
            var djv = new RemoveNodeFromClusterCommand
            {
                RemovedNode = nodeTag
            }.ToJson(context);
            var index = _parent.InsertToLeaderLog(context, context.ReadObject(djv, "remove"), RachisEntryFlags.StateMachineCommand);
            context.Transaction.InnerTransaction.LowLevelTransaction.OnDispose += tx =>
            {
                if (tx is LowLevelTransaction llt && llt.Committed)
                {
                    _parent.CurrentLeader.AddToEntries(index);
                }
            };
        }

        private void NotifyValueChanged(TransactionOperationContext context, string type, long index)
        {
            context.Transaction.InnerTransaction.LowLevelTransaction.OnDispose += transaction =>
            {
                if (transaction is LowLevelTransaction llt && llt.Committed)
                    TaskExecutor.Execute(_ =>
                    {
                        try
                        {
                            ValueChanged?.Invoke(this, (index, type));
                            _rachisLogIndexNotifications.NotifyListenersAbout(index, null);
                        }
                        catch (Exception e)
                        {
                            _rachisLogIndexNotifications.NotifyListenersAbout(index, e);
                        }
                    }, null);
            };
        }

        private void NotifyDatabaseChanged(TransactionOperationContext context, string databaseName, long index, string type)
        {
            context.Transaction.InnerTransaction.LowLevelTransaction.OnDispose += transaction =>
            {
                if (transaction is LowLevelTransaction llt && llt.Committed)
                    TaskExecutor.Execute(_ =>
                    {
                        try
                        {
                            DatabaseChanged?.Invoke(this, (databaseName, index, type));
                            _rachisLogIndexNotifications.NotifyListenersAbout(index, null);
                        }
                        catch (Exception e)
                        {
                            _rachisLogIndexNotifications.NotifyListenersAbout(index, e);
                        }
                    }, null);
            };
        }

        private void NotifyDatabaseValueChanged(TransactionOperationContext context, string databaseName, long index, string type)
        {
            context.Transaction.InnerTransaction.LowLevelTransaction.OnDispose += transaction =>
            {
                if (transaction is LowLevelTransaction llt && llt.Committed)
                    TaskExecutor.Execute(_ =>
                    {
                        try
                        {
                            DatabaseValueChanged?.Invoke(this, (databaseName, index, type));
                            _rachisLogIndexNotifications.NotifyListenersAbout(index, null);
                        }
                        catch (Exception e)
                        {
                            _rachisLogIndexNotifications.NotifyListenersAbout(index, e);
                        }
                    }, null);
            };
        }

        private void UpdateDatabase(TransactionOperationContext context, string type, BlittableJsonReaderObject cmd, long index, Leader leader, ServerStore serverStore)
        {
            if (cmd.TryGet(DatabaseName, out string databaseName) == false || string.IsNullOrEmpty(databaseName))
                throw new ArgumentException("Update database command must contain a DatabaseName property");

            try
            {
                var items = context.Transaction.InnerTransaction.OpenTable(ItemsSchema, Items);
                var dbKey = "db/" + databaseName;

                using (Slice.From(context.Allocator, dbKey, out Slice valueName))
                using (Slice.From(context.Allocator, dbKey.ToLowerInvariant(), out Slice valueNameLowered))
                {
                    DatabaseRecord databaseRecord;
                    try
                    {
                        var databaseRecordJson = ReadInternal(context, out long etag, valueNameLowered);
                        var updateCommand = (UpdateDatabaseCommand)JsonDeserializationCluster.Commands[type](cmd);

                        if (databaseRecordJson == null)
                        {
                            if (updateCommand.ErrorOnDatabaseDoesNotExists)
                                NotifyLeaderAboutError(index, leader,
                                    DatabaseDoesNotExistException.CreateWithMessage(databaseName, $"Could not execute update command of type '{type}'."));
                            return;
                        }

                        databaseRecord = JsonDeserializationCluster.DatabaseRecord(databaseRecordJson);

                        if (updateCommand.RaftCommandIndex != null && etag != updateCommand.RaftCommandIndex.Value)
                        {
                            NotifyLeaderAboutError(index, leader,
                                new ConcurrencyException(
                                    $"Concurrency violation at executing {type} command, the database {databaseRecord.DatabaseName} has etag {etag} but was expecting {updateCommand.RaftCommandIndex}"));
                            return;
                        }
                        updateCommand.Initialize(serverStore, context);
                        var relatedRecordIdToDelete = updateCommand.UpdateDatabaseRecord(databaseRecord, index);
                        if (relatedRecordIdToDelete != null)
                        {
                            var itemKey = relatedRecordIdToDelete;
                            using (Slice.From(context.Allocator, itemKey, out Slice _))
                            using (Slice.From(context.Allocator, itemKey.ToLowerInvariant(), out Slice valueNameToDeleteLowered))
                            {
                                items.DeleteByKey(valueNameToDeleteLowered);
                            }
                        }

                        if (databaseRecord.Topology.Count == 0 && databaseRecord.DeletionInProgress.Count == 0)
                        {
                            DeleteDatabaseRecord(context, index, items, valueNameLowered, databaseName);
                            return;
                        }
                    }
                    catch (Exception e)
                    {
                        NotifyLeaderAboutError(index, leader, new CommandExecutionException($"Cannot execute command of type {type} for database {databaseName}", e));
                        return;
                    }

                    var updatedDatabaseBlittable = EntityToBlittable.ConvertEntityToBlittable(databaseRecord, DocumentConventions.Default, context);
                    UpdateValue(index, items, valueNameLowered, valueName, updatedDatabaseBlittable);
                }
            }
            finally
            {
                NotifyDatabaseChanged(context, databaseName, index, type);
            }
        }

        public override bool ShouldSnapshot(Slice slice, RootObjectType type)
        {
            return slice.Content.Match(Items.Content)
                   || slice.Content.Match(CmpXchg.Content)
                   || slice.Content.Match(Identities.Content);
        }

        public override void Initialize(RachisConsensus parent, TransactionOperationContext context)
        {
            base.Initialize(parent, context);
            ItemsSchema.Create(context.Transaction.InnerTransaction, Items, 32);
            CmpXchgItemsSchema.Create(context.Transaction.InnerTransaction, CmpXchg, 32);
            context.Transaction.InnerTransaction.CreateTree(LocalNodeStateTreeName);
            context.Transaction.InnerTransaction.CreateTree(Identities);
        }

        public unsafe void PutLocalState(TransactionOperationContext context, string key, BlittableJsonReaderObject value)
        {
            var localState = context.Transaction.InnerTransaction.CreateTree(LocalNodeStateTreeName);
            using (localState.DirectAdd(key, value.Size, out var ptr))
            {
                value.CopyTo(ptr);
            }
        }

        public void DeleteLocalState(TransactionOperationContext context, string key)
        {
            var localState = context.Transaction.InnerTransaction.CreateTree(LocalNodeStateTreeName);
            localState.Delete(key);
        }

        public unsafe BlittableJsonReaderObject GetLocalState(TransactionOperationContext context, string key)
        {
            var localState = context.Transaction.InnerTransaction.ReadTree(LocalNodeStateTreeName);
            var read = localState.Read(key);
            if (read == null)
                return null;
            return new BlittableJsonReaderObject(read.Reader.Base, read.Reader.Length, context);
        }

        public IEnumerable<string> GetCertificateKeysFromLocalState(TransactionOperationContext context)
        {
            var tree = context.Transaction.InnerTransaction.ReadTree(LocalNodeStateTreeName);
            if (tree == null)
                yield break;

            using (var it = tree.Iterate(prefetch: false))
            {
                if (it.Seek(Slices.BeforeAllKeys) == false)
                    yield break;
                do
                {
                    yield return it.CurrentKey.ToString();
                } while (it.MoveNext());
            }
        }

        public IEnumerable<Tuple<string, BlittableJsonReaderObject>> ItemsStartingWith(TransactionOperationContext context, string prefix, int start, int take)
        {
            var items = context.Transaction.InnerTransaction.OpenTable(ItemsSchema, Items);

            var dbKey = prefix.ToLowerInvariant();
            using (Slice.From(context.Allocator, dbKey, out Slice loweredPrefix))
            {
                foreach (var result in items.SeekByPrimaryKeyPrefix(loweredPrefix, Slices.Empty, start))
                {
                    if (take-- <= 0)
                        yield break;

                    yield return GetCurrentItem(context, result.Value);
                }
            }
        }

        public unsafe void CompareExchange(TransactionOperationContext context, string type, BlittableJsonReaderObject cmd, long index)
        {
            var items = context.Transaction.InnerTransaction.OpenTable(CmpXchgItemsSchema, CmpXchg);
            cmd.TryGet(nameof(CompareExchangeCommand.Key), out string key);
            cmd.TryGet(nameof(CompareExchangeCommand.Index), out long cmdIndex);
            var dbKey = key.ToLowerInvariant();
            using (Slice.From(context.Allocator, dbKey, out Slice keySlice))
            using (items.Allocate(out TableValueBuilder tvb))
            {
                cmd.TryGet(nameof(CompareExchangeCommand.Value), out BlittableJsonReaderObject value);
                value = context.ReadObject(value, nameof(CompareExchangeCommand.Value));

                tvb.Add(keySlice.Content.Ptr, keySlice.Size);
                tvb.Add(index);
                tvb.Add(value.BasePointer, value.Size);

                if (items.ReadByKey(keySlice, out var reader))
                {
                    var itemIndex = *(long*)reader.Read((int)UniqueItems.Index, out var _);
                    if (cmdIndex == itemIndex)
                    {
                        items.Update(reader.Id, tvb);
                    }
                }
                else
                {
                    items.Set(tvb);
                }
            }
            context.Transaction.InnerTransaction.LowLevelTransaction.OnDispose += transaction =>
            {
                if (transaction is LowLevelTransaction llt && llt.Committed)
                    TaskExecutor.Execute(_ =>
                    {
                        try
                        {
                            _rachisLogIndexNotifications.NotifyListenersAbout(index, null);
                        }
                        catch (Exception e)
                        {
                            _rachisLogIndexNotifications.NotifyListenersAbout(index, e);
                        }
                    }, null);
            };
        }

        public unsafe (long Index, BlittableJsonReaderObject Value) GetCmpXchg(TransactionOperationContext context, string key)
        {
            var items = context.Transaction.InnerTransaction.OpenTable(CmpXchgItemsSchema, CmpXchg);
            var dbKey = key.ToLowerInvariant();
            using (Slice.From(context.Allocator, dbKey, out Slice keySlice))
            {
                if (items.ReadByKey(keySlice, out var reader))
                {
                    var index = *(long*)reader.Read((int)UniqueItems.Index, out var _);
                    var value = reader.Read((int)UniqueItems.Value, out var size);
                    return (index, new BlittableJsonReaderObject(value, size, context));
                }
                return (0, null);
            }
        }

        public IEnumerable<string> ItemKeysStartingWith(TransactionOperationContext context, string prefix, int start, int take)
        {
            var items = context.Transaction.InnerTransaction.OpenTable(ItemsSchema, Items);

            var dbKey = prefix.ToLowerInvariant();
            using (Slice.From(context.Allocator, dbKey, out Slice loweredPrefix))
            {
                foreach (var result in items.SeekByPrimaryKeyPrefix(loweredPrefix, Slices.Empty, start))
                {
                    if (take-- <= 0)
                        yield break;

                    yield return GetCurrentItemKey(result.Value);
                }
            }
        }

        public IEnumerable<string> GetDatabaseNames(TransactionOperationContext context, int start = 0, int take = int.MaxValue)
        {
            var items = context.Transaction.InnerTransaction.OpenTable(ItemsSchema, Items);

            const string dbKey = "db/";
            using (Slice.From(context.Allocator, dbKey, out Slice loweredPrefix))
            {
                foreach (var result in items.SeekByPrimaryKeyPrefix(loweredPrefix, Slices.Empty, 0))
                {
                    if (take-- <= 0)
                        yield break;

                    yield return GetCurrentItemKey(result.Value).Substring(3);
                }
            }
        }

        public int GetClusterSize(TransactionOperationContext context)
        {
            var items = context.Transaction.InnerTransaction.OpenTable(ItemsSchema, Items);

            const string dbKey = "db/";
            var count = 0;

            using (Slice.From(context.Allocator, dbKey, out Slice loweredPrefix))
            {
                foreach (var _ in items.SeekByPrimaryKeyPrefix(loweredPrefix, Slices.Empty, 0))
                {
                    count++;
                }
            }

            return count;
        }

        public IEnumerable<DatabaseRecord> ReadAllDatabases(TransactionOperationContext context, int start = 0, int take = int.MaxValue)
        {
            var items = context.Transaction.InnerTransaction.OpenTable(ItemsSchema, Items);

            const string dbKey = "db/";
            using (Slice.From(context.Allocator, dbKey, out Slice loweredPrefix))
            {
                foreach (var result in items.SeekByPrimaryKeyPrefix(loweredPrefix, Slices.Empty, 0))
                {
                    if (take-- <= 0)
                        yield break;

                    var doc = Read(context, GetCurrentItemKey(result.Value));
                    if (doc == null)
                        continue;

                    yield return JsonDeserializationCluster.DatabaseRecord(doc);
                }
            }
        }

        private static unsafe string GetCurrentItemKey(Table.TableValueHolder result)
        {
            return Encoding.UTF8.GetString(result.Reader.Read(1, out int size), size);
        }

        private static unsafe Tuple<string, BlittableJsonReaderObject> GetCurrentItem(JsonOperationContext context, Table.TableValueHolder result)
        {
            var ptr = result.Reader.Read(2, out int size);
            var doc = new BlittableJsonReaderObject(ptr, size, context);

            var key = Encoding.UTF8.GetString(result.Reader.Read(1, out size), size);

            return Tuple.Create(key, doc);
        }

        public DatabaseRecord ReadDatabase(TransactionOperationContext context, string name)
        {
            return ReadDatabase(context, name, out long _);
        }

        public DatabaseRecord ReadDatabase<T>(TransactionOperationContext<T> context, string name, out long etag)
            where T : RavenTransaction
        {
            var doc = Read(context, "db/" + name.ToLowerInvariant(), out etag);
            if (doc == null)
                return null;

            return JsonDeserializationCluster.DatabaseRecord(doc);
        }

        public IEnumerable<(string Prefix, long Value)> ReadIdentities<T>(TransactionOperationContext<T> context, string databaseName, int start, long take)
            where T : RavenTransaction
        {
            var identities = context.Transaction.InnerTransaction.ReadTree(Identities);

            var prefixString = IncrementClusterIdentityCommand.GetStorageKey(databaseName, null);
            using (Slice.From(context.Allocator, prefixString, out var prefix))
            {
                using (var it = identities.Iterate(prefetch: false))
                {
                    it.SetRequiredPrefix(prefix);

                    if (it.Seek(prefix) == false || it.Skip(start) == false)
                        yield break;

                    do
                    {
                        if (take-- <= 0)
                            break;

                        var key = it.CurrentKey;
                        var keyAsString = key.ToString();
                        var value = it.CreateReaderForCurrent().ReadLittleEndianInt64();

                        yield return (keyAsString.Substring(prefixString.Length), value);

                    } while (it.MoveNext());
                }
            }
        }

        private static void DeleteIdentities<T>(TransactionOperationContext<T> context, string name)
            where T : RavenTransaction
        {
            const int batchSize = 1024;
            var identities = context.Transaction.InnerTransaction.ReadTree(Identities);

            var prefixString = IncrementClusterIdentityCommand.GetStorageKey(name, null);
            using (Slice.From(context.Allocator, prefixString, out var prefix))
            {
                var toRemove = new List<Slice>();
                while (true)
                {
                    using (var it = identities.Iterate(prefetch: false))
                    {
                        it.SetRequiredPrefix(prefix);

                        if (it.Seek(prefix) == false)
                            return;

                        do
                        {
                            toRemove.Add(it.CurrentKey.Clone(context.Allocator, ByteStringType.Immutable));

                        } while (toRemove.Count < batchSize && it.MoveNext());
                    }

                    foreach (var key in toRemove)
                        identities.Delete(key);

                    if (toRemove.Count < batchSize)
                        break;

                    toRemove.Clear();
                }
            }
        }

        public BlittableJsonReaderObject Read<T>(TransactionOperationContext<T> context, string name)
            where T : RavenTransaction
        {
            return Read(context, name, out long _);
        }

        public BlittableJsonReaderObject Read<T>(TransactionOperationContext<T> context, string name, out long etag)
            where T : RavenTransaction
        {
            var dbKey = name.ToLowerInvariant();
            using (Slice.From(context.Allocator, dbKey, out Slice key))
            {
                return ReadInternal(context, out etag, key);
            }
        }

        private static unsafe BlittableJsonReaderObject ReadInternal<T>(TransactionOperationContext<T> context, out long etag, Slice key)
            where T : RavenTransaction
        {
            var items = context.Transaction.InnerTransaction.OpenTable(ItemsSchema, Items);
            if (items.ReadByKey(key, out TableValueReader reader) == false)
            {
                etag = 0;
                return null;
            }

            var ptr = reader.Read(2, out int size);
            var doc = new BlittableJsonReaderObject(ptr, size, context);

            etag = Bits.SwapBytes(*(long*)reader.Read(3, out size));
            Debug.Assert(size == sizeof(long));

            return doc;
        }

        public static IEnumerable<(Slice Key, BlittableJsonReaderObject Value)> ReadValuesStartingWith(
            TransactionOperationContext context, string startsWithKey)
        {
            var startsWithKeyLower = startsWithKey.ToLowerInvariant();
            using (Slice.From(context.Allocator, startsWithKeyLower, out Slice startsWithSlice))
            {
                var items = context.Transaction.InnerTransaction.OpenTable(ItemsSchema, Items);

                foreach (var holder in items.SeekByPrimaryKeyPrefix(startsWithSlice, Slices.Empty, 0))
                {
                    var reader = holder.Value.Reader;
                    var size = GetDataAndEtagTupleFromReader(context, reader, out BlittableJsonReaderObject doc, out long _);
                    Debug.Assert(size == sizeof(long));

                    yield return (holder.Key, doc);
                }
            }
        }

        public long GetIdentitiesCount(TransactionOperationContext context)
        {
            var identities = context.Transaction.InnerTransaction.ReadTree(Identities);
            return identities.State.NumberOfEntries;
        }

        private static unsafe int GetDataAndEtagTupleFromReader(JsonOperationContext context, TableValueReader reader, out BlittableJsonReaderObject doc,
            out long etag)
        {
            var ptr = reader.Read(2, out int size);
            doc = new BlittableJsonReaderObject(ptr, size, context);

            etag = Bits.SwapBytes(*(long*)reader.Read(3, out size));
            Debug.Assert(size == sizeof(long));
            return size;
        }

        public override async Task<Stream> ConnectToPeer(string url, X509Certificate2 certificate)
        {
            if (url == null)
                throw new ArgumentNullException(nameof(url));
            if (_parent == null)
                throw new InvalidOperationException("Cannot connect to peer without a parent");
            if (_parent.IsEncrypted && url.StartsWith("https:", StringComparison.OrdinalIgnoreCase) == false)
                throw new InvalidOperationException($"Failed to connect to node {url}. Connections from encrypted store must use HTTPS.");

            var info = await ReplicationUtils.GetTcpInfoAsync(url, null, "Cluster", certificate);

            TcpClient tcpClient = null;
            Stream stream = null;
            try
            {
                tcpClient = await TcpUtils.ConnectAsync(info.Url).ConfigureAwait(false);
                stream = await TcpUtils.WrapStreamWithSslAsync(tcpClient, info, _parent.ClusterCertificate);

                using (ContextPoolForReadOnlyOperations.AllocateOperationContext(out JsonOperationContext context))
                {
                    var msg = new DynamicJsonValue
                    {
                        [nameof(TcpConnectionHeaderMessage.DatabaseName)] = null,
                        [nameof(TcpConnectionHeaderMessage.Operation)] = TcpConnectionHeaderMessage.OperationTypes.Cluster,
                        [nameof(TcpConnectionHeaderMessage.OperationVersion)] = TcpConnectionHeaderMessage.ClusterTcpVersion
                    };
                    using (var writer = new BlittableJsonTextWriter(context, stream))
                    using (var msgJson = context.ReadObject(msg, "message"))
                    {
                        context.Write(writer, msgJson);
                    }
                    using (var response = context.ReadForMemory(stream, "cluster-ConnectToPeer-header-response"))
                    {
                        var reply = JsonDeserializationServer.TcpConnectionHeaderResponse(response);
                        switch (reply.Status)
                        {
                            case TcpConnectionStatus.Ok:
                                break;
                            case TcpConnectionStatus.AuthorizationFailed:
                                throw new AuthorizationException($"Unable to access  {url} because {reply.Message}");
                            case TcpConnectionStatus.TcpVersionMismatch:
                                throw new InvalidOperationException($"Unable to access  {url} because {reply.Message}");
                        }
                    }
                }
                return stream;
            }
            catch (Exception)
            {
                stream?.Dispose();
                tcpClient?.Dispose();
                throw;
            }
        }

        public override void OnSnapshotInstalled(TransactionOperationContext context, long lastIncludedIndex, ServerStore serverStore)
        {
            using (context.OpenWriteTransaction())
            {
                // lets read all the certificate keys from the cluster, and delete the matching ones from the local state
                var clusterCertificateKeys = serverStore.Cluster.ItemKeysStartingWith(context, Constants.Certificates.Prefix, 0, int.MaxValue);

                foreach (var key in clusterCertificateKeys)
                {
                    using (GetLocalState(context, key))
                    {
                        DeleteLocalState(context, key);
                    }
                }

                serverStore.InvokeLicenseChanged();

                // there is potentially a lot of work to be done here so we are responding to the change on a separate task.
                var onDatabaseChanged = DatabaseChanged;
                if (onDatabaseChanged != null)
                {
                    var listOfDatabaseName = GetDatabaseNames(context).ToList();
                    TaskExecutor.Execute(_ =>
                    {
                        foreach (var db in listOfDatabaseName)
                            onDatabaseChanged.Invoke(this, (db, lastIncludedIndex, "SnapshotInstalled"));
                    }, null);
                }

                context.Transaction.Commit();
            }

            _rachisLogIndexNotifications.NotifyListenersAbout(lastIncludedIndex, null);
        }
    }

    public class RachisLogIndexNotifications
    {
        public long LastModifiedIndex;
        private readonly AsyncManualResetEvent _notifiedListeners;
        private readonly ConcurrentQueue<ErrorHolder> _errors = new ConcurrentQueue<ErrorHolder>();
        private int _numberOfErrors;

        public readonly Queue<RecentLogIndexNotification> RecentNotifications = new Queue<RecentLogIndexNotification>();

        private class ErrorHolder
        {
            public long Index;
            public ExceptionDispatchInfo Exception;
        }

        public RachisLogIndexNotifications(CancellationToken token)
        {
            _notifiedListeners = new AsyncManualResetEvent(token);
        }

        public async Task WaitForIndexNotification(long index, TimeSpan? timeout = null)
        {
            while (true)
            {
                // first get the task, then wait on it
                var waitAsync = timeout.HasValue == false ?
                    _notifiedListeners.WaitAsync() :
                    _notifiedListeners.WaitAsync(timeout.Value);

                if (index <= Interlocked.Read(ref LastModifiedIndex))
                    break;

                if (await waitAsync == false)
                {
                    var copy = Interlocked.Read(ref LastModifiedIndex);
                    if (index <= copy)
                        break;
                    ThrowTimeoutException(timeout ?? TimeSpan.MaxValue, index, copy);
                }
            }

            if (_errors.IsEmpty)
                return;

            foreach (var error in _errors)
            {
                if (error.Index == index)
                    error.Exception.Throw();// rethrow
            }
        }

        private void ThrowTimeoutException(TimeSpan value, long index, long lastModifiedIndex)
        {
            throw new TimeoutException($"Waited for {value} but didn't get index notification for {index}. " +
                                       $"Last commit index is: {lastModifiedIndex}. " +
                                       $"Number of errors is: {_numberOfErrors}." + Environment.NewLine +
                                       PrintLastNotifications());
        }

        private string PrintLastNotifications()
        {
            var notifications = RecentNotifications.ToArray();
            var builder = new StringBuilder(notifications.Length);
            foreach (var notification in notifications)
            {
                builder
                    .Append("Index: ")
                    .Append(notification.Index)
                    .Append(". Type: ")
                    .Append(notification.Type)
                    .Append(". ExecutionTime: ")
                    .Append(notification.ExecutionTime)
                    .Append(". Term: ")
                    .Append(notification.Term)
                    .Append(". LeaderErrorCount: ")
                    .Append(notification.LeaderErrorCount)
                    .Append(". LeaderShipDuration: ")
                    .Append(notification.LeaderShipDuration)
                    .Append(". Exception: ")
                    .Append(notification.Exception)
                    .AppendLine();
            }
            return builder.ToString();
        }

        public void RecordNotification(RecentLogIndexNotification notification)
        {
            RecentNotifications.Enqueue(notification);
            while (RecentNotifications.Count > 25)
                RecentNotifications.TryDequeue(out _);
        }

        public void NotifyListenersAbout(long index, Exception e)
        {
            if (e != null)
            {
                _errors.Enqueue(new ErrorHolder
                {
                    Index = index,
                    Exception = ExceptionDispatchInfo.Capture(e)
                });
                if (Interlocked.Increment(ref _numberOfErrors) > 25)
                {
                    _errors.TryDequeue(out _);
                    Interlocked.Decrement(ref _numberOfErrors);
                }
            }
            InterlockedExchangeMax(ref LastModifiedIndex, index);
            _notifiedListeners.SetAndResetAtomically();
        }

        private static bool InterlockedExchangeMax(ref long location, long newValue)
        {
            long initialValue;
            do
            {
                initialValue = location;
                if (initialValue >= newValue)
                    return false;
            }
            while (Interlocked.CompareExchange(ref location, newValue, initialValue) != initialValue);
            return true;
        }
    }

    public class RecentLogIndexNotification
    {
        public string Type;
        public TimeSpan ExecutionTime;
        public long Index;
        public int? LeaderErrorCount;
        public long? Term;
        public long? LeaderShipDuration;
        public Exception Exception;
    }
}

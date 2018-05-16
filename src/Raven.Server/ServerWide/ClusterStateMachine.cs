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
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.Documents.Operations.Configuration;
using Raven.Client.Documents.Session;
using Raven.Client.Exceptions;
using Raven.Client.Exceptions.Cluster;
using Raven.Client.Exceptions.Database;
using Raven.Client.Exceptions.Security;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Commands;
using Raven.Client.ServerWide.Operations.Certificates;
using Raven.Client.ServerWide.Tcp;
using Raven.Client.Util;
using Raven.Server.Commercial;
using Raven.Server.Json;
using Raven.Server.NotificationCenter.Notifications;
using Raven.Server.NotificationCenter.Notifications.Details;
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

        private static readonly TableSchema CompareExchangeSchema;
        public enum UniqueItems
        {
            Key,
            Index,
            Value
        }

        private static readonly Slice Items;
        private static readonly Slice CompareExchange;
        public static readonly Slice Identities;

        static ClusterStateMachine()
        {
            Slice.From(StorageEnvironment.LabelsContext, "Items", out Items);
            Slice.From(StorageEnvironment.LabelsContext, "CmpXchg", out CompareExchange);
            Slice.From(StorageEnvironment.LabelsContext, "Identities", out Identities);

            ItemsSchema = new TableSchema();

            // We use the follow format for the items data
            // { lowered key, key, data, etag }
            ItemsSchema.DefineKey(new TableSchema.SchemaIndexDef
            {
                StartIndex = 0,
                Count = 1
            });

            CompareExchangeSchema = new TableSchema();
            CompareExchangeSchema.DefineKey(new TableSchema.SchemaIndexDef
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
                    case nameof(AddOrUpdateCompareExchangeBatchCommand):
                        if (cmd.TryGet(nameof(AddOrUpdateCompareExchangeBatchCommand.Commands), out BlittableJsonReaderArray commands) == false)
                        {
                            throw new InvalidDataException($"'{nameof(AddOrUpdateCompareExchangeBatchCommand.Commands)}' is missing in '{nameof(AddOrUpdateCompareExchangeBatchCommand)}'.");
                        }
                        foreach (BlittableJsonReaderObject command in commands)
                        {
                            Apply(context, command, index, leader, serverStore);
                        }
                        break;
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
                    case nameof(DeleteCertificateFromClusterCommand):
                        DeleteValue(context, type, cmd, index, leader);
                        break;
                    case nameof(DeleteMultipleValuesCommand):
                    case nameof(DeleteCertificateCollectionFromClusterCommand):
                        DeleteMultipleValues(context, type, cmd, index, leader);
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
                    case nameof(IncrementClusterIdentitiesBatchCommand):
                        if (ValidatePropertyExistence(cmd, nameof(IncrementClusterIdentitiesBatchCommand), nameof(IncrementClusterIdentitiesBatchCommand.DatabaseName), out errorMessage) == false)
                        {
                            NotifyLeaderAboutError(index, leader, new InvalidDataException(errorMessage));
                            return;
                        }

                        SetValueForTypedDatabaseCommand(context, type, cmd, index, leader, out result);
                        leader?.SetStateOf(index, result);
                        break;
                    case nameof(UpdateClusterIdentityCommand):
                        if (ValidatePropertyExistence(cmd, nameof(UpdateClusterIdentityCommand), nameof(UpdateClusterIdentityCommand.Identities), out errorMessage) == false)
                        {
                            NotifyLeaderAboutError(index, leader, new InvalidDataException(errorMessage));
                            return;
                        }

                        SetValueForTypedDatabaseCommand(context, type, cmd, index, leader, out result);
                        leader?.SetStateOf(index, result);
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
                    case nameof(PutRavenConnectionStringCommand):
                    case nameof(PutSqlConnectionStringCommand):
                    case nameof(RemoveRavenConnectionStringCommand):
                    case nameof(RemoveSqlConnectionStringCommand):
                        UpdateDatabase(context, type, cmd, index, leader, serverStore);
                        break;
                    case nameof(UpdatePeriodicBackupStatusCommand):
                    case nameof(UpdateExternalReplicationStateCommand):
                    case nameof(AcknowledgeSubscriptionBatchCommand):
                    case nameof(PutSubscriptionCommand):
                    case nameof(DeleteSubscriptionCommand):
                    case nameof(UpdateEtlProcessStateCommand):
                    case nameof(ToggleSubscriptionStateCommand):
                    case nameof(UpdateSubscriptionClientConnectionTime):
                    case nameof(UpdateSnmpDatabaseIndexesMappingCommand):
                    case nameof(RemoveEtlProcessStateCommand):
                        SetValueForTypedDatabaseCommand(context, type, cmd, index, leader, out _);
                        break;
                    case nameof(AddOrUpdateCompareExchangeCommand):
                    case nameof(RemoveCompareExchangeCommand):
                        ExecuteCompareExchange(context, type, cmd, index, out var removeItem);
                        leader?.SetStateOf(index, removeItem);
                        break;
                    case nameof(InstallUpdatedServerCertificateCommand):
                        InstallUpdatedServerCertificate(context, cmd, index, serverStore);
                        break;
                    case nameof(RecheckStatusOfServerCertificateCommand):
                        if (_parent.Log.IsOperationsEnabled)
                            _parent.Log.Operations($"Received {nameof(RecheckStatusOfServerCertificateCommand)}.");
                        NotifyValueChanged(context, type, index); // just need to notify listeners
                        break;
                    case nameof(ConfirmReceiptServerCertificateCommand):
                        ConfirmReceiptServerCertificate(context, cmd, index, serverStore);
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
                        var cert = PutValue<CertificateDefinition>(context, type, cmd, index, leader);
                        // Once the certificate is in the cluster, no need to keep it locally so we delete it.
                        if (cmd.TryGet(nameof(PutCertificateCommand.Name), out string key))
                            DeleteLocalState(context, key);

                        Task.Run(() =>
                        {
                            try
                            {
                                // we do this in an async manner because on some machines it pops up a UI and we need to ensure
                                // that it isn't blocking the state machine
                                CertificateUtils.RegisterCertificateInOperatingSystem(new X509Certificate2(Convert.FromBase64String(cert.Certificate)));
                            }
                            catch (Exception e)
                            {
                                if(_parent.Log.IsOperationsEnabled)
                                    _parent.Log.Operations($"Failed to register {cert.Name} in the operating system", e);
                            }
                        });

                        break;
                    case nameof(PutClientConfigurationCommand):
                        PutValue<ClientConfiguration>(context, type, cmd, index, leader);
                        break;
                    case nameof(AddDatabaseCommand):
                        AddDatabase(context, cmd, index, leader);
                        break;
                }
            }
            catch (VoronErrorException e)
            {
                NotifyLeaderAboutError(index, leader, new CommandExecutionException($"Cannot execute command of type {type}", e));
                throw;
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

        private void ConfirmReceiptServerCertificate(TransactionOperationContext context, BlittableJsonReaderObject cmd, long index, ServerStore serverStore)
        {
            if (_parent.Log.IsOperationsEnabled)
                _parent.Log.Operations($"Received {nameof(ConfirmReceiptServerCertificateCommand)}.");
            try
            {
                var items = context.Transaction.InnerTransaction.OpenTable(ItemsSchema, Items);
                using (Slice.From(context.Allocator, "server/cert", out var key))
                {
                    if (cmd.TryGet(nameof(ConfirmReceiptServerCertificateCommand.Thumbprint), out string thumbprint) == false)
                    {
                        throw new ArgumentException($"Thumbprint property didn't exist in {nameof(ConfirmReceiptServerCertificateCommand)}");
                    }
                    var certInstallation = GetItem(context, "server/cert");
                    if (certInstallation == null)
                        return; // already applied? 

                    if (certInstallation.TryGet("Thumbprint", out string storedThumbprint) == false)
                        throw new ArgumentException("Thumbprint property didn't exist in 'server/cert' value");

                    if (storedThumbprint != thumbprint)
                        return; // confirmation for a different cert, ignoring

                    certInstallation.TryGet("Confirmations", out int confirmations);

                    certInstallation.Modifications = new DynamicJsonValue(certInstallation)
                    {
                        ["Confirmations"] = confirmations + 1
                    };

                    certInstallation = context.ReadObject(certInstallation, "server.cert.update");

                    UpdateValue(index, items, key, key, certInstallation);

                    if (_parent.Log.IsOperationsEnabled)
                        _parent.Log.Operations("Confirming to replace the server certificate.");

                    // this will trigger the handling of the certificate update 
                    NotifyValueChanged(context, nameof(ConfirmReceiptServerCertificateCommand), index);
                }
            }
            catch (Exception e)
            {
                if (_parent.Log.IsOperationsEnabled)
                    _parent.Log.Operations($"{nameof(ConfirmReceiptServerCertificate)} failed.", e);

                serverStore.NotificationCenter.Add(AlertRaised.Create(
                    null,
                    "Server certificate",
                    "Failed to confirm receipt of the new certificate.",
                    AlertType.Certificates_ReplaceError,
                    NotificationSeverity.Error,
                    "Cluster.Certificate.Replace.Error",
                    new ExceptionDetails(e)));
            }
        }

        private void InstallUpdatedServerCertificate(TransactionOperationContext context, BlittableJsonReaderObject cmd, long index, ServerStore serverStore)
        {
            if (_parent.Log.IsOperationsEnabled)
                _parent.Log.Operations($"Received {nameof(InstallUpdatedServerCertificateCommand)}.");
            try
            {
                if (cmd.TryGet(nameof(InstallUpdatedServerCertificateCommand.Certificate), out string cert) == false || string.IsNullOrEmpty(cert))
                {
                    throw new ArgumentException($"Certificate property didn't exist in {nameof(InstallUpdatedServerCertificateCommand)}");
                }

                cmd.TryGet(nameof(InstallUpdatedServerCertificateCommand.ReplaceImmediately), out bool replaceImmediately);

                var x509Certificate = new X509Certificate2(Convert.FromBase64String(cert));
                // we assume that this is valid, and we don't check dates, since that would introduce external factor to the state machine, which is not alllowed
                using (Slice.From(context.Allocator, "server/cert", out var key))
                {
                    var djv = new DynamicJsonValue
                    {
                        ["Certificate"] = cert,
                        ["Thumbprint"] = x509Certificate.Thumbprint,
                        ["Confirmations"] = 0,
                        ["ReplaceImmediately"] = replaceImmediately
                    };

                    var json = context.ReadObject(djv, "server.cert.update.info");

                    var items = context.Transaction.InnerTransaction.OpenTable(ItemsSchema, Items);
                    UpdateValue(index, items, key, key, json);
                }
                // this will trigger the notification to the leader
                NotifyValueChanged(context, nameof(InstallUpdatedServerCertificateCommand), index);
            }
            catch (Exception e)
            {
                if (_parent.Log.IsOperationsEnabled)
                    _parent.Log.Operations($"{nameof(InstallUpdatedServerCertificate)} failed.", e);
                
                serverStore.NotificationCenter.Add(AlertRaised.Create(
                    null,
                    "Server certificate",
                    $"{nameof(InstallUpdatedServerCertificate)} failed.",
                    AlertType.Certificates_ReplaceError,
                    NotificationSeverity.Error,
                    "Cluster.Certificate.Replace.Error",
                    new ExceptionDetails(e)));
            }
        }

        private void RemoveNodeFromCluster(TransactionOperationContext context, BlittableJsonReaderObject cmd, long index, Leader leader)
        {
            var removed = JsonDeserializationCluster.RemoveNodeFromClusterCommand(cmd).RemovedNode;
            var items = context.Transaction.InnerTransaction.OpenTable(ItemsSchema, Items);

            foreach (var record in ReadAllDatabases(context))
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
                            NotifyDatabaseChanged(context, record.DatabaseName, index, nameof(RemoveNodeFromCluster));
                            continue;
                        }
                    }

                    if (record.Topology.RelevantFor(removed))
                    {
                        record.Topology.RemoveFromTopology(removed);
                        // Explict removing of the node means that we modify the replication factor
                        record.Topology.ReplicationFactor = record.Topology.Count;
                    }
                    var updated = EntityToBlittable.ConvertCommandToBlittable(record, context);

                    UpdateValue(index, items, lowerKey, key, updated);
                }

                NotifyDatabaseChanged(context, record.DatabaseName, index, nameof(RemoveNodeFromCluster));
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
                        new CommandExecutionException($"Cannot set typed value of type {type} for database {updateCommand.DatabaseName}, because it does not exist"));
                    return;
                }

                try
                {
                    updateCommand.Execute(context, items, index, record, _parent.CurrentState, out result);
                }
                catch (Exception e)
                {
                    NotifyLeaderAboutError(index, leader,
                        new CommandExecutionException($"Operation of type {type} for database {updateCommand.DatabaseName} has failed", e));
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
                    NotifyDatabaseChanged(context, databaseName, index, nameof(RemoveNodeFromDatabaseCommand));
                    return;
                }

                var updated = EntityToBlittable.ConvertCommandToBlittable(databaseRecord, context);

                UpdateValue(index, items, lowerKey, key, updated);
            }

            NotifyDatabaseChanged(context, databaseName, index, nameof(RemoveNodeFromDatabaseCommand));
        }

        private void DeleteDatabaseRecord(TransactionOperationContext context, long index, Table items, Slice lowerKey, string databaseName)
        {
            // delete database record
            items.DeleteByKey(lowerKey);

            // delete all values linked to database record - for subscription, etl etc.
            CleanupDatabaseRelatedValues(context, items, databaseName);
        }

        private void CleanupDatabaseRelatedValues(TransactionOperationContext context, Table items, string databaseName)
        {
            var dbValuesPrefix = Helpers.ClusterStateMachineValuesPrefix(databaseName).ToLowerInvariant();
            using (Slice.From(context.Allocator, dbValuesPrefix, out var loweredKey))
            {
                items.DeleteByPrimaryKeyPrefix(loweredKey);
            }

            DeleteTreeByPrefix(context, UpdateValueForDatabaseCommand.GetStorageKey(databaseName, null), Identities);

            using (Slice.From(context.Allocator, databaseName + "/", out var databaseSlice))
            {
                context.Transaction.InnerTransaction.OpenTable(CompareExchangeSchema, CompareExchange).DeleteByPrimaryKeyPrefix(databaseSlice);
            }
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
                using (var databaseRecordAsJson = EntityToBlittable.ConvertCommandToBlittable(addDatabaseCommand.Record, context))
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
                    SetDatabaseValues(addDatabaseCommand.DatabaseValues, addDatabaseCommand.Name, context, index, items);
                }
            }
            finally
            {
                NotifyDatabaseChanged(context, addDatabaseCommand.Name, index, nameof(AddDatabaseCommand));
            }
        }

        private static void SetDatabaseValues(
            Dictionary<string, ExpandoObject> databaseValues,
            string databaseName,
            TransactionOperationContext context,
            long index,
            Table items)
        {
            if (databaseValues == null)
                return;

            foreach (var keyValue in databaseValues)
            {
                if (keyValue.Key.StartsWith(PeriodicBackupStatus.Prefix, StringComparison.OrdinalIgnoreCase))
                {
                    // don't use the old backup status
                    continue;
                }

                var key = $"{Helpers.ClusterStateMachineValuesPrefix(databaseName)}{keyValue.Key}";
                using (Slice.From(context.Allocator, key, out Slice databaseValueName))
                using (Slice.From(context.Allocator, key.ToLowerInvariant(), out Slice databaseValueNameLowered))
                using (var value = EntityToBlittable.ConvertCommandToBlittable(keyValue.Value, context))
                {
                    UpdateValue(index, items, databaseValueNameLowered, databaseValueName, value);
                }
            }
        }

        private void DeleteValue(TransactionOperationContext context, string type, BlittableJsonReaderObject cmd, long index, Leader leader)
        {
            try
            {
                var delCmd = JsonDeserializationCluster.DeleteValueCommand(cmd);
                if (delCmd.Name.StartsWith("db/"))
                {
                    NotifyLeaderAboutError(index, leader,
                        new InvalidOperationException("Cannot delete " + delCmd.Name + " using DeleteValueCommand, only via dedicated database calls"));
                    return;
                }

                DeleteItem(context, delCmd.Name);
            }
            finally
            {
                NotifyValueChanged(context, type, index);
            }
        }

        public void DeleteItem(TransactionOperationContext context, string name)
        {
            var items = context.Transaction.InnerTransaction.OpenTable(ItemsSchema, Items);
            using (Slice.From(context.Allocator, name.ToLowerInvariant(), out Slice keyNameLowered))
            {
                items.DeleteByKey(keyNameLowered);
            }
        }

        private void DeleteMultipleValues(TransactionOperationContext context, string type, BlittableJsonReaderObject cmd, long index, Leader leader)
        {
            try
            {
                var items = context.Transaction.InnerTransaction.OpenTable(ItemsSchema, Items);
                var delCmd = JsonDeserializationCluster.DeleteMultipleValuesCommand(cmd);
                if (delCmd.Names.Any(name => name.StartsWith("db/")))
                {
                    NotifyLeaderAboutError(index, leader,
                        new InvalidOperationException("Cannot delete " + delCmd.Names + " using DeleteMultipleValuesCommand, only via dedicated database calls"));
                    return;
                }

                foreach (var name in delCmd.Names)
                {
                    using (Slice.From(context.Allocator, name, out Slice _))
                    using (Slice.From(context.Allocator, name.ToLowerInvariant(), out Slice keyNameLowered))
                    {
                        items.DeleteByKey(keyNameLowered);
                    }
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

        private T PutValue<T>(TransactionOperationContext context, string type, BlittableJsonReaderObject cmd, long index, Leader leader)
        {
            try
            {
                var items = context.Transaction.InnerTransaction.OpenTable(ItemsSchema, Items);
                var command = (PutValueCommand<T>)CommandBase.CreateFrom(cmd);
                if (command.Name.StartsWith(Constants.Documents.Prefix))
                {
                    NotifyLeaderAboutError(index, leader,
                        new InvalidOperationException("Cannot set " + command.Name + " using PutValueCommand, only via dedicated database calls"));
                }

                using (Slice.From(context.Allocator, command.Name, out Slice valueName))
                using (Slice.From(context.Allocator, command.Name.ToLowerInvariant(), out Slice valueNameLowered))
                using (var rec = context.ReadObject(command.ValueToJson(), "inner-val"))
                {
                    UpdateValue(index, items, valueNameLowered, valueName, rec);
                    return command.Value;
                }
            }
            finally
            {
                NotifyValueChanged(context, type, index);
            }
        }

        public override void EnsureNodeRemovalOnDeletion(TransactionOperationContext context, long term, string nodeTag)
        {
            var djv = new RemoveNodeFromClusterCommand
            {
                RemovedNode = nodeTag
            }.ToJson(context);
            
            var index = _parent.InsertToLeaderLog(context, term, context.ReadObject(djv, "remove"), RachisEntryFlags.StateMachineCommand);
            context.Transaction.InnerTransaction.LowLevelTransaction.OnDispose += tx =>
            {
                if (tx is LowLevelTransaction llt && llt.Committed)
                {
                    _parent.CurrentLeader.AddToEntries(index, null);
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

                    var updatedDatabaseBlittable = EntityToBlittable.ConvertCommandToBlittable(databaseRecord, context);
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
                   || slice.Content.Match(CompareExchange.Content)
                   || slice.Content.Match(Identities.Content);
        }

        public override void Initialize(RachisConsensus parent, TransactionOperationContext context)
        {
            base.Initialize(parent, context);
            ItemsSchema.Create(context.Transaction.InnerTransaction, Items, 32);
            CompareExchangeSchema.Create(context.Transaction.InnerTransaction, CompareExchange, 32);
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

        public void DeleteLocalState(TransactionOperationContext context, List<string> keys)
        {
            var localState = context.Transaction.InnerTransaction.CreateTree(LocalNodeStateTreeName);
            foreach (var key in keys)
            {
                localState.Delete(key);
            }
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

        public IEnumerable<(string ItemName, BlittableJsonReaderObject Value)> ItemsStartingWith(TransactionOperationContext context, string prefix, int start, int take)
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
        
        
        public BlittableJsonReaderObject GetItem(TransactionOperationContext context, string key)
        {
            var items = context.Transaction.InnerTransaction.OpenTable(ItemsSchema, Items);

            using (Slice.From(context.Allocator, key, out var k))
            {
                var tvh = new Table.TableValueHolder();
                if (items.ReadByKey(k, out tvh.Reader) == false)
                    return null;
                return GetCurrentItem(context, tvh).Item2;
            }
        }

        public void ExecuteCompareExchange(TransactionOperationContext context, string type, BlittableJsonReaderObject cmd, long index, out object result)
        {
            var items = context.Transaction.InnerTransaction.OpenTable(CompareExchangeSchema, CompareExchange);
            var compareExchange = (CompareExchangeCommandBase)JsonDeserializationCluster.Commands[type](cmd);
            result = compareExchange.Execute(context, items, index);
            OnTransactionDispose(context, index);
        }

        private void OnTransactionDispose(TransactionOperationContext context, long index)
        {
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
        
        public (long Index, BlittableJsonReaderObject Value) GetCompareExchangeValue(TransactionOperationContext context, string key)
        {
            var items = context.Transaction.InnerTransaction.OpenTable(CompareExchangeSchema, CompareExchange);
            var dbKey = key.ToLowerInvariant();
            using (Slice.From(context.Allocator, dbKey, out Slice keySlice))
            {
                if (items.ReadByKey(keySlice, out var reader))
                {
                    var index = ReadCompareExchangeIndex(reader);
                    var value = ReadCompareExchangeValue(context, reader);
                    return (index, value);
                }
                return (-1, null);
            }
        }
        
        public IEnumerable<(string Key, long Index, BlittableJsonReaderObject Value)> GetCompareExchangeValuesStartsWith(TransactionOperationContext context, 
            string dbName, string prefix, int start = 0, int pageSize = 1024)
        {
            var items = context.Transaction.InnerTransaction.OpenTable(CompareExchangeSchema, CompareExchange);
            var dbKey = prefix.ToLowerInvariant();
            using (Slice.From(context.Allocator, dbKey, out Slice keySlice))
            {
                foreach (var item in items.SeekByPrimaryKeyPrefix(keySlice, Slices.Empty, start))
                {
                    pageSize--;
                    var key = ReadCompareExchangeKey(item.Value.Reader, dbName);
                    var index = ReadCompareExchangeIndex(item.Value.Reader);
                    var value = ReadCompareExchangeValue(context, item.Value.Reader);
                    yield return (key, index, value);
                    
                    if(pageSize == 0)
                        yield break;
                }
            }
        }

        private static unsafe string ReadCompareExchangeKey(TableValueReader reader, string dbPrefix)
        {
            var ptr = reader.Read((int)UniqueItems.Key, out var size);
            return Encodings.Utf8.GetString(ptr, size).Substring(dbPrefix.Length + 1);
        }

        private static unsafe BlittableJsonReaderObject ReadCompareExchangeValue(TransactionOperationContext context, TableValueReader reader)
        {
            return new BlittableJsonReaderObject(reader.Read((int)UniqueItems.Value, out var size), size, context);
        }

        private static unsafe long ReadCompareExchangeIndex(TableValueReader reader)
        {
            return *(long*)reader.Read((int)UniqueItems.Index, out var _);
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

        private static unsafe (string, BlittableJsonReaderObject) GetCurrentItem(JsonOperationContext context, Table.TableValueHolder result)
        {
            var ptr = result.Reader.Read(2, out int size);
            var doc = new BlittableJsonReaderObject(ptr, size, context);

            var key = Encoding.UTF8.GetString(result.Reader.Read(1, out size), size);

            return (key, doc);
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

            var prefixString = UpdateValueForDatabaseCommand.GetStorageKey(databaseName, null);
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

        private static void DeleteTreeByPrefix<T>(TransactionOperationContext<T> context, string prefixString, Slice treeSlice, 
            RootObjectType type = RootObjectType.VariableSizeTree)
            where T : RavenTransaction
        {
            const int batchSize = 1024;
            var identities = context.Transaction.InnerTransaction.ReadTree(treeSlice, type);

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

        private static unsafe int GetDataAndEtagTupleFromReader(JsonOperationContext context, TableValueReader reader, out BlittableJsonReaderObject doc,
            out long etag)
        {
            var ptr = reader.Read(2, out int size);
            doc = new BlittableJsonReaderObject(ptr, size, context);

            etag = Bits.SwapBytes(*(long*)reader.Read(3, out size));
            Debug.Assert(size == sizeof(long));
            return size;
        }

        public override async Task<(Stream Stream, Action Disconnect)> ConnectToPeer(string url, X509Certificate2 certificate)
        {
            if (url == null)
                throw new ArgumentNullException(nameof(url));
            if (_parent == null)
                throw new InvalidOperationException("Cannot connect to peer without a parent");
            if (_parent.IsEncrypted && url.StartsWith("https:", StringComparison.OrdinalIgnoreCase) == false)
                throw new InvalidOperationException($"Failed to connect to node {url}. Connections from encrypted store must use HTTPS.");

            TcpConnectionInfo info;
            using (var cts = new CancellationTokenSource(_parent.TcpConnectionTimeout))
            {
                info = await ReplicationUtils.GetTcpInfoAsync(url, null, "Cluster", certificate, cts.Token);
            }

            TcpClient tcpClient = null;
            Stream stream = null;
            try
            {
                tcpClient = await TcpUtils.ConnectAsync(info.Url, _parent.TcpConnectionTimeout).ConfigureAwait(false);
                stream = await TcpUtils.WrapStreamWithSslAsync(tcpClient, info, _parent.ClusterCertificate, _parent.TcpConnectionTimeout);

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
                                //Kindly request the server to drop the connection
                                msg = new DynamicJsonValue
                                {
                                    [nameof(TcpConnectionHeaderMessage.DatabaseName)] = null,
                                    [nameof(TcpConnectionHeaderMessage.Operation)] = TcpConnectionHeaderMessage.OperationTypes.Drop,
                                    [nameof(TcpConnectionHeaderMessage.OperationVersion)] = TcpConnectionHeaderMessage.ClusterTcpVersion,
                                    [nameof(TcpConnectionHeaderMessage.Info)] = $"Couldn't agree on cluster tcp version ours:{TcpConnectionHeaderMessage.ClusterTcpVersion} theirs:{reply.Version}"
                                };
                                using (var writer = new BlittableJsonTextWriter(context, stream))
                                using (var msgJson = context.ReadObject(msg, "message"))
                                {
                                    context.Write(writer, msgJson);
                                }
                                throw new InvalidOperationException($"Unable to access  {url} because {reply.Message}");
                        }
                    }
                }
                return (stream, () =>
                {
                    {
                        try
                        {
                            tcpClient.Client.Disconnect(false);
                        }
                        catch (ObjectDisposedException ode)
                        {
                            //Happens, we don't really care at this point
                        }
                    }
                });
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

                var onValueChanged = ValueChanged;
                if (onValueChanged != null)
                {
                    TaskExecutor.Execute(_ =>
                    {
                        onValueChanged.Invoke(this, (lastIncludedIndex, "InstallUpdatedServerCertificateCommand"));
                    }, null);
                }
                context.Transaction.Commit();
            }

            // reload license can send a notification which will open a write tx
            serverStore.LicenseManager.ReloadLicense();
            AsyncHelpers.RunSync(() => serverStore.LicenseManager.CalculateLicenseLimits());

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

        public async Task WaitForIndexNotification(long index, TimeSpan timeout)
        {
            while (true)
            {
                // first get the task, then wait on it
                var waitAsync = _notifiedListeners.WaitAsync(timeout);

                if (index <= Interlocked.Read(ref LastModifiedIndex))
                    break;

                if (await waitAsync == false)
                {
                    var copy = Interlocked.Read(ref LastModifiedIndex);
                    if (index <= copy)
                        break;
                    ThrowTimeoutException(timeout, index, copy);
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

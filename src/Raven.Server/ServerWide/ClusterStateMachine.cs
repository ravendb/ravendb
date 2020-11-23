using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Net.Sockets;
using System.Runtime.CompilerServices;
using System.Runtime.ExceptionServices;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.Documents.Operations.Configuration;
using Raven.Client.Documents.Operations.OngoingTasks;
using Raven.Client.Documents.Operations.Replication;
using Raven.Client.Documents.Operations.TimeSeries;
using Raven.Client.Exceptions;
using Raven.Client.Exceptions.Database;
using Raven.Client.Exceptions.Documents.Subscriptions;
using Raven.Client.Exceptions.Security;
using Raven.Client.Http;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Commands;
using Raven.Client.ServerWide.Operations.Certificates;
using Raven.Client.ServerWide.Operations.Configuration;
using Raven.Client.ServerWide.Operations.OngoingTasks;
using Raven.Client.ServerWide.Tcp;
using Raven.Client.Util;
using Raven.Server.Commercial;
using Raven.Server.Config;
using Raven.Server.Documents;
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
using Raven.Server.ServerWide.Commands.Sorters;
using Raven.Server.ServerWide.Commands.Subscriptions;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow;
using Sparrow.Binary;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Logging;
using Sparrow.Server;
using Sparrow.Utils;
using Voron;
using Voron.Data;
using Voron.Data.BTrees;
using Voron.Data.Tables;
using Voron.Impl;
using Constants = Raven.Client.Constants;

namespace Raven.Server.ServerWide
{
    public class ClusterStateMachine : RachisStateMachine
    {
        private readonly Logger _clusterAuditLog = LoggingSource.AuditLog.GetLogger("ClusterStateMachine", "Audit");

        private const string LocalNodeStateTreeName = "LocalNodeState";
        private static readonly StringSegment DatabaseName = new StringSegment("DatabaseName");

        public static readonly TableSchema ItemsSchema;
        public static readonly TableSchema CompareExchangeSchema;
        public static readonly TableSchema CompareExchangeTombstoneSchema;
        public static readonly TableSchema TransactionCommandsSchema;
        public static readonly TableSchema IdentitiesSchema;
        public static readonly TableSchema CertificatesSchema;
        public static readonly TableSchema ReplicationCertificatesSchema;

        public class ServerWideConfigurationKey
        {
            public static string Backup = "server-wide/backup/configurations";

            public static string ExternalReplication = "server-wide/external-replication/configurations";

            public static string GetKeyByType(OngoingTaskType type)
            {
                switch (type)
                {
                    case OngoingTaskType.Replication:
                        return ExternalReplication;

                    case OngoingTaskType.Backup:
                        return Backup;

                    default:
                        throw new ArgumentOutOfRangeException(nameof(type), type, null);
                }
            }
        }

        public enum CompareExchangeTable
        {
            Key,
            Index,
            Value,
            PrefixIndex
        }

        public enum CompareExchangeTombstoneTable
        {
            Key,
            Index,
            PrefixIndex
        }

        public enum IdentitiesTable
        {
            Key,
            Value,
            Index,
            KeyIndex
        }

        public static readonly Slice Items;
        public static readonly Slice CompareExchange;
        public static readonly Slice CompareExchangeTombstones;
        public static readonly Slice Identities;
        public static readonly Slice IdentitiesIndex;
        public static readonly Slice TransactionCommands;
        public static readonly Slice TransactionCommandsCountPerDatabase;
        public static readonly Slice CompareExchangeIndex;
        public static readonly Slice CompareExchangeTombstoneIndex;
        public static readonly Slice CertificatesSlice;
        public static readonly Slice CertificatesHashSlice;
        public static readonly Slice ReplicationCertificatesSlice;
        public static readonly Slice ReplicationCertificatesHashSlice;

        public enum CertificatesTable
        {
            Thumbprint = 0,
            PublicKeyHash = 1,
            Data = 2
        }

        public enum ReplicationCertificatesTable
        {
            Thumbprint = 0,
            PublicKeyHash = 1,
            Certificate = 2,
            Access = 3
        }

        static ClusterStateMachine()
        {
            using (StorageEnvironment.GetStaticContext(out var ctx))
            {
                Slice.From(ctx, "Items", out Items);
                Slice.From(ctx, "CompareExchange", out CompareExchange);
                Slice.From(ctx, "CmpXchgTombstones", out CompareExchangeTombstones);
                Slice.From(ctx, "Identities", out Identities);
                Slice.From(ctx, "IdentitiesIndex", out IdentitiesIndex);
                Slice.From(ctx, "TransactionCommands", out TransactionCommands);
                Slice.From(ctx, "TransactionCommandsIndex", out TransactionCommandsCountPerDatabase);
                Slice.From(ctx, "CompareExchangeIndex", out CompareExchangeIndex);
                Slice.From(ctx, "CompareExchangeTombstoneIndex", out CompareExchangeTombstoneIndex);
                Slice.From(ctx, "CertificatesSlice", out CertificatesSlice);
                Slice.From(ctx, "CertificatesHashSlice", out CertificatesHashSlice);
                Slice.From(ctx, "ReplicationCertificatesSlice", out ReplicationCertificatesSlice);
                Slice.From(ctx, "ReplicationCertificatesHashSlice", out ReplicationCertificatesHashSlice);
            }

            ItemsSchema = new TableSchema();

            // We use the follow format for the items data
            // { lowered key, key, data, etag }
            ItemsSchema.DefineKey(new TableSchema.SchemaIndexDef
            {
                StartIndex = 0,
                Count = 1
            });

            IdentitiesSchema = new TableSchema();
            IdentitiesSchema.DefineKey(new TableSchema.SchemaIndexDef
            {
                StartIndex = (int)IdentitiesTable.Key,
                Count = 1
            });
            IdentitiesSchema.DefineIndex(new TableSchema.SchemaIndexDef
            {
                StartIndex = (int)IdentitiesTable.KeyIndex,
                Count = 1,
                IsGlobal = true,
                Name = IdentitiesIndex
            });

            CompareExchangeSchema = new TableSchema();
            CompareExchangeSchema.DefineKey(new TableSchema.SchemaIndexDef
            {
                StartIndex = (int)CompareExchangeTable.Key,
                Count = 1
            });
            CompareExchangeSchema.DefineIndex(new TableSchema.SchemaIndexDef
            {
                StartIndex = (int)CompareExchangeTable.PrefixIndex,
                Count = 1,
                Name = CompareExchangeIndex
            });

            CompareExchangeTombstoneSchema = new TableSchema();
            CompareExchangeTombstoneSchema.DefineKey(new TableSchema.SchemaIndexDef
            {
                StartIndex = (int)CompareExchangeTombstoneTable.Key,
                Count = 1
            });
            CompareExchangeTombstoneSchema.DefineIndex(new TableSchema.SchemaIndexDef
            {
                StartIndex = (int)CompareExchangeTombstoneTable.PrefixIndex,
                Count = 1,
                IsGlobal = true,
                Name = CompareExchangeTombstoneIndex
            });

            TransactionCommandsSchema = new TableSchema();
            TransactionCommandsSchema.DefineKey(new TableSchema.SchemaIndexDef()
            {
                StartIndex = 0,
                Count = 1, // Database, Separator, Commands count
            });

            // We use the follow format for the certificates data
            // { thumbprint, public key hash, data }
            CertificatesSchema = new TableSchema();
            CertificatesSchema.DefineKey(new TableSchema.SchemaIndexDef()
            {
                StartIndex = (int)CertificatesTable.Thumbprint,
                Count = 1,
                IsGlobal = false,
                Name = CertificatesSlice
            });
            CertificatesSchema.DefineIndex(new TableSchema.SchemaIndexDef
            {
                StartIndex = (int)CertificatesTable.PublicKeyHash,
                Count = 1,
                IsGlobal = false,
                Name = CertificatesHashSlice
            });

            // We use the follow format for the replication certificates data
            // { thumbprint, public key hash, data}
            ReplicationCertificatesSchema = new TableSchema();
            ReplicationCertificatesSchema.DefineKey(new TableSchema.SchemaIndexDef()
            {
                StartIndex = (int)ReplicationCertificatesTable.Thumbprint,
                Count = 1,
                IsGlobal = false,
                Name = ReplicationCertificatesSlice
            });
            ReplicationCertificatesSchema.DefineIndex(new TableSchema.SchemaIndexDef
            {
                StartIndex = (int)ReplicationCertificatesTable.PublicKeyHash,
                Count = 1,
                IsGlobal = false,
                Name = ReplicationCertificatesHashSlice
            });
        }

        private readonly RachisLogIndexNotifications _rachisLogIndexNotifications = new RachisLogIndexNotifications(CancellationToken.None);

        public override void Dispose()
        {
            base.Dispose();
            _rachisLogIndexNotifications.Dispose();
        }

        protected override void Apply(ClusterOperationContext context, BlittableJsonReaderObject cmd, long index, Leader leader, ServerStore serverStore)
        {
            if (cmd.TryGet("Type", out string type) == false)
            {
                NotifyLeaderAboutError(index, leader, new RachisApplyException("Cannot execute command, wrong format"));
                return;
            }

            ValidateGuid(cmd, type);
            object result = null;
            var sw = Stopwatch.StartNew();
            try
            {
                string errorMessage;
                switch (type)
                {
                    case nameof(ClusterTransactionCommand):
                        var errors = ExecuteClusterTransaction(context, cmd, index);
                        if (errors != null)
                        {
                            result = errors;
                            leader?.SetStateOf(index, errors);
                        }
                        break;

                    case nameof(CleanUpClusterStateCommand):
                        ClusterStateCleanUp(context, cmd, index);
                        break;

                    case nameof(PutSubscriptionBatchCommand):
                        ExecutePutSubscriptionBatch(context, cmd, index, type);
                        break;

                    case nameof(AddOrUpdateCompareExchangeBatchCommand):
                        ExecuteCompareExchangeBatch(context, cmd, index, type);
                        break;
                    //The reason we have a separate case for removing node from database is because we must
                    //actually delete the database before we notify about changes to the record otherwise we
                    //don't know that it was us who needed to delete the database.
                    case nameof(RemoveNodeFromDatabaseCommand):
                        RemoveNodeFromDatabase(context, cmd, index, leader, serverStore);
                        break;

                    case nameof(RemoveNodeFromClusterCommand):
                        RemoveNodeFromCluster(context, cmd, index, leader, serverStore);
                        break;

                    case nameof(DeleteCertificateFromClusterCommand):
                        DeleteCertificate(context, type, cmd, index);
                        break;

                    case nameof(DeleteValueCommand):
                        DeleteValue(context, type, cmd, index);
                        break;

                    case nameof(DeleteCertificateCollectionFromClusterCommand):
                        DeleteMultipleCertificates(context, type, cmd, index);
                        break;

                    case nameof(DeleteMultipleValuesCommand):
                        DeleteMultipleValues(context, type, cmd, index, leader);
                        break;

                    case nameof(CleanCompareExchangeTombstonesCommand):
                        ClearCompareExchangeTombstones(context, type, cmd, index, out var hasMore);
                        result = hasMore;
                        leader?.SetStateOf(index, hasMore);
                        break;

                    case nameof(DeleteExpiredCompareExchangeCommand):
                        result = DeleteExpiredCompareExchange(context, type, cmd, index);
                        leader?.SetStateOf(index, result);
                        break;

                    case nameof(IncrementClusterIdentityCommand):
                        if (ValidatePropertyExistence(cmd, nameof(IncrementClusterIdentityCommand), nameof(IncrementClusterIdentityCommand.Prefix), out errorMessage) == false)
                            throw new RachisApplyException(errorMessage);
                        SetValueForTypedDatabaseCommand(context, type, cmd, index, leader, out result);
                        leader?.SetStateOf(index, result);
                        SetIndexForBackup(context, cmd, index, type);
                        break;

                    case nameof(IncrementClusterIdentitiesBatchCommand):
                        if (ValidatePropertyExistence(cmd, nameof(IncrementClusterIdentitiesBatchCommand), nameof(IncrementClusterIdentitiesBatchCommand.DatabaseName), out errorMessage) == false)
                            throw new RachisApplyException(errorMessage);
                        SetValueForTypedDatabaseCommand(context, type, cmd, index, leader, out result);
                        leader?.SetStateOf(index, result);
                        SetIndexForBackup(context, cmd, index, type);
                        break;

                    case nameof(UpdateClusterIdentityCommand):
                        if (ValidatePropertyExistence(cmd, nameof(UpdateClusterIdentityCommand), nameof(UpdateClusterIdentityCommand.Identities), out errorMessage) == false)
                            throw new RachisApplyException(errorMessage);
                        SetValueForTypedDatabaseCommand(context, type, cmd, index, leader, out result);
                        leader?.SetStateOf(index, result);
                        SetIndexForBackup(context, cmd, index, type);
                        break;

                    case nameof(PutSortersCommand):
                    case nameof(DeleteSorterCommand):
                    case nameof(PutIndexCommand):
                    case nameof(PutIndexesCommand):
                    case nameof(PutAutoIndexCommand):
                    case nameof(DeleteIndexCommand):
                    case nameof(SetIndexLockCommand):
                    case nameof(SetIndexPriorityCommand):
                    case nameof(SetIndexStateCommand):
                    case nameof(EditTimeSeriesConfigurationCommand):
                    case nameof(EditRevisionsConfigurationCommand):
                    case nameof(EditRevisionsForConflictsConfigurationCommand):
                    case nameof(UpdatePeriodicBackupCommand):
                    case nameof(EditExpirationCommand):
                    case nameof(EditRefreshCommand):
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
                    case nameof(UpdatePullReplicationAsHubCommand):
                    case nameof(UpdatePullReplicationAsSinkCommand):
                    case nameof(EditDatabaseClientConfigurationCommand):
                    case nameof(EditDocumentsCompressionCommand):
                    case nameof(UpdateUnusedDatabaseIdsCommand):
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
                        ExecuteCompareExchange(context, type, cmd, index, out result);
                        leader?.SetStateOf(index, result);
                        SetIndexForBackup(context, cmd, index, type);
                        break;

                    case nameof(InstallUpdatedServerCertificateCommand):
                        InstallUpdatedServerCertificate(context, cmd, index, serverStore);
                        break;

                    case nameof(RecheckStatusOfServerCertificateCommand):
                        if (_parent.Log.IsOperationsEnabled)
                            _parent.Log.Operations($"Received {nameof(RecheckStatusOfServerCertificateCommand)}, index = {index}.");
                        NotifyValueChanged(context, type, index); // just need to notify listeners
                        break;

                    case nameof(ConfirmReceiptServerCertificateCommand):
                        ConfirmReceiptServerCertificate(context, cmd, index, serverStore);
                        break;

                    case nameof(RecheckStatusOfServerCertificateReplacementCommand):
                        if (_parent.Log.IsOperationsEnabled)
                            _parent.Log.Operations($"Received {nameof(RecheckStatusOfServerCertificateReplacementCommand)}, index = {index}.");
                        NotifyValueChanged(context, type, index); // just need to notify listeners
                        break;

                    case nameof(ConfirmServerCertificateReplacedCommand):
                        ConfirmServerCertificateReplaced(context, cmd, index, serverStore);
                        break;

                    case nameof(BulkRegisterReplicationHubAccessCommand):
                        BulkPutReplicationCertificate(context, type, cmd, index, serverStore);
                        break;

                    case nameof(RegisterReplicationHubAccessCommand):
                        PutReplicationCertificate(context, type, cmd, index, serverStore);
                        break;

                    case nameof(UnregisterReplicationHubAccessCommand):
                        RemoveReplicationCertificate(context, type, cmd, index, serverStore);
                        break;

                    case nameof(UpdateSnmpDatabasesMappingCommand):
                        UpdateValue<List<string>>(context, type, cmd, index);
                        break;

                    case nameof(PutLicenseCommand):
                        PutValue<License>(context, type, cmd, index);
                        break;

                    case nameof(PutLicenseLimitsCommand):
                        PutValue<LicenseLimits>(context, type, cmd, index);
                        break;

                    case nameof(UpdateLicenseLimitsCommand):
                        UpdateValue<NodeLicenseLimits>(context, type, cmd, index);
                        break;

                    case nameof(ToggleDatabasesStateCommand):
                        ToggleDatabasesState(cmd, context, type, index);
                        break;

                    case nameof(PutServerWideBackupConfigurationCommand):
                        var serverWideBackupConfiguration = UpdateValue<ServerWideBackupConfiguration>(context, type, cmd, index, skipNotifyValueChanged: true);
                        UpdateDatabasesWithServerWideBackupConfiguration(context, type, serverWideBackupConfiguration);
                        break;

                    case nameof(DeleteServerWideBackupConfigurationCommand):
                        UpdateValue<string>(context, type, cmd, index, skipNotifyValueChanged: true);
                        cmd.TryGet(nameof(DeleteServerWideBackupConfigurationCommand.Name), out string name);
                        var deleteServerWideTaskConfiguration = new DeleteServerWideTaskCommand.DeleteConfiguration
                        {
                            TaskName = name,
                            Type = OngoingTaskType.Backup
                        };
                        DeleteServerWideBackupConfigurationFromAllDatabases(deleteServerWideTaskConfiguration, context, type, index);
                        break;

                    case nameof(PutServerWideExternalReplicationCommand):
                        var serverWideExternalReplication = UpdateValue<ServerWideExternalReplication>(context, type, cmd, index, skipNotifyValueChanged: true);
                        UpdateDatabasesWithExternalReplication(context, type, serverWideExternalReplication);
                        break;

                    case nameof(DeleteServerWideTaskCommand):
                        var deleteConfiguration = UpdateValue<DeleteServerWideTaskCommand.DeleteConfiguration>(context, type, cmd, index, skipNotifyValueChanged: true);
                        DeleteServerWideBackupConfigurationFromAllDatabases(deleteConfiguration, context, type, index);
                        break;

                    case nameof(ToggleServerWideTaskStateCommand):
                        var parameters = UpdateValue<ToggleServerWideTaskStateCommand.Parameters>(context, type, cmd, index, skipNotifyValueChanged: true);
                        ToggleServerWideTaskState(cmd, parameters, context, type);
                        break;

                    case nameof(PutCertificateWithSamePinningHashCommand):
                        PutCertificate(context, type, cmd, index, serverStore);
                        if (cmd.TryGet(nameof(PutCertificateWithSamePinningHashCommand.Name), out string thumbprint))
                            DeleteLocalState(context, thumbprint);
                        if (cmd.TryGet(nameof(PutCertificateWithSamePinningHashCommand.PublicKeyPinningHash), out string hash))
                            DiscardLeftoverCertsWithSamePinningHash(context, hash, type, index);
                        break;

                    case nameof(PutCertificateCommand):
                        PutCertificate(context, type, cmd, index, serverStore);
                        // Once the certificate is in the cluster, no need to keep it locally so we delete it.
                        if (cmd.TryGet(nameof(PutCertificateCommand.Name), out string key))
                            DeleteLocalState(context, key);
                        break;

                    case nameof(PutClientConfigurationCommand):
                        PutValue<ClientConfiguration>(context, type, cmd, index);
                        break;

                    case nameof(PutServerWideStudioConfigurationCommand):
                        PutValue<ServerWideStudioConfiguration>(context, type, cmd, index);
                        break;

                    case nameof(AddDatabaseCommand):
                        var addedNodes = AddDatabase(context, cmd, index, leader);
                        if (addedNodes != null)
                        {
                            result = addedNodes;
                            leader?.SetStateOf(index, addedNodes);
                        }
                        break;

                    default:
                        var massage = $"The command '{type}' is unknown and cannot be executed on server with version '{ServerVersion.FullVersion}'.{Environment.NewLine}" +
                                      "Updating this node version to match the rest should resolve this issue.";
                        throw new UnknownClusterCommand(massage);
                }

                _parent.LogHistory.UpdateHistoryLog(context, index, _parent.CurrentTerm, cmd, result, null);
            }
            catch (Exception e) when (ExpectedException(e))
            {
                if (_parent.Log.IsInfoEnabled)
                    _parent.Log.Info($"Failed to execute command of type '{type}' on database '{DatabaseName}'", e);

                _parent.LogHistory.UpdateHistoryLog(context, index, _parent.CurrentTerm, cmd, null, e);
                NotifyLeaderAboutError(index, leader, e);
            }
            catch (Exception e)
            {
                // IMPORTANT
                // Other exceptions MUST be consistent across the cluster (meaning: if it occured on one node it must occur on the rest also).
                // the exceptions here are machine specific and will cause a jam in the state machine until the exception will be resolved.
                var error = $"Unrecoverable exception on database '{DatabaseName}' at command type '{type}', execution will be retried later.";
                if (_parent.Log.IsOperationsEnabled)
                    _parent.Log.Operations(error, e);

                AddUnrecoverableNotification(serverStore, error, e);
                NotifyLeaderAboutError(index, leader, e);
                throw;
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

            static void AddUnrecoverableNotification(ServerStore serverStore, string error, Exception exception)
            {
                try
                {
                    serverStore.NotificationCenter.Add(AlertRaised.Create(
                        null,
                        "Unrecoverable Cluster Error",
                        error,
                        AlertType.UnrecoverableClusterError,
                        NotificationSeverity.Error,
                        details: new ExceptionDetails(exception)));
                }
                catch
                {
                    // nothing we can do here
                }
            }
        }

        private void ExecutePutSubscriptionBatch(ClusterOperationContext context, BlittableJsonReaderObject cmd, long index, string type)
        {
            if (cmd.TryGet(nameof(PutSubscriptionBatchCommand.Commands), out BlittableJsonReaderArray subscriptionCommands) == false)
            {
                throw new RachisApplyException($"'{nameof(PutSubscriptionBatchCommand.Commands)}' is missing in '{nameof(PutSubscriptionBatchCommand)}'.");
            }

            PutSubscriptionCommand updateCommand = null;
            Exception exception = null;
            var actionsByDatabase = new Dictionary<string, List<Func<Task>>>();
            var items = context.Transaction.InnerTransaction.OpenTable(ItemsSchema, Items);
            try
            {
                foreach (BlittableJsonReaderObject command in subscriptionCommands)
                {
                    if (command.TryGet("Type", out string putSubscriptionType) == false && putSubscriptionType != nameof(PutSubscriptionCommand))
                    {
                        throw new RachisApplyException($"Cannot execute {type} command, wrong format");
                    }

                    updateCommand = (PutSubscriptionCommand)JsonDeserializationCluster.Commands[nameof(PutSubscriptionCommand)](command);

                    var database = updateCommand.DatabaseName;
                    if (DatabaseExists(context, database) == false)
                    {
                        throw new DatabaseDoesNotExistException(
                            $"Cannot set typed value of type {type} for database {database}, because it does not exist");
                    }

                    var id = updateCommand.FindFreeId(context, index);
                    updateCommand.Execute(context, items, id, record: null, _parent.CurrentState, out _);

                    if (actionsByDatabase.ContainsKey(database) == false)
                    {
                        actionsByDatabase[database] = new List<Func<Task>>();
                    }

                    actionsByDatabase[database].Add(() =>
                        Changes.OnDatabaseChanges(database, index, nameof(PutSubscriptionCommand), DatabasesLandlord.ClusterDatabaseChangeType.ValueChanged, null));
                }

                foreach (var action in actionsByDatabase)
                {
                    ExecuteManyOnDispose(context, index, type, action.Value);
                }
            }
            catch (Exception e)
            {
                exception = e;
                throw;
            }
            finally
            {
                LogCommand(type, index, exception, updateCommand?.AdditionalDebugInformation(exception));
            }
        }

        private void SetIndexForBackup(ClusterOperationContext context, BlittableJsonReaderObject bjro, long index, string type)
        {
            if (index < 0)
                return;

            if ((bjro.TryGet(nameof(CompareExchangeCommandBase.Database), out string databaseName) == false ||
                 string.IsNullOrEmpty(databaseName)) && type.Equals(nameof(AddOrUpdateCompareExchangeCommand)))
                throw new RachisApplyException($"Command {type} must contain a DatabaseName property. Index {index}");

            var dbKey = $"db/{databaseName}";
            var items = context.Transaction.InnerTransaction.OpenTable(ItemsSchema, Items);

            using (Slice.From(context.Allocator, dbKey, out Slice key))
            using (Slice.From(context.Allocator, dbKey.ToLowerInvariant(), out Slice keyLowered))
            {
                var databaseRecordJson = ReadInternal(context, out _, keyLowered);
                if (databaseRecordJson == null)
                    return;

                databaseRecordJson.Modifications = new DynamicJsonValue { [nameof(DatabaseRecord.EtagForBackup)] = index };

                using (var old = databaseRecordJson)
                {
                    databaseRecordJson = context.ReadObject(databaseRecordJson, dbKey);
                }

                UpdateValue(index, items, keyLowered, key, databaseRecordJson);
            }
        }

        [Conditional("DEBUG")]
        private void ValidateGuid(BlittableJsonReaderObject cmd, string type)
        {
            if (_parent.InMemoryDebug.IsInterVersionTest)
                return;

            if (cmd.TryGet(nameof(CommandBase.UniqueRequestId), out string guid) == false)
            {
                throw new ArgumentNullException($"Guid is not provided in the command {type}.");
            }
        }

        private void ExecuteCompareExchangeBatch(ClusterOperationContext context, BlittableJsonReaderObject cmd, long index, string commandType)
        {
            CompareExchangeCommandBase compareExchange = null;
            Exception exception = null;
            try
            {
                var hasAddCommands = cmd.TryGet(nameof(AddOrUpdateCompareExchangeBatchCommand.Commands), out BlittableJsonReaderArray addCommands);
                var hasRemoveCommands = cmd.TryGet(nameof(AddOrUpdateCompareExchangeBatchCommand.RemoveCommands), out BlittableJsonReaderArray removeCommands);

                if (hasAddCommands == false && hasRemoveCommands == false)
                {
                    throw new RachisApplyException($"'{nameof(AddOrUpdateCompareExchangeBatchCommand.Commands)}' and '{nameof(AddOrUpdateCompareExchangeBatchCommand.RemoveCommands)}' are missing in '{nameof(AddOrUpdateCompareExchangeBatchCommand)}'.");
                }

                var items = context.Transaction.InnerTransaction.OpenTable(CompareExchangeSchema, CompareExchange);

                if (hasAddCommands)
                {
                    foreach (BlittableJsonReaderObject command in addCommands)
                    {
                        if (command.TryGet("Type", out string type) == false || type != nameof(AddOrUpdateCompareExchangeCommand))
                        {
                            throw new RachisApplyException($"Cannot execute {commandType} command, wrong format");
                        }

                        compareExchange = (CompareExchangeCommandBase)JsonDeserializationCluster.Commands[type](command);
                        compareExchange.Execute(context, items, index);
                        SetIndexForBackup(context, command, index, type);
                    }
                }

                if (hasRemoveCommands)
                {
                    foreach (BlittableJsonReaderObject command in removeCommands)
                    {
                        if (command.TryGet("Type", out string type) == false || type != nameof(RemoveCompareExchangeCommand))
                        {
                            throw new RachisApplyException($"Cannot execute {commandType} command, wrong format");
                        }

                        compareExchange = (CompareExchangeCommandBase)JsonDeserializationCluster.Commands[type](command);
                        compareExchange.Execute(context, items, index);
                        SetIndexForBackup(context, command, index, type);
                    }
                }

                OnTransactionDispose(context, index);
            }
            catch (Exception e)
            {
                exception = e;
                throw;
            }
            finally
            {
                LogCommand(commandType, index, exception, compareExchange?.AdditionalDebugInformation(exception));
            }
        }

        public static bool ExpectedException(Exception e)
        {
            return e is RachisException ||
                   e is SubscriptionException ||
                   e is DatabaseDoesNotExistException ||
                   e is AuthorizationException ||
                   e is CompareExchangeKeyTooBigException;
        }

        private void ClusterStateCleanUp(ClusterOperationContext context, BlittableJsonReaderObject cmd, long index)
        {
            Exception exception = null;
            CleanUpClusterStateCommand cleanCommand = null;
            try
            {
                var items = context.Transaction.InnerTransaction.OpenTable(ItemsSchema, Items);
                cleanCommand = (CleanUpClusterStateCommand)JsonDeserializationCluster.Commands[nameof(CleanUpClusterStateCommand)](cmd);
                var affectedDatabases = cleanCommand.Clean(context, index);
                foreach (var tuple in affectedDatabases)
                {
                    var dbKey = $"db/{tuple.Key}";
                    using (Slice.From(context.Allocator, dbKey, out Slice valueName))
                    using (Slice.From(context.Allocator, dbKey.ToLowerInvariant(), out Slice valueNameLowered))
                    using (var databaseRecord = ReadRawDatabaseRecord(context, tuple.Key))
                    {
                        if (databaseRecord == null)
                            continue;

                        var databaseRecordJson = databaseRecord.Raw;
                        databaseRecordJson.Modifications = new DynamicJsonValue
                        {
                            [nameof(DatabaseRecord.TruncatedClusterTransactionCommandsCount)] = tuple.Value
                        };
                        var newDatabaseRecordJson = context.ReadObject(databaseRecordJson, dbKey);

                        UpdateValue(index, items, valueNameLowered, valueName, newDatabaseRecordJson);
                    }

                    // we simply update the value without invoking the OnChange function
                }

                OnTransactionDispose(context, index);
            }
            catch (Exception e)
            {
                exception = e;
                throw;
            }
            finally
            {
                LogCommand(nameof(CleanUpClusterStateCommand), index, exception, cleanCommand?.AdditionalDebugInformation(exception));
            }
        }

        private List<string> ExecuteClusterTransaction(ClusterOperationContext context, BlittableJsonReaderObject cmd, long index)
        {
            ClusterTransactionCommand clusterTransaction = null;
            Exception exception = null;
            try
            {
                clusterTransaction = (ClusterTransactionCommand)JsonDeserializationCluster.Commands[nameof(ClusterTransactionCommand)](cmd);
                UpdateDatabaseRecordId(context, index, clusterTransaction);

                var compareExchangeItems = context.Transaction.InnerTransaction.OpenTable(CompareExchangeSchema, CompareExchange);
                var error = clusterTransaction.ExecuteCompareExchangeCommands(context, index, compareExchangeItems);
                if (error == null)
                {
                    clusterTransaction.SaveCommandsBatch(context, index);
                    var notify = clusterTransaction.HasDocumentsInTransaction
                        ? DatabasesLandlord.ClusterDatabaseChangeType.PendingClusterTransactions
                        : DatabasesLandlord.ClusterDatabaseChangeType.ClusterTransactionCompleted;

                    NotifyDatabaseAboutChanged(context, clusterTransaction.DatabaseName, index, nameof(ClusterTransactionCommand), notify, null);

                    return null;
                }

                OnTransactionDispose(context, index);
                return error;
            }
            catch (Exception e)
            {
                exception = e;
                throw;
            }
            finally
            {
                LogCommand(nameof(ClusterTransactionCommand), index, exception, clusterTransaction?.AdditionalDebugInformation(exception));
            }
        }

        private void UpdateDatabaseRecordId(ClusterOperationContext context, long index, ClusterTransactionCommand clusterTransaction)
        {
            var rawRecord = ReadRawDatabaseRecord(context, clusterTransaction.DatabaseName);

            if (rawRecord == null)
                throw DatabaseDoesNotExistException.CreateWithMessage(clusterTransaction.DatabaseName, $"Could not execute update command of type '{nameof(ClusterTransactionCommand)}'.");

            var topology = rawRecord.Topology;
            if (topology.DatabaseTopologyIdBase64 == null)
            {
                var items = context.Transaction.InnerTransaction.OpenTable(ItemsSchema, Items);
                var databaseRecordJson = rawRecord.Raw;
                topology.DatabaseTopologyIdBase64 = clusterTransaction.DatabaseRecordId;
                var dbKey = $"db/{clusterTransaction.DatabaseName}";
                using (Slice.From(context.Allocator, dbKey, out var valueName))
                using (Slice.From(context.Allocator, dbKey.ToLowerInvariant(), out var valueNameLowered))
                {
                    databaseRecordJson.Modifications = new DynamicJsonValue
                    {
                        [nameof(DatabaseRecord.Topology)] = topology.ToJson()
                    };

                    using (var old = databaseRecordJson)
                    {
                        databaseRecordJson = context.ReadObject(databaseRecordJson, dbKey);
                    }

                    UpdateValue(index, items, valueNameLowered, valueName, databaseRecordJson);
                }
            }
        }

        private void ConfirmReceiptServerCertificate(ClusterOperationContext context, BlittableJsonReaderObject cmd, long index, ServerStore serverStore)
        {
            if (_parent.Log.IsOperationsEnabled)
                _parent.Log.Operations($"Received {nameof(ConfirmReceiptServerCertificateCommand)}, index = {index}.");
            try
            {
                var items = context.Transaction.InnerTransaction.OpenTable(ItemsSchema, Items);
                using (Slice.From(context.Allocator, CertificateReplacement.CertificateReplacementDoc, out var key))
                {
                    if (cmd.TryGet(nameof(ConfirmReceiptServerCertificateCommand.Thumbprint), out string thumbprint) == false)
                    {
                        throw new RachisApplyException($"{nameof(ConfirmReceiptServerCertificateCommand.Thumbprint)} property didn't exist in {nameof(ConfirmReceiptServerCertificateCommand)}");
                    }
                    var certInstallation = GetItem(context, CertificateReplacement.CertificateReplacementDoc);
                    if (certInstallation == null)
                        return; // already applied?

                    if (certInstallation.TryGet(nameof(CertificateReplacement.Thumbprint), out string storedThumbprint) == false)
                        throw new RachisApplyException($"{nameof(CertificateReplacement.Thumbprint)} property didn't exist in 'server/cert' value");

                    if (storedThumbprint != thumbprint)
                        return; // confirmation for a different cert, ignoring

                    certInstallation.TryGet(nameof(CertificateReplacement.Confirmations), out int confirmations);

                    certInstallation.Modifications = new DynamicJsonValue(certInstallation)
                    {
                        [nameof(CertificateReplacement.Confirmations)] = confirmations + 1
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
                    _parent.Log.Operations($"{nameof(ConfirmReceiptServerCertificate)} failed (index = {index}).", e);

                serverStore.NotificationCenter.Add(AlertRaised.Create(
                    null,
                    CertificateReplacement.CertReplaceAlertTitle,
                    "Failed to confirm receipt of the new certificate.",
                    AlertType.Certificates_ReplaceError,
                    NotificationSeverity.Error,
                    details: new ExceptionDetails(e)));

                throw;
            }
        }

        private void InstallUpdatedServerCertificate(ClusterOperationContext context, BlittableJsonReaderObject cmd, long index, ServerStore serverStore)
        {
            if (_parent.Log.IsOperationsEnabled)
                _parent.Log.Operations($"Received {nameof(InstallUpdatedServerCertificateCommand)}.");
            Exception exception = null;
            try
            {
                if (cmd.TryGet(nameof(InstallUpdatedServerCertificateCommand.Certificate), out string cert) == false || string.IsNullOrEmpty(cert))
                {
                    throw new RachisApplyException($"{nameof(InstallUpdatedServerCertificateCommand.Certificate)} property didn't exist in {nameof(InstallUpdatedServerCertificateCommand)}");
                }

                cmd.TryGet(nameof(InstallUpdatedServerCertificateCommand.ReplaceImmediately), out bool replaceImmediately);

                var x509Certificate = new X509Certificate2(Convert.FromBase64String(cert), (string)null, X509KeyStorageFlags.MachineKeySet);
                // we assume that this is valid, and we don't check dates, since that would introduce external factor to the state machine, which is not allowed
                using (Slice.From(context.Allocator, CertificateReplacement.CertificateReplacementDoc, out var key))
                {
                    var djv = new DynamicJsonValue
                    {
                        [nameof(CertificateReplacement.Certificate)] = cert,
                        [nameof(CertificateReplacement.Thumbprint)] = x509Certificate.Thumbprint,
                        [nameof(CertificateReplacement.OldThumbprint)] = serverStore.Server.Certificate.Certificate.Thumbprint,
                        [nameof(CertificateReplacement.Confirmations)] = 0,
                        [nameof(CertificateReplacement.Replaced)] = 0,
                        [nameof(CertificateReplacement.ReplaceImmediately)] = replaceImmediately
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
                exception = e;
                if (_parent.Log.IsOperationsEnabled)
                    _parent.Log.Operations($"{nameof(InstallUpdatedServerCertificateCommand)} failed (index = {index}).", e);

                serverStore.NotificationCenter.Add(AlertRaised.Create(
                    null,
                    CertificateReplacement.CertReplaceAlertTitle,
                    $"{nameof(InstallUpdatedServerCertificateCommand)} failed.",
                    AlertType.Certificates_ReplaceError,
                    NotificationSeverity.Error,
                    details: new ExceptionDetails(e)));

                throw;
            }
            finally
            {
                LogCommand(nameof(InstallUpdatedServerCertificateCommand), index, exception);
            }
        }

        private void ConfirmServerCertificateReplaced(ClusterOperationContext context, BlittableJsonReaderObject cmd, long index, ServerStore serverStore)
        {
            if (_parent.Log.IsOperationsEnabled)
                _parent.Log.Operations($"Received {nameof(ConfirmServerCertificateReplacedCommand)}, index = {index}.");
            try
            {
                var items = context.Transaction.InnerTransaction.OpenTable(ItemsSchema, Items);
                using (Slice.From(context.Allocator, CertificateReplacement.CertificateReplacementDoc, out var key))
                {
                    if (cmd.TryGet(nameof(ConfirmServerCertificateReplacedCommand.Thumbprint), out string thumbprint) == false)
                    {
                        throw new RachisApplyException($"{nameof(ConfirmServerCertificateReplacedCommand.Thumbprint)} property didn't exist in {nameof(ConfirmServerCertificateReplacedCommand)}");
                    }
                    if (cmd.TryGet(nameof(ConfirmServerCertificateReplacedCommand.OldThumbprint), out string oldThumbprintFromCommand) == false)
                    {
                        throw new RachisApplyException($"{nameof(ConfirmServerCertificateReplacedCommand.OldThumbprint)} property didn't exist in {nameof(ConfirmServerCertificateReplacedCommand)}");
                    }

                    var certInstallation = GetItem(context, CertificateReplacement.CertificateReplacementDoc);
                    if (certInstallation == null)
                        return; // already applied?

                    if (certInstallation.TryGet(nameof(CertificateReplacement.Thumbprint), out string storedThumbprint) == false)
                        throw new RachisApplyException($"'{nameof(CertificateReplacement.Thumbprint)}' property didn't exist in 'server/cert' value");

                    if (storedThumbprint != thumbprint)
                        return; // confirmation for a different cert, ignoring

                    // If "Replaced" or "OldThumbprint" are not there, it means this node started the replacement process with a lower version and was then upgraded.
                    // No worries, it got the command now and it can join the confirmation process which is still happening. Let's synchronize the 'server/cert' doc
                    // to have the new properties:
                    if (certInstallation.TryGet(nameof(CertificateReplacement.Replaced), out int replaced) == false)
                        replaced = 0;

                    if (certInstallation.TryGet(nameof(CertificateReplacement.OldThumbprint), out string oldThumbprint) == false)
                    {
                        oldThumbprint = oldThumbprintFromCommand;
                        certInstallation.Modifications = new DynamicJsonValue(certInstallation)
                        {
                            [nameof(CertificateReplacement.OldThumbprint)] = oldThumbprint
                        };
                    }

                    certInstallation.Modifications = new DynamicJsonValue(certInstallation)
                    {
                        [nameof(CertificateReplacement.Replaced)] = replaced + 1
                    };

                    certInstallation = context.ReadObject(certInstallation, "server.cert.update");

                    UpdateValue(index, items, key, key, certInstallation);

                    if (_parent.Log.IsOperationsEnabled)
                        _parent.Log.Operations($"Confirming that certificate replacement has happened. Old certificate thumbprint: '{oldThumbprint}'. New certificate thumbprint: '{thumbprint}'.");

                    // this will trigger the deletion of the new and old server certs from the cluster
                    NotifyValueChanged(context, nameof(ConfirmServerCertificateReplacedCommand), index);
                }
            }
            catch (Exception e)
            {
                if (_parent.Log.IsOperationsEnabled)
                    _parent.Log.Operations($"{nameof(ConfirmServerCertificateReplaced)} failed (index = {index}).", e);

                serverStore.NotificationCenter.Add(AlertRaised.Create(
                    null,
                    CertificateReplacement.CertReplaceAlertTitle,
                    "Failed to confirm replacement of the new certificate.",
                    AlertType.Certificates_ReplaceError,
                    NotificationSeverity.Error,
                    details: new ExceptionDetails(e)));

                throw;
            }
        }

        private void RemoveNodeFromCluster(ClusterOperationContext context, BlittableJsonReaderObject cmd, long index, Leader leader, ServerStore serverStore)
        {
            RemoveNodeFromClusterCommand removedCmd = null;
            Exception exception = null;
            try
            {
                removedCmd = JsonDeserializationCluster.RemoveNodeFromClusterCommand(cmd);
                var removed = removedCmd.RemovedNode;
                var items = context.Transaction.InnerTransaction.OpenTable(ItemsSchema, Items);

                var tasks = new List<Func<Task>>();

                foreach (var record in GetAllDatabases(context))
                {
                    using (Slice.From(context.Allocator, "db/" + record.DatabaseName.ToLowerInvariant(), out Slice lowerKey))
                    using (Slice.From(context.Allocator, "db/" + record.DatabaseName, out Slice key))
                    {
                        if (record.DeletionInProgress != null)
                        {
                            // delete immediately if this node was removed.
                            var deleteNow = record.DeletionInProgress.Remove(removed) && _parent.Tag == removed;
                            if (record.DeletionInProgress.Count == 0 && record.Topology.Count == 0 || deleteNow)
                            {
                                DeleteDatabaseRecord(context, index, items, lowerKey, record.DatabaseName, serverStore);
                                tasks.Add(() => Changes.OnDatabaseChanges(record.DatabaseName, index, nameof(RemoveNodeFromCluster),
                                    DatabasesLandlord.ClusterDatabaseChangeType.RecordChanged, null));

                                continue;
                            }
                        }

                        if (record.Topology.RelevantFor(removed))
                        {
                            record.Topology.RemoveFromTopology(removed);
                            // Explicit removing of the node means that we modify the replication factor
                            record.Topology.ReplicationFactor = record.Topology.Count;
                            if (record.Topology.Count == 0)
                            {
                                DeleteDatabaseRecord(context, index, items, lowerKey, record.DatabaseName, serverStore);
                                continue;
                            }
                        }

                        var updated = DocumentConventions.DefaultForServer.Serialization.DefaultConverter.ToBlittable(record, context);

                        UpdateValue(index, items, lowerKey, key, updated);
                    }

                    tasks.Add(() => Changes.OnDatabaseChanges(record.DatabaseName, index, nameof(RemoveNodeFromCluster), DatabasesLandlord.ClusterDatabaseChangeType.RecordChanged, null));
                }

                // delete the node license limits
                var licenseLimitsBlittable = Read(context, ServerStore.LicenseLimitsStorageKey, out _);
                if (licenseLimitsBlittable != null)
                {
                    var licenseLimits = JsonDeserializationServer.LicenseLimits(licenseLimitsBlittable);
                    licenseLimits.NodeLicenseDetails.Remove(removed);
                    var value = context.ReadObject(licenseLimits.ToJson(), "overwrite-license-limits");
                    PutValueDirectly(context, ServerStore.LicenseLimitsStorageKey, value, index);
                }

                ExecuteManyOnDispose(context, index, nameof(RemoveNodeFromCluster), tasks);
            }
            catch (Exception e)
            {
                exception = e;
                throw;
            }
            finally
            {
                LogCommand(nameof(RemoveNodeFromClusterCommand), index, exception, removedCmd?.AdditionalDebugInformation(exception));
            }
        }

        private void ExecuteManyOnDispose(ClusterOperationContext context, long index, string type, List<Func<Task>> tasks)
        {
            context.Transaction.InnerTransaction.LowLevelTransaction.AfterCommitWhenNewReadTransactionsPrevented += _ =>
            {
                _rachisLogIndexNotifications.AddTask(index);
                var count = tasks.Count;

                if (count == 0)
                {
                    NotifyAndSetCompleted(index);
                    return;
                }

                context.Transaction.InnerTransaction.LowLevelTransaction.OnDispose += tx =>
                {
                    var exceptionAggregator =
                        new ExceptionAggregator(_parent.Log, $"the raft index {index} is committed, but an error occured during executing the {type} command.");

                    foreach (var task in tasks)
                    {
                        Task.Run(async () =>
                        {
                            await exceptionAggregator.ExecuteAsync(task());
                            if (Interlocked.Decrement(ref count) == 0)
                            {
                                Exception error = null;
                                try
                                {
                                    exceptionAggregator.ThrowIfNeeded();
                                }
                                catch (Exception e)
                                {
                                    error = e;
                                }
                                finally
                                {
                                    _rachisLogIndexNotifications.NotifyListenersAbout(index, error);
                                    _rachisLogIndexNotifications.SetTaskCompleted(index, error);
                                }
                            }
                        });
                    }
                };
            };
        }

        private void NotifyAndSetCompleted(long index)
        {
            try
            {
                _rachisLogIndexNotifications.NotifyListenersAbout(index, null);
                _rachisLogIndexNotifications.SetTaskCompleted(index, null);
            }
            catch (OperationCanceledException e)
            {
                _rachisLogIndexNotifications.SetTaskCompleted(index, e);
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

        private void SetValueForTypedDatabaseCommand(ClusterOperationContext context, string type, BlittableJsonReaderObject cmd, long index, Leader leader, out object result)
        {
            result = null;
            UpdateValueForDatabaseCommand updateCommand = null;
            Exception exception = null;
            try
            {
                var items = context.Transaction.InnerTransaction.OpenTable(ItemsSchema, Items);

                updateCommand = (UpdateValueForDatabaseCommand)JsonDeserializationCluster.Commands[type](cmd);

                using (var databaseRecord = ReadRawDatabaseRecord(context, updateCommand.DatabaseName))
                {
                    if (databaseRecord == null)
                        throw new DatabaseDoesNotExistException($"Cannot set typed value of type {type} for database {updateCommand.DatabaseName}, because it does not exist");

                    updateCommand.Execute(context, items, index, databaseRecord, _parent.CurrentState, out result);
                }
            }
            catch (Exception e)
            {
                exception = e;
                throw;
            }
            finally
            {
                LogCommand(type, index, exception, updateCommand?.AdditionalDebugInformation(exception));
                NotifyDatabaseAboutChanged(context, updateCommand?.DatabaseName, index, type, DatabasesLandlord.ClusterDatabaseChangeType.ValueChanged, null);
            }
        }

        public async Task WaitForIndexNotification(long index, TimeSpan? timeout = null)
        {
            await _rachisLogIndexNotifications.WaitForIndexNotification(index, timeout ?? _parent.OperationTimeout);
        }

        private unsafe void RemoveNodeFromDatabase(ClusterOperationContext context, BlittableJsonReaderObject cmd, long index, Leader leader, ServerStore serverStore)
        {
            Exception exception = null;
            RemoveNodeFromDatabaseCommand remove = null;
            try
            {
                var items = context.Transaction.InnerTransaction.OpenTable(ItemsSchema, Items);
                remove = JsonDeserializationCluster.RemoveNodeFromDatabaseCommand(cmd);
                var databaseName = remove.DatabaseName;
                var databaseNameLowered = databaseName.ToLowerInvariant();
                using (Slice.From(context.Allocator, "db/" + databaseNameLowered, out Slice lowerKey))
                using (Slice.From(context.Allocator, "db/" + databaseName, out Slice key))
                {
                    if (items.ReadByKey(lowerKey, out TableValueReader reader) == false)
                        throw new DatabaseDoesNotExistException($"The database {databaseName} does not exists");

                    var doc = new BlittableJsonReaderObject(reader.Read(2, out int size), size, context);

                    if (doc.TryGet(nameof(DatabaseRecord.Topology), out BlittableJsonReaderObject _) == false)
                    {
                        items.DeleteByKey(lowerKey);
                        NotifyDatabaseAboutChanged(context, databaseName, index, nameof(RemoveNodeFromDatabaseCommand),
                            DatabasesLandlord.ClusterDatabaseChangeType.RecordChanged, null);
                        return;
                    }

                    var databaseRecord = JsonDeserializationCluster.DatabaseRecord(doc);
                    remove.UpdateDatabaseRecord(databaseRecord, index);

                    if (databaseRecord.DeletionInProgress.Count == 0 && databaseRecord.Topology.Count == 0)
                    {
                        DeleteDatabaseRecord(context, index, items, lowerKey, databaseName, serverStore);
                        NotifyDatabaseAboutChanged(context, databaseName, index, nameof(RemoveNodeFromDatabaseCommand),
                            DatabasesLandlord.ClusterDatabaseChangeType.RecordChanged, null);
                        return;
                    }

                    var updated = DocumentConventions.DefaultForServer.Serialization.DefaultConverter.ToBlittable(databaseRecord, context);

                    UpdateValue(index, items, lowerKey, key, updated);
                }

                NotifyDatabaseAboutChanged(context, databaseName, index, nameof(RemoveNodeFromDatabaseCommand),
                    DatabasesLandlord.ClusterDatabaseChangeType.RecordChanged, null);
            }
            catch (Exception e)
            {
                exception = e;
                throw;
            }
            finally
            {
                LogCommand(nameof(RemoveNodeFromDatabaseCommand), index, exception, remove?.AdditionalDebugInformation(exception));
            }
        }

        private static void DeleteDatabaseRecord(ClusterOperationContext context, long index, Table items, Slice lowerKey, string databaseName, ServerStore serverStore)
        {
            // delete database record
            items.DeleteByKey(lowerKey);

            // delete all values linked to database record - for subscription, etl etc.
            CleanupDatabaseRelatedValues(context, items, databaseName, serverStore);
            CleanupDatabaseReplicationCertificate(context, databaseName);

            var transactionsCommands = context.Transaction.InnerTransaction.OpenTable(TransactionCommandsSchema, TransactionCommands);
            var commandsCountPerDatabase = context.Transaction.InnerTransaction.ReadTree(TransactionCommandsCountPerDatabase);

            using (ClusterTransactionCommand.GetPrefix(context, databaseName, out var prefixSlice))
            {
                commandsCountPerDatabase.Delete(prefixSlice);
                transactionsCommands.DeleteByPrimaryKeyPrefix(prefixSlice);
            }
        }

        private static void CleanupDatabaseReplicationCertificate(ClusterOperationContext context, string databaseName)
        {
            using var certs = context.Transaction.InnerTransaction.OpenTable(ReplicationCertificatesSchema, ReplicationCertificatesSlice);

            string prefixString = (databaseName + "/").ToLowerInvariant();
            using var _ = Slice.From(context.Allocator, prefixString, out var prefix);

            certs.DeleteByPrimaryKeyPrefix(prefix);
        }

        private static void CleanupDatabaseRelatedValues(ClusterOperationContext context, Table items, string databaseName, ServerStore serverStore)
        {
            var dbValuesPrefix = Helpers.ClusterStateMachineValuesPrefix(databaseName).ToLowerInvariant();
            using (Slice.From(context.Allocator, dbValuesPrefix, out var loweredKey))
            {
                items.DeleteByPrimaryKeyPrefix(loweredKey);
            }

            var databaseLowered = $"{databaseName.ToLowerInvariant()}/";
            using (Slice.From(context.Allocator, databaseLowered, out var databaseSlice))
            {
                context.Transaction.InnerTransaction.OpenTable(CompareExchangeSchema, CompareExchange).DeleteByPrimaryKeyPrefix(databaseSlice);
                context.Transaction.InnerTransaction.OpenTable(CompareExchangeTombstoneSchema, CompareExchangeTombstones).DeleteByPrimaryKeyPrefix(databaseSlice);
                context.Transaction.InnerTransaction.OpenTable(IdentitiesSchema, Identities).DeleteByPrimaryKeyPrefix(databaseSlice);
            }

            // db can be idle when we are deleting it
            serverStore?.IdleDatabases.TryRemove(databaseName, out _);
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

        internal static unsafe void UpdateCertificate(Table certificates, Slice key, Slice hash, BlittableJsonReaderObject updated)
        {
            Debug.Assert(key.ToString() == key.ToString().ToLowerInvariant(), $"Key of certificate table (thumbprint) must be lower cased while we got '{key}'");

            using (certificates.Allocate(out TableValueBuilder builder))
            {
                builder.Add(key);
                builder.Add(hash);
                builder.Add(updated.BasePointer, updated.Size);

                certificates.Set(builder);
            }
        }

        private static readonly string[] DatabaseRecordTasks =
        {
            nameof(DatabaseRecord.PeriodicBackups),
            nameof(DatabaseRecord.ExternalReplications),
            nameof(DatabaseRecord.SinkPullReplications),
            nameof(DatabaseRecord.HubPullReplications),
            nameof(DatabaseRecord.RavenEtls),
            nameof(DatabaseRecord.SqlEtls)
        };

        private unsafe List<string> AddDatabase(ClusterOperationContext context, BlittableJsonReaderObject cmd, long index, Leader leader)
        {
            var addDatabaseCommand = JsonDeserializationCluster.AddDatabaseCommand(cmd);
            Exception exception = null;
            try
            {
                Debug.Assert(addDatabaseCommand.Record.Topology.Count != 0, "Attempt to add database with no nodes");
                var items = context.Transaction.InnerTransaction.OpenTable(ItemsSchema, Items);
                using (Slice.From(context.Allocator, "db/" + addDatabaseCommand.Name, out Slice valueName))
                using (Slice.From(context.Allocator, "db/" + addDatabaseCommand.Name.ToLowerInvariant(), out Slice valueNameLowered))
                using (var newDatabaseRecord = DocumentConventions.DefaultForServer.Serialization.DefaultConverter.ToBlittable(addDatabaseCommand.Record, context))
                {
                    var databaseExists = items.ReadByKey(valueNameLowered, out TableValueReader reader);
                    if (addDatabaseCommand.RaftCommandIndex != null)
                    {
                        if (databaseExists == false && addDatabaseCommand.RaftCommandIndex != 0)
                            throw new RachisConcurrencyException("Concurrency violation, the database " + addDatabaseCommand.Name +
                                                           " does not exist, but had a non zero etag");

                        var actualEtag = Bits.SwapBytes(*(long*)reader.Read(3, out int size));
                        Debug.Assert(size == sizeof(long));

                        if (actualEtag != addDatabaseCommand.RaftCommandIndex.Value)
                        {
                            throw new RachisConcurrencyException("Concurrency violation, the database " + addDatabaseCommand.Name + " has etag " + actualEtag +
                                                                 " but was expecting " + addDatabaseCommand.RaftCommandIndex);
                        }
                    }

                    bool shouldSetClientConfigEtag;
                    var dbId = Constants.Documents.Prefix + addDatabaseCommand.Name;
                    using (var oldDatabaseRecord = Read(context, dbId, out _))
                    {
                        VerifyUnchangedTasks(oldDatabaseRecord);
                        shouldSetClientConfigEtag = ShouldSetClientConfigEtag(newDatabaseRecord, oldDatabaseRecord);
                    }

                    using (var databaseRecordAsJson = UpdateDatabaseRecordIfNeeded(databaseExists, shouldSetClientConfigEtag, index, addDatabaseCommand, newDatabaseRecord, context))
                    {
                        UpdateValue(index, items, valueNameLowered, valueName, databaseRecordAsJson);
                        SetDatabaseValues(addDatabaseCommand.DatabaseValues, addDatabaseCommand.Name, context, index, items);
                        return addDatabaseCommand.Record.Topology.Members;
                    }

                    void VerifyUnchangedTasks(BlittableJsonReaderObject dbDoc)
                    {
                        if (addDatabaseCommand.IsRestore)
                            return;

                        if (dbDoc == null)
                        {
                            foreach (var task in DatabaseRecordTasks)
                            {
                                if (newDatabaseRecord.TryGet(task, out BlittableJsonReaderArray dbRecordVal) && dbRecordVal.Length > 0)
                                {
                                    throw new RachisInvalidOperationException(
                                        $"Failed to create a new Database {addDatabaseCommand.Name}. Updating tasks configurations via DatabaseRecord is not supported, please use a dedicated operation to update the {task} configuration.");
                                }
                            }
                        }
                        else
                        {
                            // compare tasks configurations of both db records
                            foreach (var task in DatabaseRecordTasks)
                            {
                                var hasChanges = false;

                                if (dbDoc.TryGet(task, out BlittableJsonReaderArray oldDbRecordVal))
                                {
                                    if (newDatabaseRecord.TryGet(task, out BlittableJsonReaderArray newDbRecordVal) == false && oldDbRecordVal.Length > 0)
                                    {
                                        hasChanges = true;
                                    }
                                    else if (oldDbRecordVal.Equals(newDbRecordVal) == false)
                                    {
                                        hasChanges = true;
                                    }
                                }
                                else if (newDatabaseRecord.TryGet(task, out BlittableJsonReaderArray newDbRecordObject) && newDbRecordObject.Length > 0)
                                {
                                    hasChanges = true;
                                }

                                if (hasChanges)
                                    throw new RachisInvalidOperationException(
                                        $"Cannot update {task} configuration with DatabaseRecord. Please use a dedicated operation to update the {task} configuration.");
                            }
                        }
                    }
                }
            }
            catch (Exception e)
            {
                exception = e;
                throw;
            }
            finally
            {
                LogCommand(nameof(AddDatabaseCommand), index, exception, addDatabaseCommand.AdditionalDebugInformation(exception));
                NotifyDatabaseAboutChanged(context, addDatabaseCommand.Name, index, nameof(AddDatabaseCommand),
                    addDatabaseCommand.IsRestore
                        ? DatabasesLandlord.ClusterDatabaseChangeType.RecordRestored
                        : DatabasesLandlord.ClusterDatabaseChangeType.RecordChanged, null);
            }
        }

        private BlittableJsonReaderObject UpdateDatabaseRecordIfNeeded(bool databaseExists, bool shouldSetClientConfigEtag, long index, AddDatabaseCommand addDatabaseCommand, BlittableJsonReaderObject newDatabaseRecord, ClusterOperationContext context)
        {
            var hasChanges = false;

            if (shouldSetClientConfigEtag)
            {
                addDatabaseCommand.Record.Client ??= new ClientConfiguration();
                addDatabaseCommand.Record.Client.Etag = index;
                hasChanges = true;
            }

            if (databaseExists == false || addDatabaseCommand.IsRestore)
            {
                // the backup tasks cannot be changed by modifying the database record
                // (only by using the dedicated UpdatePeriodicBackup command)
                UpdatePeriodicBackups();
                UpdateExternalReplications();
            }

            return hasChanges
                ? DocumentConventions.DefaultForServer.Serialization.DefaultConverter.ToBlittable(addDatabaseCommand.Record, context)
                : newDatabaseRecord;

            void UpdatePeriodicBackups()
            {
                var serverWideBackups = Read(context, ServerWideConfigurationKey.Backup);
                if (serverWideBackups == null)
                    return;

                var propertyNames = serverWideBackups.GetPropertyNames();
                if (propertyNames.Length == 0)
                    return;

                foreach (var propertyName in propertyNames)
                {
                    if (serverWideBackups.TryGet(propertyName, out BlittableJsonReaderObject configurationBlittable) == false)
                        continue;

                    if (IsExcluded(configurationBlittable, addDatabaseCommand.Name))
                        continue;

                    var backupConfiguration = JsonDeserializationCluster.PeriodicBackupConfiguration(configurationBlittable);
                    PutServerWideBackupConfigurationCommand.UpdateTemplateForDatabase(backupConfiguration, addDatabaseCommand.Name, addDatabaseCommand.Encrypted);
                    addDatabaseCommand.Record.PeriodicBackups.Add(backupConfiguration);
                    hasChanges = true;
                }
            }

            void UpdateExternalReplications()
            {
                var externalReplications = Read(context, ServerWideConfigurationKey.ExternalReplication);
                if (externalReplications == null)
                    return;

                var propertyNames = externalReplications.GetPropertyNames();
                if (propertyNames.Length == 0)
                    return;

                foreach (var propertyName in propertyNames)
                {
                    if (externalReplications.TryGet(propertyName, out BlittableJsonReaderObject configurationBlittable) == false)
                        continue;

                    if (configurationBlittable.TryGet(nameof(ServerWideExternalReplication.TopologyDiscoveryUrls), out BlittableJsonReaderArray topologyDiscoveryUrlsBlittableArray) == false)
                        continue;

                    if (IsExcluded(configurationBlittable, addDatabaseCommand.Name))
                        continue;

                    var topologyDiscoveryUrls = topologyDiscoveryUrlsBlittableArray.Select(x => x.ToString()).ToArray();

                    var externalReplication = JsonDeserializationCluster.ExternalReplication(configurationBlittable);
                    var connectionString = PutServerWideExternalReplicationCommand.UpdateExternalReplicationTemplateForDatabase(externalReplication, addDatabaseCommand.Name, topologyDiscoveryUrls);
                    addDatabaseCommand.Record.ExternalReplications.Add(externalReplication);
                    addDatabaseCommand.Record.RavenConnectionStrings[connectionString.Name] = connectionString;
                    hasChanges = true;
                }
            }
        }

        private static bool IsExcluded(BlittableJsonReaderObject configurationBlittable, string databaseName)
        {
            if (configurationBlittable.TryGet(nameof(IServerWideTask.ExcludedDatabases), out BlittableJsonReaderArray excludedDatabases) == false)
                return false;

            foreach (object excludedDatabase in excludedDatabases)
            {
                if (excludedDatabase == null)
                    continue;

                if (string.Equals(databaseName, excludedDatabase.ToString(), StringComparison.OrdinalIgnoreCase))
                    return true;
            }

            return false;
        }

        private static bool ShouldSetClientConfigEtag(BlittableJsonReaderObject newDatabaseRecord, BlittableJsonReaderObject oldDatabaseRecord)
        {
            const string clientPropName = nameof(DatabaseRecord.Client);
            var hasNewConfiguration = newDatabaseRecord.TryGet(clientPropName, out BlittableJsonReaderObject newDbClientConfig) && newDbClientConfig != null;
            if (oldDatabaseRecord == null)
                return hasNewConfiguration;

            if (hasNewConfiguration == false)
                return true;

            return oldDatabaseRecord.TryGet(clientPropName, out BlittableJsonReaderObject oldDbClientConfig) == false
                   || oldDbClientConfig == null
                   || oldDbClientConfig.Equals(newDbClientConfig) == false;
        }

        private static void SetDatabaseValues(
            Dictionary<string, BlittableJsonReaderObject> databaseValues,
            string databaseName,
            ClusterOperationContext context,
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
                using (var value = keyValue.Value.Clone(context))
                {
                    UpdateValue(index, items, databaseValueNameLowered, databaseValueName, value);
                }
            }
        }

        private void DeleteValue(ClusterOperationContext context, string type, BlittableJsonReaderObject cmd, long index)
        {
            Exception exception = null;
            DeleteValueCommand delCmd = null;
            try
            {
                delCmd = JsonDeserializationCluster.DeleteValueCommand(cmd);
                if (delCmd.Name.StartsWith("db/"))
                    throw new RachisApplyException("Cannot delete " + delCmd.Name + " using DeleteValueCommand, only via dedicated database calls");

                DeleteItem(context, delCmd.Name);
            }
            catch (Exception e)
            {
                exception = e;
                throw;
            }
            finally
            {
                LogCommand(type, index, exception, delCmd?.AdditionalDebugInformation(exception));
                NotifyValueChanged(context, type, index);
            }
        }

        private void DeleteCertificate(ClusterOperationContext context, string type, BlittableJsonReaderObject cmd, long index)
        {
            try
            {
                var command = (DeleteCertificateFromClusterCommand)CommandBase.CreateFrom(cmd);

                DeleteCertificate(context, command.Name);
            }
            finally
            {
                NotifyValueChanged(context, type, index);
            }
        }

        private void DeleteMultipleCertificates(ClusterOperationContext context, string type, BlittableJsonReaderObject cmd, long index)
        {
            try
            {
                var command = (DeleteCertificateCollectionFromClusterCommand)CommandBase.CreateFrom(cmd);

                foreach (var thumbprint in command.Names)
                {
                    DeleteCertificate(context, thumbprint);
                }
            }
            finally
            {
                NotifyValueChanged(context, type, index);
            }
        }

        public void DeleteItem<TTransaction>(TransactionOperationContext<TTransaction> context, string name)
            where TTransaction : RavenTransaction
        {
            var items = context.Transaction.InnerTransaction.OpenTable(ItemsSchema, Items);
            using (Slice.From(context.Allocator, name.ToLowerInvariant(), out Slice keyNameLowered))
            {
                items.DeleteByKey(keyNameLowered);
            }
        }

        public void DeleteCertificate<TTransaction>(TransactionOperationContext<TTransaction> context, string thumbprint)
            where TTransaction : RavenTransaction
        {
            var certs = context.Transaction.InnerTransaction.OpenTable(CertificatesSchema, CertificatesSlice);
            using (Slice.From(context.Allocator, thumbprint.ToLowerInvariant(), out var thumbprintSlice))
            {
                certs.DeleteByKey(thumbprintSlice);
            }
        }

        private void DeleteMultipleValues(ClusterOperationContext context, string type, BlittableJsonReaderObject cmd, long index, Leader leader)
        {
            Exception exception = null;
            DeleteMultipleValuesCommand delCmd = null;
            try
            {
                var items = context.Transaction.InnerTransaction.OpenTable(ItemsSchema, Items);
                delCmd = JsonDeserializationCluster.DeleteMultipleValuesCommand(cmd);
                if (delCmd.Names.Any(name => name.StartsWith("db/")))
                    throw new RachisApplyException("Cannot delete " + delCmd.Names + " using DeleteMultipleValuesCommand, only via dedicated database calls");

                foreach (var name in delCmd.Names)
                {
                    using (Slice.From(context.Allocator, name, out Slice _))
                    using (Slice.From(context.Allocator, name.ToLowerInvariant(), out Slice keyNameLowered))
                    {
                        items.DeleteByKey(keyNameLowered);
                    }
                }
            }
            catch (Exception e)
            {
                exception = e;
                throw;
            }
            finally
            {
                LogCommand(type, index, exception, delCmd?.AdditionalDebugInformation(exception));
                NotifyValueChanged(context, type, index);
            }
        }

        private unsafe T UpdateValue<T>(ClusterOperationContext context, string type, BlittableJsonReaderObject cmd, long index, bool skipNotifyValueChanged = false)
        {
            UpdateValueCommand<T> command = null;
            Exception exception = null;
            try
            {
                var items = context.Transaction.InnerTransaction.OpenTable(ItemsSchema, Items);
                command = (UpdateValueCommand<T>)CommandBase.CreateFrom(cmd);
                if (command.Name.StartsWith(Constants.Documents.Prefix))
                    throw new RachisApplyException("Cannot set " + command.Name + " using PutValueCommand, only via dedicated database calls");

                using (Slice.From(context.Allocator, command.Name, out Slice valueName))
                using (Slice.From(context.Allocator, command.Name.ToLowerInvariant(), out Slice valueNameLowered))
                {
                    BlittableJsonReaderObject previousValue = null;
                    if (items.ReadByKey(valueNameLowered, out var tvr))
                    {
                        var ptr = tvr.Read(2, out int size);
                        previousValue = new BlittableJsonReaderObject(ptr, size, context);
                    }

                    var newValue = command.GetUpdatedValue(context, previousValue, index);
                    if (newValue == null)
                        return default;

                    UpdateValue(index, items, valueNameLowered, valueName, newValue);
                    return command.Value;
                }
            }
            catch (Exception e)
            {
                exception = e;
                throw;
            }
            finally
            {
                LogCommand(type, index, exception, command?.AdditionalDebugInformation(exception));

                if (skipNotifyValueChanged == false)
                    NotifyValueChanged(context, type, index);
            }
        }

        public string GetServerWideTaskNameByTaskId(TransactionOperationContext context, string key, long taskId)
        {
            var configurationsBlittable = Read(context, key);
            if (configurationsBlittable == null)
                return null;

            foreach (var propertyName in configurationsBlittable.GetPropertyNames())
            {
                if (configurationsBlittable.TryGet(propertyName, out BlittableJsonReaderObject serverWideBlittable) == false)
                    continue;

                if (serverWideBlittable.TryGet(nameof(ServerWideBackupConfiguration.TaskId), out long taskIdFromConfiguration) == false)
                    continue;

                if (taskId == taskIdFromConfiguration)
                {
                    serverWideBlittable.TryGet(nameof(ServerWideBackupConfiguration.Name), out string taskName);
                    return taskName;
                }
            }

            return null;
        }

        public IEnumerable<BlittableJsonReaderObject> GetServerWideConfigurations(TransactionOperationContext context, OngoingTaskType type, string name)
        {
            var configurationsBlittable = Read(context, ServerWideConfigurationKey.GetKeyByType(type));
            if (configurationsBlittable == null)
                yield break;

            foreach (var propertyName in configurationsBlittable.GetPropertyNames())
            {
                if (name != null && propertyName.Equals(name, StringComparison.OrdinalIgnoreCase) == false)
                    continue;

                if (configurationsBlittable.TryGet(propertyName, out BlittableJsonReaderObject serverWideBackupBlittable))
                {
                    yield return serverWideBackupBlittable;
                }
            }
        }

        private T PutValue<T>(ClusterOperationContext context, string type, BlittableJsonReaderObject cmd, long index)
        {
            Exception exception = null;
            PutValueCommand<T> command = null;
            try
            {
                var items = context.Transaction.InnerTransaction.OpenTable(ItemsSchema, Items);
                command = (PutValueCommand<T>)CommandBase.CreateFrom(cmd);
                if (command.Name.StartsWith(Constants.Documents.Prefix))
                    throw new RachisApplyException("Cannot set " + command.Name + " using PutValueCommand, only via dedicated database calls");

                command.UpdateValue(index);

                using (Slice.From(context.Allocator, command.Name, out Slice valueName))
                using (Slice.From(context.Allocator, command.Name.ToLowerInvariant(), out Slice valueNameLowered))
                using (var rec = context.ReadObject(command.ValueToJson(), "inner-val"))
                {
                    UpdateValue(index, items, valueNameLowered, valueName, rec);
                    return command.Value;
                }
            }
            catch (Exception e)
            {
                exception = e;
                throw;
            }
            finally
            {
                LogCommand(type, index, exception, command.AdditionalDebugInformation(exception));
                NotifyValueChanged(context, type, index);
            }
        }

        private void PutValueDirectly(ClusterOperationContext context, string key, BlittableJsonReaderObject value, long index)
        {
            var items = context.Transaction.InnerTransaction.OpenTable(ItemsSchema, Items);
            using (Slice.From(context.Allocator, key, out Slice keySlice))
            using (Slice.From(context.Allocator, key.ToLowerInvariant(), out Slice keyLoweredSlice))
            {
                UpdateValue(index, items, keyLoweredSlice, keySlice, value);
            }
        }

        private void PutCertificate(ClusterOperationContext context, string type, BlittableJsonReaderObject cmd, long index, ServerStore serverStore)
        {
            try
            {
                var certs = context.Transaction.InnerTransaction.OpenTable(CertificatesSchema, CertificatesSlice);
                var command = (PutCertificateCommand)CommandBase.CreateFrom(cmd);

                using (Slice.From(context.Allocator, command.PublicKeyPinningHash, out var hashSlice))
                using (Slice.From(context.Allocator, command.Name.ToLowerInvariant(), out var thumbprintSlice))
                using (var cert = context.ReadObject(command.ValueToJson(), "inner-val"))
                {
                    if (_clusterAuditLog.IsInfoEnabled)
                        _clusterAuditLog.Info($"Registering new certificate '{command.Value.Thumbprint}' in the cluster. Security Clearance: {command.Value.SecurityClearance}. " +
                                              $"Permissions:{Environment.NewLine}{string.Join(Environment.NewLine, command.Value.Permissions.Select(kvp => kvp.Key + ": " + kvp.Value.ToString()))}");

                    UpdateCertificate(certs, thumbprintSlice, hashSlice, cert);
                    return;
                }
            }
            finally
            {
                NotifyValueChanged(context, type, index);
            }
        }

        private void BulkPutReplicationCertificate(ClusterOperationContext context, string type, BlittableJsonReaderObject cmd, long index, ServerStore serverStore)
        {
            var command = (BulkRegisterReplicationHubAccessCommand)CommandBase.CreateFrom(cmd);
            try
            {
                var certs = context.Transaction.InnerTransaction.OpenTable(ReplicationCertificatesSchema, ReplicationCertificatesSlice);

                foreach (RegisterReplicationHubAccessCommand inner in command.Commands)
                {
                    PutRegisterReplicationHubAccessInternal(context, inner, certs);
                }
            }
            finally
            {
                NotifyDatabaseAboutChanged(context, command.Database, index, type, DatabasesLandlord.ClusterDatabaseChangeType.ValueChanged, command);
            }
        }

        private void PutReplicationCertificate(ClusterOperationContext context, string type, BlittableJsonReaderObject cmd, long index, ServerStore serverStore)
        {
            var command = (RegisterReplicationHubAccessCommand)CommandBase.CreateFrom(cmd);
            try
            {
                var certs = context.Transaction.InnerTransaction.OpenTable(ReplicationCertificatesSchema, ReplicationCertificatesSlice);

                PutRegisterReplicationHubAccessInternal(context, command, certs);
            }
            finally
            {
                NotifyDatabaseAboutChanged(context, command.Database, index, type, DatabasesLandlord.ClusterDatabaseChangeType.ValueChanged, command);
            }
        }

        private unsafe void PutRegisterReplicationHubAccessInternal(ClusterOperationContext context, RegisterReplicationHubAccessCommand command, Table certs)
        {
            using (Slice.From(context.Allocator, (command.Database + "/" + command.HubName + "/" + command.CertificateThumbprint).ToLowerInvariant(), out var keySlice))
            using (Slice.From(context.Allocator, (command.Database + "/" + command.HubName + "/" + command.CertificatePublicKeyHash).ToLowerInvariant(),
                out var publicKeySlice))
            using (var obj = context.ReadObject(command.PrepareForStorage(), "inner-val"))
            {
                if (_clusterAuditLog.IsInfoEnabled)
                    _clusterAuditLog.Info(
                        $"Registering new replication certificate {command.Name} = '{command.CertificateThumbprint}' for replication in {command.Database} using {command.HubName} " +
                        $"Allowed read paths: {string.Join(", ", command.AllowedHubToSinkPaths)}, Allowed write paths: {string.Join(", ", command.AllowedSinkToHubPaths)}.");

                var certificate = Convert.FromBase64String(command.CertificateBase64);
                fixed (byte* pCert = certificate)
                {
                    using (certs.Allocate(out TableValueBuilder builder))
                    {
                        builder.Add(keySlice);
                        builder.Add(publicKeySlice);
                        builder.Add(pCert, certificate.Length);
                        builder.Add(obj.BasePointer, obj.Size);

                        certs.Set(builder);
                    }
                }

                if (command.RegisteringSamePublicKeyPinningHash == false)
                    return;

                // here we'll clear the old values
                var samePublicKeyHash = new SortedList<DateTime, long>();
                foreach (var result in certs.SeekForwardFromPrefix(ReplicationCertificatesSchema.Indexes[ReplicationCertificatesHashSlice], publicKeySlice, publicKeySlice, 0))
                {
                    using var accessBlittable = new BlittableJsonReaderObject(result.Result.Reader.Read((int)ReplicationCertificatesTable.Access, out var size), size, context);

                    accessBlittable.TryGet(nameof(command.NotAfter), out DateTime notAfter);

                    samePublicKeyHash.Add(notAfter, result.Result.Reader.Id);
                }

                while (samePublicKeyHash.Count > Constants.Certificates.MaxNumberOfCertsWithSameHash)
                {
                    certs.Delete(samePublicKeyHash.Values[0]);
                    samePublicKeyHash.RemoveAt(0);
                }
            }
        }

        private void RemoveReplicationCertificate(ClusterOperationContext context, string type, BlittableJsonReaderObject cmd, long index, ServerStore serverStore)
        {
            var command = (UnregisterReplicationHubAccessCommand)CommandBase.CreateFrom(cmd);
            try
            {
                var certs = context.Transaction.InnerTransaction.OpenTable(ReplicationCertificatesSchema, ReplicationCertificatesSlice);

                using (Slice.From(context.Allocator, (command.Database + "/" + command.HubName + "/" + command.CertificateThumbprint).ToLowerInvariant(), out var keySlice))
                {
                    if (certs.DeleteByKey(keySlice) == false)
                        return;

                    if (_clusterAuditLog.IsInfoEnabled)
                        _clusterAuditLog.Info($"Removed replication certificate '{command.CertificateThumbprint}' for replication in {command.Database} using {command.HubName}.");
                }
            }
            finally
            {
                NotifyDatabaseAboutChanged(context, command.Database, index, type, DatabasesLandlord.ClusterDatabaseChangeType.ValueChanged, command);
            }
        }

        private void DiscardLeftoverCertsWithSamePinningHash(ClusterOperationContext context, string hash, string type, long index)
        {
            var certsWithSameHash = GetCertificatesByPinningHashSortedByExpiration(context, hash);

            var thumbprintsToDelete = certsWithSameHash.Select(x => x.Thumbprint).Skip(Constants.Certificates.MaxNumberOfCertsWithSameHash).ToList();

            if (thumbprintsToDelete.Count == 0)
                return;

            try
            {
                foreach (var thumbprint in thumbprintsToDelete)
                {
                    DeleteCertificate(context, thumbprint);
                }
            }
            finally
            {
                if (_clusterAuditLog.IsInfoEnabled)
                    _clusterAuditLog.Info($"After allowing a connection based on Public Key Pinning Hash, deleting the following old certificates from the cluster: {string.Join(", ", thumbprintsToDelete)}");
                NotifyValueChanged(context, type, index);
            }
        }

        public override void EnsureNodeRemovalOnDeletion(ClusterOperationContext context, long term, string nodeTag)
        {
            var djv = new RemoveNodeFromClusterCommand(RaftIdGenerator.NewId())
            {
                RemovedNode = nodeTag
            }.ToJson(context);

            _parent.InsertToLeaderLog(context, term, context.ReadObject(djv, "remove"), RachisEntryFlags.StateMachineCommand);
        }

        private void NotifyValueChanged(ClusterOperationContext context, string type, long index)
        {
            context.Transaction.InnerTransaction.LowLevelTransaction.AfterCommitWhenNewReadTransactionsPrevented += _ =>
            {
                // we do this under the write tx lock before we update the last applied index
                _rachisLogIndexNotifications.AddTask(index);

                context.Transaction.InnerTransaction.LowLevelTransaction.OnDispose += tx =>
                {
                    ExecuteAsyncTask(index, () => Changes.OnValueChanges(index, type));
                };
            };
        }

        private void NotifyDatabaseAboutChanged(ClusterOperationContext context, string databaseName, long index, string type, DatabasesLandlord.ClusterDatabaseChangeType change, object changeState)
        {
            Debug.Assert(changeState.ContainsBlittableObject() == false, "You cannot use a blittable in the command state, since this is handled outside of the transaction");

            context.Transaction.InnerTransaction.LowLevelTransaction.AfterCommitWhenNewReadTransactionsPrevented += _ =>
            {
                // we do this under the write tx lock before we update the last applied index
                _rachisLogIndexNotifications.AddTask(index);

                context.Transaction.InnerTransaction.LowLevelTransaction.OnDispose += tx =>
                {
                    ExecuteAsyncTask(index, () => Changes.OnDatabaseChanges(databaseName, index, type, change, changeState));
                };
            };
        }

        private void ExecuteAsyncTask(long index, Func<Task> task)
        {
            Task.Run(async () =>
            {
                Exception error = null;
                try
                {
                    await task();
                }
                catch (Exception e)
                {
                    error = e;
                }
                finally
                {
                    _rachisLogIndexNotifications.NotifyListenersAbout(index, error);
                    _rachisLogIndexNotifications.SetTaskCompleted(index, error);
                }
            });
        }

        private void UpdateDatabase(ClusterOperationContext context, string type, BlittableJsonReaderObject cmd, long index, Leader leader, ServerStore serverStore)
        {
            if (cmd.TryGet(DatabaseName, out string databaseName) == false || string.IsNullOrEmpty(databaseName))
                throw new RachisApplyException("Update database command must contain a DatabaseName property");

            UpdateDatabaseCommand updateCommand = null;
            Exception exception = null;
            try
            {
                var items = context.Transaction.InnerTransaction.OpenTable(ItemsSchema, Items);
                var dbKey = "db/" + databaseName;

                using (Slice.From(context.Allocator, dbKey, out Slice valueName))
                using (Slice.From(context.Allocator, dbKey.ToLowerInvariant(), out Slice valueNameLowered))
                {
                    var databaseRecordJson = ReadInternal(context, out long etag, valueNameLowered);
                    updateCommand = (UpdateDatabaseCommand)JsonDeserializationCluster.Commands[type](cmd);

                    if (databaseRecordJson == null)
                    {
                        if (updateCommand.ErrorOnDatabaseDoesNotExists)
                            throw DatabaseDoesNotExistException.CreateWithMessage(databaseName, $"Could not execute update command of type '{type}'.");
                        return;
                    }

                    if (updateCommand.RaftCommandIndex != null && etag != updateCommand.RaftCommandIndex.Value)
                        throw new RachisConcurrencyException(
                            $"Concurrency violation at executing {type} command, the database {databaseName} has etag {etag} but was expecting {updateCommand.RaftCommandIndex}");

                    var databaseRecord = JsonDeserializationCluster.DatabaseRecord(databaseRecordJson);

                    updateCommand.Initialize(serverStore, context);

                    try
                    {
                        updateCommand.UpdateDatabaseRecord(databaseRecord, index);
                    }
                    catch (Exception e)
                    {
                        // We are not using the transaction, so any exception here doesn't involve any kind of corruption
                        // and is consistent across the cluster.
                        throw new RachisApplyException("Failed to update database record.", e);
                    }

                    updateCommand.AfterDatabaseRecordUpdate(context, items, _clusterAuditLog);

                    if (databaseRecord.Topology.Count == 0 && databaseRecord.DeletionInProgress.Count == 0)
                    {
                        DeleteDatabaseRecord(context, index, items, valueNameLowered, databaseName, serverStore);
                        return;
                    }

                    UpdateEtagForBackup(databaseRecord, type, index);
                    var updatedDatabaseBlittable = DocumentConventions.DefaultForServer.Serialization.DefaultConverter.ToBlittable(databaseRecord, context);
                    UpdateValue(index, items, valueNameLowered, valueName, updatedDatabaseBlittable);
                }
            }
            catch (Exception e)
            {
                exception = e;
                throw;
            }
            finally
            {
                LogCommand(type, index, exception, updateCommand?.AdditionalDebugInformation(exception));
                NotifyDatabaseAboutChanged(context, databaseName, index, type, DatabasesLandlord.ClusterDatabaseChangeType.RecordChanged, updateCommand);
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void LogCommand(string type, long index, Exception exception, string additionalDebugInformation = null)
        {
            if (_parent.Log.IsInfoEnabled)
            {
                LogCommandInternal(type, index, exception, additionalDebugInformation);
            }
        }

        private void LogCommandInternal(string type, long index, Exception exception, string additionalDebugInformation)
        {
            var successStatues = exception != null ? "has failed" : "was successful";
            var msg = $"Apply of {type} with index {index} {successStatues}.";
            var additionalDebugInfo = additionalDebugInformation;
            if (additionalDebugInfo != null)
            {
                msg += $" AdditionalDebugInformation: {additionalDebugInfo}.";
            }

            _parent.Log.Info(msg);
        }

        private void UpdateEtagForBackup(DatabaseRecord databaseRecord, string type, long index)
        {
            switch (type)
            {
                case nameof(UpdateClusterIdentityCommand):
                case nameof(IncrementClusterIdentitiesBatchCommand):
                case nameof(IncrementClusterIdentityCommand):
                case nameof(AddOrUpdateCompareExchangeCommand):
                case nameof(RemoveCompareExchangeCommand):
                case nameof(AddOrUpdateCompareExchangeBatchCommand):
                case nameof(UpdatePeriodicBackupCommand):
                case nameof(UpdateExternalReplicationCommand):
                case nameof(AddRavenEtlCommand):
                case nameof(AddSqlEtlCommand):
                case nameof(UpdateRavenEtlCommand):
                case nameof(UpdateSqlEtlCommand):
                case nameof(DeleteOngoingTaskCommand):
                case nameof(PutRavenConnectionStringCommand):
                case nameof(PutSqlConnectionStringCommand):
                case nameof(RemoveRavenConnectionStringCommand):
                case nameof(RemoveSqlConnectionStringCommand):
                case nameof(PutIndexCommand):
                case nameof(PutAutoIndexCommand):
                case nameof(DeleteIndexCommand):
                case nameof(SetIndexLockCommand):
                case nameof(SetIndexPriorityCommand):
                case nameof(SetIndexStateCommand):
                case nameof(EditRevisionsConfigurationCommand):
                case nameof(EditTimeSeriesConfigurationCommand):
                case nameof(EditRevisionsForConflictsConfigurationCommand):
                case nameof(EditExpirationCommand):
                case nameof(EditRefreshCommand):
                case nameof(EditDatabaseClientConfigurationCommand):
                    databaseRecord.EtagForBackup = index;
                    break;
            }
        }

        private enum SnapshotEntryType
        {
            Command,
            Core
        }

        private readonly (ByteString Name, int Version, SnapshotEntryType Type)[] _snapshotEntries =
        {
            (Items.Content, ClusterCommandsVersionManager.Base40CommandsVersion, SnapshotEntryType.Command),
            (CompareExchange.Content, ClusterCommandsVersionManager.Base40CommandsVersion,SnapshotEntryType.Command),
            (Identities.Content, ClusterCommandsVersionManager.Base40CommandsVersion,SnapshotEntryType.Command),

            (TransactionCommands.Content, ClusterCommandsVersionManager.Base41CommandsVersion,SnapshotEntryType.Command),
            (TransactionCommandsCountPerDatabase.Content, ClusterCommandsVersionManager.Base41CommandsVersion,SnapshotEntryType.Command),

            (CompareExchangeTombstones.Content, ClusterCommandsVersionManager.Base42CommandsVersion,SnapshotEntryType.Command),
            (CertificatesSlice.Content, ClusterCommandsVersionManager.Base42CommandsVersion,SnapshotEntryType.Command),
            (RachisLogHistory.LogHistorySlice.Content, 42_000,SnapshotEntryType.Core)
        };

        public override bool ShouldSnapshot(Slice slice, RootObjectType type)
        {
            for (int i = 0; i < _snapshotEntries.Length; i++)
            {
                var entry = _snapshotEntries[i];
                if (entry.Name.Match(slice.Content) == false)
                    continue;

                switch (entry.Type)
                {
                    case SnapshotEntryType.Command:
                        return ClusterCommandsVersionManager.CurrentClusterMinimalVersion >= entry.Version;

                    case SnapshotEntryType.Core:
                        return ClusterCommandsVersionManager.ClusterEngineVersion >= entry.Version;

                    default:
                        throw new ArgumentOutOfRangeException($"Unknown type '{entry.Type}'");
                }
            }

            return false;
        }

        public override void Initialize(RachisConsensus parent, ClusterOperationContext context, ClusterChanges changes)
        {
            base.Initialize(parent, context, changes);

            _rachisLogIndexNotifications.Log = _parent.Log;

            ItemsSchema.Create(context.Transaction.InnerTransaction, Items, 32);
            CompareExchangeSchema.Create(context.Transaction.InnerTransaction, CompareExchange, 32);
            CompareExchangeTombstoneSchema.Create(context.Transaction.InnerTransaction, CompareExchangeTombstones, 32);
            TransactionCommandsSchema.Create(context.Transaction.InnerTransaction, TransactionCommands, 32);
            IdentitiesSchema.Create(context.Transaction.InnerTransaction, Identities, 32);
            CertificatesSchema.Create(context.Transaction.InnerTransaction, CertificatesSlice, 32);
            ReplicationCertificatesSchema.Create(context.Transaction.InnerTransaction, ReplicationCertificatesSlice, 32);
            context.Transaction.InnerTransaction.CreateTree(TransactionCommandsCountPerDatabase);
            context.Transaction.InnerTransaction.CreateTree(LocalNodeStateTreeName);
            context.Transaction.InnerTransaction.CreateTree(CompareExchangeExpirationStorage.CompareExchangeByExpiration);

            _parent.SwitchToSingleLeaderAction = SwitchToSingleLeader;
        }

        private void SwitchToSingleLeader(ClusterOperationContext context)
        {
            // when switching to a single node cluster we need to clear all of the irrelevant databases
            var clusterTopology = _parent.GetTopology(context);
            var newTag = clusterTopology.Members.First().Key;
            var oldTag = _parent.ReadPreviousNodeTag(context) ?? newTag;

            SqueezeDatabasesToSingleNodeCluster(context, oldTag, newTag);

            ShrinkClusterTopology(context, clusterTopology, newTag, _parent.GetLastEntryIndex(context));

            UpdateLicenseOnSwitchingToSingleLeader(context);
        }

        private void UpdateLicenseOnSwitchingToSingleLeader(ClusterOperationContext context)
        {
            var licenseLimitsBlittable = Read(context, ServerStore.LicenseLimitsStorageKey, out long index);
            if (licenseLimitsBlittable == null)
                return;

            var tag = _parent.ReadNodeTag(context);
            var licenseLimits = JsonDeserializationServer.LicenseLimits(licenseLimitsBlittable);

            if (licenseLimits.NodeLicenseDetails.ContainsKey(tag) && licenseLimits.NodeLicenseDetails.Count == 1)
                return;

            if (licenseLimits.NodeLicenseDetails.TryGetValue(_parent.Tag, out var details) == false)
                return;

            var newLimits = new LicenseLimits
            {
                NodeLicenseDetails = new Dictionary<string, DetailsPerNode>
                {
                    [tag] = details
                }
            };

            var value = context.ReadObject(newLimits.ToJson(), "overwrite-license-limits");
            PutValueDirectly(context, ServerStore.LicenseLimitsStorageKey, value, index);
        }

        private void ShrinkClusterTopology(ClusterOperationContext context, ClusterTopology clusterTopology, string newTag, long index)
        {
            _parent.UpdateNodeTag(context, newTag);

            var topology = new ClusterTopology(
                clusterTopology.TopologyId,
                new Dictionary<string, string>
                {
                    [newTag] = clusterTopology.GetUrlFromTag(newTag)
                },
                new Dictionary<string, string>(),
                new Dictionary<string, string>(),
                newTag,
                index
            );

            _parent.SetTopology(context, topology);
        }

        private void SqueezeDatabasesToSingleNodeCluster(ClusterOperationContext context, string oldTag, string newTag)
        {
            var toDelete = new List<string>();
            var toShrink = new List<DatabaseRecord>();

            foreach (var name in GetDatabaseNames(context))
            {
                using (var rawRecord = ReadRawDatabaseRecord(context, name))
                {
                    var topology = rawRecord.Topology;
                    if (topology.RelevantFor(oldTag) == false)
                    {
                        toDelete.Add(name);
                    }
                    else
                    {
                        if (topology.RelevantFor(newTag) && topology.Count == 1)
                            continue;

                        var record = rawRecord.MaterializedRecord;
                        record.Topology = new DatabaseTopology();
                        record.Topology.Members.Add(newTag);
                        toShrink.Add(record);
                    }
                }
            }

            if (toShrink.Count == 0 && toDelete.Count == 0)
                return;

            if (_parent.Log.IsOperationsEnabled)
            {
                _parent.Log.Operations($"Squeezing databases, new tag is {newTag}, old tag is {oldTag}.");

                if (toShrink.Count > 0)
                    _parent.Log.Operations($"Databases to shrink: {string.Join(',', toShrink.Select(r => r.DatabaseName))}");

                if (toDelete.Count > 0)
                    _parent.Log.Operations($"Databases to delete: {string.Join(',', toDelete)}");
            }

            var items = context.Transaction.InnerTransaction.OpenTable(ItemsSchema, Items);
            var cmd = new DynamicJsonValue
            {
                ["Type"] = "Switch to single leader"
            };

            var index = _parent.InsertToLeaderLog(context, _parent.CurrentTerm, context.ReadObject(cmd, "single-leader"), RachisEntryFlags.Noop);

            foreach (var databaseName in toDelete)
            {
                var dbKey = "db/" + databaseName;
                using (Slice.From(context.Allocator, dbKey.ToLowerInvariant(), out Slice valueNameLowered))
                {
                    DeleteDatabaseRecord(context, index, items, valueNameLowered, databaseName, null);
                }
            }

            if (toShrink.Count == 0)
                return;

            var tasks = new List<Func<Task>>();
            var type = "ClusterTopologyChanged";

            foreach (var record in toShrink)
            {
                record.Topology.Stamp = new LeaderStamp
                {
                    Index = index,
                    LeadersTicks = 0,
                    Term = _parent.CurrentTerm
                };

                var dbKey = "db/" + record.DatabaseName;
                using (Slice.From(context.Allocator, dbKey, out Slice valueName))
                using (Slice.From(context.Allocator, dbKey.ToLowerInvariant(), out Slice valueNameLowered))
                {
                    var updatedDatabaseBlittable = DocumentConventions.DefaultForServer.Serialization.DefaultConverter.ToBlittable(record, context);
                    UpdateValue(index, items, valueNameLowered, valueName, updatedDatabaseBlittable);
                }

                tasks.Add(() => Changes.OnDatabaseChanges(record.DatabaseName, index, type, DatabasesLandlord.ClusterDatabaseChangeType.RecordChanged, null));
            }

            ExecuteManyOnDispose(context, index, type, tasks);
        }

        public unsafe void PutLocalState(TransactionOperationContext context, string thumbprint, BlittableJsonReaderObject value, CertificateDefinition certificateDefinition)
        {
            PutCertificateCommand.ValidateCertificateDefinition(certificateDefinition);

            var localState = context.Transaction.InnerTransaction.CreateTree(LocalNodeStateTreeName);
            using (localState.DirectAdd(thumbprint, value.Size, out var ptr))
            {
                value.CopyTo(ptr);
            }
        }

        public void DeleteLocalState<TTransaction>(TransactionOperationContext<TTransaction> context, string thumbprint)
            where TTransaction : RavenTransaction
        {
            var localState = context.Transaction.InnerTransaction.CreateTree(LocalNodeStateTreeName);
            localState.Delete(thumbprint);
        }

        public void DeleteLocalState(TransactionOperationContext context, List<string> thumbprints)
        {
            var localState = context.Transaction.InnerTransaction.CreateTree(LocalNodeStateTreeName);
            foreach (var thumbprint in thumbprints)
            {
                localState.Delete(thumbprint);
            }
        }

        public unsafe BlittableJsonReaderObject GetLocalStateByThumbprint<TTransaction>(TransactionOperationContext<TTransaction> context, string thumbprint)
            where TTransaction : RavenTransaction
        {
            var localState = context.Transaction.InnerTransaction.ReadTree(LocalNodeStateTreeName);
            var read = localState.Read(thumbprint);
            if (read == null)
                return null;
            BlittableJsonReaderObject localStateBlittable = new BlittableJsonReaderObject(read.Reader.Base, read.Reader.Length, context);

            Transaction.DebugDisposeReaderAfterTransaction(context.Transaction.InnerTransaction, localStateBlittable);
            return localStateBlittable;
        }

        public IEnumerable<string> GetCertificateThumbprintsFromLocalState(TransactionOperationContext context)
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

        public static IEnumerable<(string Key, BlittableJsonReaderObject Cert)> GetAllCertificatesFromLocalState(TransactionOperationContext context)
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
                    yield return GetCertificate(context, it);
                } while (it.MoveNext());
            }
        }

        private static unsafe (string Key, BlittableJsonReaderObject Cert) GetCertificate(TransactionOperationContext context, TreeIterator treeIterator)
        {
            var reader = treeIterator.CreateReaderForCurrent();
            return GetCertificate(context, reader.Base, reader.Length, treeIterator.CurrentKey.ToString());
        }

        public static IEnumerable<(string Thumbprint, BlittableJsonReaderObject Certificate)> GetAllCertificatesFromCluster(TransactionOperationContext context, long start, long take)
        {
            var certTable = context.Transaction.InnerTransaction.OpenTable(CertificatesSchema, CertificatesSlice);

            foreach (var result in certTable.SeekByPrimaryKeyPrefix(Slices.Empty, Slices.Empty, start))
            {
                if (take-- <= 0)
                    yield break;

                yield return GetCertificate(context, result.Value);
            }
        }

        public IEnumerable<string> GetCertificateThumbprintsFromCluster<TTransaction>(TransactionOperationContext<TTransaction> context)
            where TTransaction : RavenTransaction
        {
            var certTable = context.Transaction.InnerTransaction.OpenTable(CertificatesSchema, CertificatesSlice);

            foreach (var result in certTable.SeekByPrimaryKeyPrefix(Slices.Empty, Slices.Empty, 0))
            {
                yield return result.Key.ToString();
            }
        }

        public IEnumerable<(string ItemName, BlittableJsonReaderObject Value)> ItemsStartingWith<TTransaction>(TransactionOperationContext<TTransaction> context, string prefix, long start, long take)
            where TTransaction : RavenTransaction
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

        public BlittableJsonReaderObject GetItem<TTransaction>(TransactionOperationContext<TTransaction> context, string key)
            where TTransaction : RavenTransaction
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

        public void ExecuteCompareExchange(ClusterOperationContext context, string type, BlittableJsonReaderObject cmd, long index, out object result)
        {
            Exception exception = null;
            CompareExchangeCommandBase compareExchange = null;
            try
            {
                var items = context.Transaction.InnerTransaction.OpenTable(CompareExchangeSchema, CompareExchange);
                compareExchange = (CompareExchangeCommandBase)JsonDeserializationCluster.Commands[type](cmd);

                result = compareExchange.Execute(context, items, index);

                OnTransactionDispose(context, index);
            }
            catch (Exception e)
            {
                exception = e;
                throw;
            }
            finally
            {
                LogCommand(type, index, exception, compareExchange?.AdditionalDebugInformation(exception));
            }
        }

        private void OnTransactionDispose(ClusterOperationContext context, long index)
        {
            context.Transaction.InnerTransaction.LowLevelTransaction.AfterCommitWhenNewReadTransactionsPrevented += _ =>
            {
                _rachisLogIndexNotifications.AddTask(index);
                NotifyAndSetCompleted(index);
            };
        }

        public (long Index, BlittableJsonReaderObject Value) GetCompareExchangeValue(TransactionOperationContext context, string key)
        {
            using (Slice.From(context.Allocator, key, out Slice keySlice))
                return GetCompareExchangeValue(context, keySlice);
        }

        public (long Index, BlittableJsonReaderObject Value) GetCompareExchangeValue(TransactionOperationContext context, Slice key)
        {
            var items = context.Transaction.InnerTransaction.OpenTable(CompareExchangeSchema, CompareExchange);

            if (items.ReadByKey(key, out var reader))
            {
                var index = ReadCompareExchangeOrTombstoneIndex(reader);
                var value = ReadCompareExchangeValue(context, reader);
                return (index, value);
            }

            return (-1, null);
        }

        public IEnumerable<(CompareExchangeKey Key, long Index, BlittableJsonReaderObject Value)> GetCompareExchangeValuesStartsWith(TransactionOperationContext context,
            string dbName, string prefix, long start = 0, long pageSize = 1024)
        {
            var items = context.Transaction.InnerTransaction.OpenTable(CompareExchangeSchema, CompareExchange);
            using (Slice.From(context.Allocator, prefix, out Slice keySlice))
            {
                foreach (var item in items.SeekByPrimaryKeyPrefix(keySlice, Slices.Empty, start))
                {
                    pageSize--;
                    var key = ReadCompareExchangeKey(context, item.Value.Reader, dbName);
                    var index = ReadCompareExchangeOrTombstoneIndex(item.Value.Reader);
                    var value = ReadCompareExchangeValue(context, item.Value.Reader);
                    yield return (key, index, value);

                    if (pageSize == 0)
                        yield break;
                }
            }
        }

        public IEnumerable<(CompareExchangeKey Key, long Index, BlittableJsonReaderObject Value)> GetCompareExchangeFromPrefix(TransactionOperationContext context, string dbName, long fromIndex, long take)
        {
            using (CompareExchangeCommandBase.GetPrefixIndexSlices(context.Allocator, dbName, fromIndex, out var buffer))
            {
                var table = context.Transaction.InnerTransaction.OpenTable(CompareExchangeSchema, CompareExchange);
                using (Slice.External(context.Allocator, buffer, buffer.Length, out var keySlice))
                using (Slice.External(context.Allocator, buffer, buffer.Length - sizeof(long), out var prefix))
                {
                    foreach (var tvr in table.SeekForwardFromPrefix(CompareExchangeSchema.Indexes[CompareExchangeIndex], keySlice, prefix, 0))
                    {
                        if (take-- <= 0)
                            yield break;

                        var key = ReadCompareExchangeKey(context, tvr.Result.Reader, dbName);
                        var index = ReadCompareExchangeOrTombstoneIndex(tvr.Result.Reader);
                        var value = ReadCompareExchangeValue(context, tvr.Result.Reader);

                        yield return (key, index, value);
                    }
                }
            }
        }

        public long GetLastCompareExchangeIndexForDatabase(TransactionOperationContext context, string databaseName)
        {
            CompareExchangeCommandBase.GetDbPrefixAndLastSlices(context.Allocator, databaseName, out var prefix, out var last);

            using (prefix.Scope)
            using (last.Scope)
            {
                var table = context.Transaction.InnerTransaction.OpenTable(CompareExchangeSchema, CompareExchange);

                var tvh = table.SeekOneBackwardFrom(CompareExchangeSchema.Indexes[CompareExchangeIndex], prefix.Slice, last.Slice);

                if (tvh == null)
                    return 0;

                return ReadCompareExchangeOrTombstoneIndex(tvh.Reader);
            }
        }

        public long GetLastCompareExchangeTombstoneIndexForDatabase(TransactionOperationContext context, string databaseName)
        {
            CompareExchangeCommandBase.GetDbPrefixAndLastSlices(context.Allocator, databaseName, out var prefix, out var last);

            using (prefix.Scope)
            using (last.Scope)
            {
                var table = context.Transaction.InnerTransaction.OpenTable(CompareExchangeTombstoneSchema, CompareExchangeTombstones);

                var tvh = table.SeekOneBackwardFrom(CompareExchangeTombstoneSchema.Indexes[CompareExchangeTombstoneIndex], prefix.Slice, last.Slice);

                if (tvh == null)
                    return 0;

                return ReadCompareExchangeOrTombstoneIndex(tvh.Reader);
            }
        }

        public IEnumerable<(CompareExchangeKey Key, long Index)> GetCompareExchangeTombstonesByKey(TransactionOperationContext context,
            string databaseName, long fromIndex = 0, long take = long.MaxValue)
        {
            using (CompareExchangeCommandBase.GetPrefixIndexSlices(context.Allocator, databaseName, fromIndex, out var buffer))
            {
                var table = context.Transaction.InnerTransaction.OpenTable(CompareExchangeTombstoneSchema, CompareExchangeTombstones);
                using (Slice.External(context.Allocator, buffer, buffer.Length, out var keySlice))
                using (Slice.External(context.Allocator, buffer, buffer.Length - sizeof(long), out var prefix))
                {
                    foreach (var tvr in table.SeekForwardFromPrefix(CompareExchangeTombstoneSchema.Indexes[CompareExchangeTombstoneIndex], keySlice, prefix, 0))
                    {
                        if (take-- <= 0)
                            yield break;

                        var key = ReadCompareExchangeKey(context, tvr.Result.Reader, databaseName);
                        var index = ReadCompareExchangeOrTombstoneIndex(tvr.Result.Reader);

                        yield return (key, index);
                    }
                }
            }
        }

        internal bool HasCompareExchangeTombstonesWithEtagGreaterThanStartAndLowerThanOrEqualToEnd(TransactionOperationContext context, string databaseName, long start, long end)
        {
            if (start >= end)
                return false;

            var table = context.Transaction.InnerTransaction.OpenTable(CompareExchangeTombstoneSchema, CompareExchangeTombstones);
            if (table == null)
                return false;

            using (CompareExchangeCommandBase.GetPrefixIndexSlices(context.Allocator, databaseName, start + 1, out var buffer)) // start + 1 => we want greater than
            using (Slice.External(context.Allocator, buffer, buffer.Length, out var keySlice))
            using (Slice.External(context.Allocator, buffer, buffer.Length - sizeof(long), out var prefix))
            {
                foreach (var tvr in table.SeekForwardFromPrefix(CompareExchangeTombstoneSchema.Indexes[CompareExchangeTombstoneIndex], keySlice, prefix, 0))
                {
                    var index = ReadCompareExchangeOrTombstoneIndex(tvr.Result.Reader);
                    if (index <= end)
                        return true;

                    return false;
                }
            }

            return false;
        }

        private bool DeleteExpiredCompareExchange(ClusterOperationContext context, string type, BlittableJsonReaderObject cmd, long index)
        {
            try
            {
                if (cmd.TryGet(nameof(DeleteExpiredCompareExchangeCommand.Ticks), out long ticks) == false)
                    throw new RachisApplyException($"{nameof(DeleteExpiredCompareExchangeCommand)} must contain a {nameof(DeleteExpiredCompareExchangeCommand.Ticks)} property");

                if (cmd.TryGet(nameof(CleanCompareExchangeTombstonesCommand.Take), out long take) == false)
                    throw new RachisApplyException($"{nameof(DeleteExpiredCompareExchangeCommand)} must contain a {nameof(DeleteExpiredCompareExchangeCommand.Take)} property");

                var items = context.Transaction.InnerTransaction.OpenTable(CompareExchangeSchema, CompareExchange);

                return CompareExchangeExpirationStorage.DeleteExpiredCompareExchange(context, items, ticks, take);
            }
            finally
            {
                NotifyValueChanged(context, type, index);
            }
        }

        private void ClearCompareExchangeTombstones(ClusterOperationContext context, string type, BlittableJsonReaderObject cmd, long index, out bool result)
        {
            string databaseName = null;

            try
            {
                if (cmd.TryGet(nameof(CleanCompareExchangeTombstonesCommand.DatabaseName), out databaseName) == false || string.IsNullOrEmpty(databaseName))
                    throw new RachisApplyException("Clear Compare Exchange command must contain a DatabaseName property");

                if (cmd.TryGet(nameof(CleanCompareExchangeTombstonesCommand.MaxRaftIndex), out long maxEtag) == false)
                    throw new RachisApplyException("Clear Compare Exchange command must contain a MaxRaftIndex property");

                if (cmd.TryGet(nameof(CleanCompareExchangeTombstonesCommand.Take), out long take) == false)
                    throw new RachisApplyException("Clear Compare Exchange command must contain a Take property");

                var databaseNameLowered = databaseName.ToLowerInvariant();
                result = DeleteCompareExchangeTombstonesUpToPrefix(context, databaseNameLowered, maxEtag, take);
            }
            finally
            {
                NotifyDatabaseAboutChanged(context, databaseName, index, type, DatabasesLandlord.ClusterDatabaseChangeType.ValueChanged, null);
            }
        }

        private static bool DeleteCompareExchangeTombstonesUpToPrefix(ClusterOperationContext context, string dbName, long upToIndex, long take = long.MaxValue)
        {
            using (Slice.From(context.Allocator, dbName, out var dbNameSlice))
            {
                var table = context.Transaction.InnerTransaction.OpenTable(CompareExchangeTombstoneSchema, CompareExchangeTombstones);
                return table.DeleteForwardUpToPrefix(dbNameSlice, upToIndex, take);
            }
        }

        private static unsafe CompareExchangeKey ReadCompareExchangeKey(TransactionOperationContext context, TableValueReader reader, string dbPrefix)
        {
            var ptr = reader.Read((int)CompareExchangeTable.Key, out var size);

            var storageKey = context.AllocateStringValue(null, ptr, size);
            return new CompareExchangeKey(storageKey, dbPrefix.Length + 1);
        }

        private static unsafe BlittableJsonReaderObject ReadCompareExchangeValue(TransactionOperationContext context, TableValueReader reader)
        {
            BlittableJsonReaderObject compareExchangeValue = new BlittableJsonReaderObject(reader.Read((int)CompareExchangeTable.Value, out var size), size, context);
            Transaction.DebugDisposeReaderAfterTransaction(context.Transaction.InnerTransaction, compareExchangeValue);
            return compareExchangeValue;
        }

        private static unsafe long ReadCompareExchangeOrTombstoneIndex(TableValueReader reader)
        {
            var index = *(long*)reader.Read((int)CompareExchangeTable.Index, out var size);
            Debug.Assert(size == sizeof(long));
            return index;
        }

        private static unsafe long ReadIdentitiesIndex(TableValueReader reader)
        {
            var index = *(long*)reader.Read((int)IdentitiesTable.Index, out var size);
            Debug.Assert(size == sizeof(long));
            return index;
        }

        public List<string> ItemKeysStartingWith(TransactionOperationContext context, string prefix, long start, long take)
        {
            var items = context.Transaction.InnerTransaction.OpenTable(ItemsSchema, Items);

            var results = new List<string>();

            var dbKey = prefix.ToLowerInvariant();
            using (Slice.From(context.Allocator, dbKey, out Slice loweredPrefix))
            {
                foreach (var result in items.SeekByPrimaryKeyPrefix(loweredPrefix, Slices.Empty, start))
                {
                    if (take-- <= 0)
                        break;

                    results.Add(GetCurrentItemKey(result.Value));
                }
            }

            return results;
        }

        public List<string> GetDatabaseNames<TTransaction>(TransactionOperationContext<TTransaction> context, long take = long.MaxValue)
            where TTransaction : RavenTransaction
        {
            var items = context.Transaction.InnerTransaction.OpenTable(ItemsSchema, Items);

            var names = new List<string>();

            const string dbKey = "db/";
            using (Slice.From(context.Allocator, dbKey, out Slice loweredPrefix))
            {
                foreach (var result in items.SeekByPrimaryKeyPrefix(loweredPrefix, Slices.Empty, 0))
                {
                    if (take-- <= 0)
                        break;

                    names.Add(GetCurrentItemKey(result.Value).Substring(3));
                }
            }

            return names;
        }

        public List<DatabaseRecord> GetAllDatabases<TTransaction>(TransactionOperationContext<TTransaction> context, long take = long.MaxValue)
            where TTransaction : RavenTransaction
        {
            var items = context.Transaction.InnerTransaction.OpenTable(ItemsSchema, Items);

            var records = new List<DatabaseRecord>();

            const string dbKey = "db/";
            using (Slice.From(context.Allocator, dbKey, out Slice loweredPrefix))
            {
                foreach (var result in items.SeekByPrimaryKeyPrefix(loweredPrefix, Slices.Empty, 0))
                {
                    if (take-- <= 0)
                        break;

                    var doc = Read(context, GetCurrentItemKey(result.Value));
                    if (doc == null)
                        continue;

                    records.Add(JsonDeserializationCluster.DatabaseRecord(doc));
                }
            }

            return records;
        }

        public static unsafe string GetCurrentItemKey(Table.TableValueHolder result)
        {
            return Encoding.UTF8.GetString(result.Reader.Read(1, out int size), size);
        }

        private static unsafe (string, BlittableJsonReaderObject) GetCurrentItem<TTransaction>(TransactionOperationContext<TTransaction> context, Table.TableValueHolder result)
            where TTransaction : RavenTransaction
        {
            var ptr = result.Reader.Read(2, out int size);
            var doc = new BlittableJsonReaderObject(ptr, size, context);
            var key = Encoding.UTF8.GetString(result.Reader.Read(1, out size), size);

            Transaction.DebugDisposeReaderAfterTransaction(context.Transaction.InnerTransaction, doc);
            return (key, doc);
        }

        public BlittableJsonReaderObject GetCertificateByThumbprint<TTransaction>(TransactionOperationContext<TTransaction> context, string thumbprint)
            where TTransaction : RavenTransaction
        {
            var certs = context.Transaction.InnerTransaction.OpenTable(CertificatesSchema, CertificatesSlice);

            using (Slice.From(context.Allocator, thumbprint.ToLowerInvariant(), out var thumbprintSlice))
            {
                var tvh = new Table.TableValueHolder();
                if (certs.ReadByKey(thumbprintSlice, out tvh.Reader) == false)
                    return null;
                return GetCertificate(context, tvh).Item2;
            }
        }

        private static unsafe (string Key, BlittableJsonReaderObject Cert) GetCertificate<TTransaction>(TransactionOperationContext<TTransaction> context, Table.TableValueHolder result)
            where TTransaction : RavenTransaction
        {
            var ptr = result.Reader.Read((int)CertificatesTable.Data, out var dataSize);
            var key = Encoding.UTF8.GetString(result.Reader.Read((int)CertificatesTable.Thumbprint, out var size), size);

            return GetCertificate(context, ptr, dataSize, key);
        }

        private static unsafe (string Key, BlittableJsonReaderObject Cert) GetCertificate<TTransaction>(TransactionOperationContext<TTransaction> context, byte* ptr, int size, string key)
            where TTransaction : RavenTransaction
        {
            var doc = new BlittableJsonReaderObject(ptr, size, context);
            Transaction.DebugDisposeReaderAfterTransaction(context.Transaction.InnerTransaction, doc);
            return (key, doc);
        }

        private static CertificateDefinition GetCertificateDefinition<TTransaction>(TransactionOperationContext<TTransaction> context, Table.TableValueHolder result)
            where TTransaction : RavenTransaction
        {
            return JsonDeserializationServer.CertificateDefinition(GetCertificate(context, result).Cert);
        }

        public List<CertificateDefinition> GetCertificatesByPinningHashSortedByExpiration(ClusterOperationContext context, string hash)
        {
            var list = GetCertificatesByPinningHash(context, hash).ToList();
            list.Sort((x, y) => Nullable.Compare<DateTime>(y.NotAfter, x.NotAfter));
            return list;
        }

        public IEnumerable<CertificateDefinition> GetCertificatesByPinningHash<TTransaction>(TransactionOperationContext<TTransaction> context, string hash)
            where TTransaction : RavenTransaction
        {
            var certs = context.Transaction.InnerTransaction.OpenTable(CertificatesSchema, CertificatesSlice);

            using (Slice.From(context.Allocator, hash, out Slice hashSlice))
            {
                foreach (var tvr in certs.SeekForwardFrom(CertificatesSchema.Indexes[CertificatesHashSlice], hashSlice, 0))
                {
                    var def = GetCertificateDefinition(context, tvr.Result);
                    if (def.PublicKeyPinningHash.Equals(hash) == false)
                        break;
                    yield return def;
                }
            }
        }

        public DatabaseRecord ReadDatabase<TTransaction>(TransactionOperationContext<TTransaction> context, string name)
            where TTransaction : RavenTransaction
        {
            return ReadDatabase(context, name, out long _);
        }

        public RawDatabaseRecord ReadRawDatabaseRecord<TTransaction>(TransactionOperationContext<TTransaction> context, string name, out long etag)
            where TTransaction : RavenTransaction
        {
            var rawRecord = Read(context, "db/" + name.ToLowerInvariant(), out etag);
            if (rawRecord == null)
                return null;

            return new RawDatabaseRecord(rawRecord);
        }

        public RawDatabaseRecord ReadRawDatabaseRecord<TTransaction>(TransactionOperationContext<TTransaction> context, string name)
            where TTransaction : RavenTransaction
        {
            return ReadRawDatabaseRecord(context, name, out _);
        }

        public bool DatabaseExists<TTransaction>(TransactionOperationContext<TTransaction> context, string name)
            where TTransaction : RavenTransaction
        {
            var dbKey = "db/" + name.ToLowerInvariant();
            var items = context.Transaction.InnerTransaction.OpenTable(ItemsSchema, Items);

            using (Slice.From(context.Allocator, dbKey, out var key))
                return items.VerifyKeyExists(key);
        }

        public DatabaseRecord ReadDatabase<TTransaction>(TransactionOperationContext<TTransaction> context, string name, out long etag)
            where TTransaction : RavenTransaction
        {
            using (var databaseRecord = ReadRawDatabaseRecord(context, name, out etag))
            {
                if (databaseRecord == null)
                    return null;

                return databaseRecord.MaterializedRecord;
            }
        }

        public DatabaseTopology ReadDatabaseTopology(TransactionOperationContext context, string name)
        {
            using (var databaseRecord = ReadRawDatabaseRecord(context, name))
            {
                var topology = databaseRecord.Topology;
                if (topology == null)
                    throw new InvalidOperationException($"The database record '{name}' doesn't contain topology.");

                return topology;
            }
        }

        public bool TryReadPullReplicationDefinition(string database, string definitionName, TransactionOperationContext context, out PullReplicationDefinition pullReplication)
        {
            pullReplication = null;
            try
            {
                pullReplication = ReadPullReplicationDefinition(database, definitionName, context);
                return true;
            }
            catch
            {
                return false;
            }
        }

        public TimeSeriesConfiguration ReadTimeSeriesConfiguration(string database)
        {
            using (ContextPoolForReadOnlyOperations.AllocateOperationContext(out ClusterOperationContext context))
            using (context.OpenReadTransaction())
            using (var databaseRecord = ReadRawDatabaseRecord(context, database))
            {
                return databaseRecord.TimeSeriesConfiguration;
            }
        }

        public PullReplicationDefinition ReadPullReplicationDefinition(string database, string definitionName, TransactionOperationContext context)
        {
            using (var databaseRecord = ReadRawDatabaseRecord(context, database))
            {
                if (databaseRecord == null)
                {
                    throw new DatabaseDoesNotExistException($"The database '{database}' doesn't exists.");
                }

                var definition = databaseRecord.GetHubPullReplicationByName(definitionName);
                if (definition == null)
                {
                    throw new InvalidOperationException($"Pull replication with the name '{definitionName}' isn't defined for the database '{database}'.");
                }

                return definition;
            }
        }

        public IEnumerable<(string Prefix, long Value, long index)> GetIdentitiesFromPrefix(TransactionOperationContext context, string dbName, long fromIndex, long take)
        {
            using (CompareExchangeCommandBase.GetPrefixIndexSlices(context.Allocator, dbName, fromIndex, out var buffer))
            {
                var items = context.Transaction.InnerTransaction.OpenTable(IdentitiesSchema, Identities);

                using (Slice.External(context.Allocator, buffer, buffer.Length, out var keySlice))
                using (Slice.External(context.Allocator, buffer, buffer.Length - sizeof(long), out var prefix))
                {
                    foreach (var tvr in items.SeekForwardFromPrefix(IdentitiesSchema.Indexes[IdentitiesIndex], keySlice, prefix, 0))
                    {
                        if (take-- <= 0)
                            yield break;

                        var key = GetIdentityKey(tvr.Result.Reader, dbName);
                        var value = GetIdentityValue(tvr.Result.Reader);
                        var index = ReadIdentitiesIndex(tvr.Result.Reader);

                        yield return (key, value, index);
                    }
                }
            }
        }

        private static unsafe long GetIdentityValue(TableValueReader reader)
        {
            return *(long*)reader.Read((int)IdentitiesTable.Value, out var _);
        }

        private static unsafe string GetIdentityKey(TableValueReader reader, string dbName)
        {
            var ptr = reader.Read((int)IdentitiesTable.Key, out var size);
            var key = Encodings.Utf8.GetString(ptr, size).Substring(dbName.Length + 1);
            return key;
        }

        public long GetNumberOfIdentities(TransactionOperationContext context, string databaseName)
        {
            var items = context.Transaction.InnerTransaction.OpenTable(IdentitiesSchema, Identities);
            var identities = items.GetTree(IdentitiesSchema.Key);
            var prefix = IncrementClusterIdentityCommand.GetStorageKey(databaseName, null);

            return GetNumberOf(identities, prefix, context);
        }

        public long GetNumberOfCompareExchange(TransactionOperationContext context, string databaseName)
        {
            var items = context.Transaction.InnerTransaction.OpenTable(CompareExchangeSchema, CompareExchange);
            var compareExchange = items.GetTree(CompareExchangeSchema.Key);
            var prefix = CompareExchangeKey.GetStorageKey(databaseName, null);
            return GetNumberOf(compareExchange, prefix, context);
        }

        public long GetNumberOfCompareExchangeTombstones(TransactionOperationContext context, string databaseName)
        {
            var items = context.Transaction.InnerTransaction.OpenTable(CompareExchangeTombstoneSchema, CompareExchangeTombstones);
            var compareExchangeTombstone = items.GetTree(CompareExchangeTombstoneSchema.Key);
            var prefix = CompareExchangeKey.GetStorageKey(databaseName, null);
            return GetNumberOf(compareExchangeTombstone, prefix, context);
        }

        public bool HasCompareExchangeTombstones(TransactionOperationContext context, string databaseName)
        {
            var items = context.Transaction.InnerTransaction.OpenTable(CompareExchangeTombstoneSchema, CompareExchangeTombstones);
            var compareExchangeTombstone = items.GetTree(CompareExchangeTombstoneSchema.Key);
            var prefix = CompareExchangeKey.GetStorageKey(databaseName, null);
            return HasPrefixOf(compareExchangeTombstone, prefix, context);
        }

        private static bool HasPrefixOf(Tree tree, string prefix, TransactionOperationContext context)
        {
            using (Slice.From(context.Allocator, prefix, out var prefixAsSlice))
            {
                using (var it = tree.Iterate(prefetch: false))
                {
                    it.SetRequiredPrefix(prefixAsSlice);

                    if (it.Seek(prefixAsSlice) == false)
                        return false;

                    return true;
                }
            }
        }

        private static long GetNumberOf(Tree tree, string prefix, TransactionOperationContext context)
        {
            using (Slice.From(context.Allocator, prefix, out var prefixAsSlice))
            {
                using (var it = tree.Iterate(prefetch: false))
                {
                    it.SetRequiredPrefix(prefixAsSlice);

                    if (it.Seek(prefixAsSlice) == false)
                        return 0;

                    var count = 0;

                    do
                    {
                        count++;
                    } while (it.MoveNext());

                    return count;
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
            var lowerName = name.ToLowerInvariant();
            using (Slice.From(context.Allocator, lowerName, out Slice key))
            {
                return ReadInternal(context, out etag, key);
            }
        }

        public static unsafe BlittableJsonReaderObject ReadInternal<T>(TransactionOperationContext<T> context, out long etag, Slice key)
            where T : RavenTransaction
        {
            var items = context.Transaction.InnerTransaction.OpenTable(ItemsSchema, Items);
            if (items == null || items.ReadByKey(key, out TableValueReader reader) == false)
            {
                etag = 0;
                return null;
            }

            var ptr = reader.Read(2, out int size);
            var doc = new BlittableJsonReaderObject(ptr, size, context);
            Transaction.DebugDisposeReaderAfterTransaction(context.Transaction.InnerTransaction, doc);
            etag = Bits.SwapBytes(*(long*)reader.Read(3, out size));
            Debug.Assert(size == sizeof(long));

            return doc;
        }

        public static IEnumerable<(Slice Key, BlittableJsonReaderObject Value)> ReadValuesStartingWith<TTransaction>(TransactionOperationContext<TTransaction> context, string startsWithKey)
            where TTransaction : RavenTransaction
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

        private static unsafe int GetDataAndEtagTupleFromReader<TTransaction>(TransactionOperationContext<TTransaction> context, TableValueReader reader, out BlittableJsonReaderObject doc, out long etag)
            where TTransaction : RavenTransaction
        {
            var ptr = reader.Read(2, out int size);
            doc = new BlittableJsonReaderObject(ptr, size, context);

            Transaction.DebugDisposeReaderAfterTransaction(context.Transaction.InnerTransaction, doc);
            etag = Bits.SwapBytes(*(long*)reader.Read(3, out size));
            Debug.Assert(size == sizeof(long));
            return size;
        }

        private int ClusterReadResponseAndGetVersion(JsonOperationContext ctx, BlittableJsonTextWriter writer, Stream stream, string url)
        {
            using (var response = ctx.ReadForMemory(stream, "cluster-ConnectToPeer-header-response"))
            {
                var reply = JsonDeserializationServer.TcpConnectionHeaderResponse(response);
                switch (reply.Status)
                {
                    case TcpConnectionStatus.Ok:
                        return reply.Version;

                    case TcpConnectionStatus.AuthorizationFailed:
                        throw new AuthorizationException($"Unable to access  {url} because {reply.Message}");
                    case TcpConnectionStatus.TcpVersionMismatch:
                        if (reply.Version != TcpNegotiation.OutOfRangeStatus)
                        {
                            return reply.Version;
                        }
                        //Kindly request the server to drop the connection
                        ctx.Write(writer, new DynamicJsonValue
                        {
                            [nameof(TcpConnectionHeaderMessage.DatabaseName)] = null,
                            [nameof(TcpConnectionHeaderMessage.Operation)] = TcpConnectionHeaderMessage.OperationTypes.Drop,
                            [nameof(TcpConnectionHeaderMessage.OperationVersion)] = TcpConnectionHeaderMessage.ClusterTcpVersion,
                            [nameof(TcpConnectionHeaderMessage.Info)] = $"Couldn't agree on cluster tcp version ours:{TcpConnectionHeaderMessage.ClusterTcpVersion} theirs:{reply.Version}"
                        });
                        throw new InvalidOperationException($"Unable to access  {url} because {reply.Message}");
                }
            }

            return TcpConnectionHeaderMessage.ClusterTcpVersion;
        }

        public override async Task<RachisConnection> ConnectToPeer(string url, string tag, X509Certificate2 certificate)
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
            string choosenUrl = null;
            Stream stream = null;
            try
            {
                (tcpClient, choosenUrl) = await TcpUtils.ConnectAsyncWithPriority(info, _parent.TcpConnectionTimeout).ConfigureAwait(false);
                stream = await TcpUtils.WrapStreamWithSslAsync(tcpClient, info, _parent.ClusterCertificate, _parent.CipherSuitesPolicy, _parent.TcpConnectionTimeout);

                var parameters = new TcpNegotiateParameters
                {
                    Database = null,
                    Operation = TcpConnectionHeaderMessage.OperationTypes.Cluster,
                    Version = TcpConnectionHeaderMessage.ClusterTcpVersion,
                    ReadResponseAndGetVersionCallback = ClusterReadResponseAndGetVersion,
                    DestinationUrl = choosenUrl,
                    DestinationNodeTag = tag,
                    SourceNodeTag = _parent.Tag
                };

                TcpConnectionHeaderMessage.SupportedFeatures supportedFeatures;
                using (ContextPoolForReadOnlyOperations.AllocateOperationContext(out JsonOperationContext context))
                {
                    supportedFeatures = TcpNegotiation.NegotiateProtocolVersion(context, stream, parameters);

                    if (supportedFeatures.ProtocolVersion <= 0)
                    {
                        throw new InvalidOperationException(
                            $"state machine ConnectToPeer {url}: TCP negotiation resulted with an invalid protocol version:{supportedFeatures.ProtocolVersion}");
                    }
                }

                return new RachisConnection
                {
                    Stream = stream,
                    SupportedFeatures = supportedFeatures,
                    Disconnect = () =>
                    {
                        {
                            try
                            {
                                tcpClient.Client.Disconnect(false);
                            }
                            catch (ObjectDisposedException)
                            {
                                //Happens, we don't really care at this point
                            }
                        }
                    }
                };
            }
            catch (Exception)
            {
                stream?.Dispose();
                tcpClient?.Dispose();
                throw;
            }
        }

        private void ToggleDatabasesState(BlittableJsonReaderObject cmd, ClusterOperationContext context, string type, long index)
        {
            var command = (ToggleDatabasesStateCommand)JsonDeserializationCluster.Commands[type](cmd);
            if (command.Value == null)
                throw new RachisInvalidOperationException($"{nameof(ToggleDatabasesStateCommand.Parameters)} is null for command type: {type}");

            if (command.Value.DatabaseNames == null)
                throw new RachisInvalidOperationException($"{nameof(ToggleDatabasesStateCommand.Parameters.DatabaseNames)} is null for command type: {type}");

            var toUpdate = new List<(string Key, BlittableJsonReaderObject DatabaseRecord, string DatabaseName)>();

            foreach (var databaseName in command.Value.DatabaseNames)
            {
                var key = "db/" + databaseName;
                using (Slice.From(context.Allocator, key.ToLowerInvariant(), out Slice valueNameLowered))
                {
                    var oldDatabaseRecord = ReadInternal(context, out _, valueNameLowered);
                    if (oldDatabaseRecord == null)
                        continue;

                    var rawDatabaseRecord = new RawDatabaseRecord(oldDatabaseRecord);
                    switch (command.Value.Type)
                    {
                        case ToggleDatabasesStateCommand.Parameters.ToggleType.Databases:
                            if (rawDatabaseRecord.IsDisabled == command.Value.Disable)
                                continue;

                            oldDatabaseRecord.Modifications = new DynamicJsonValue(oldDatabaseRecord)
                            {
                                [nameof(DatabaseRecord.Disabled)] = command.Value.Disable
                            };

                            break;

                        case ToggleDatabasesStateCommand.Parameters.ToggleType.Indexes:
                            var settings = rawDatabaseRecord.Settings;
                            var configurationKey = RavenConfiguration.GetKey(x => x.Indexing.Disabled);
                            if (settings.TryGetValue(configurationKey, out var indexingDisabledString) &&
                                bool.TryParse(indexingDisabledString, out var currentlyIndexingDisabled) &&
                                currentlyIndexingDisabled == command.Value.Disable)
                            {
                                continue;
                            }

                            settings[configurationKey] = command.Value.Disable.ToString();

                            oldDatabaseRecord.Modifications = new DynamicJsonValue(oldDatabaseRecord)
                            {
                                [nameof(DatabaseRecord.Settings)] = TypeConverter.ToBlittableSupportedType(settings)
                            };

                            break;

                        case ToggleDatabasesStateCommand.Parameters.ToggleType.DynamicDatabaseDistribution:
                            if (rawDatabaseRecord.IsEncrypted)
                            {
                                throw new RachisInvalidOperationException($"Cannot toggle '{nameof(DatabaseTopology.DynamicNodesDistribution)}' for encrypted database: {databaseName}");
                            }

                            var topology = rawDatabaseRecord.Topology;
                            if (topology == null)
                                continue;

                            var enable = command.Value.Disable == false;
                            if (topology.DynamicNodesDistribution == enable)
                                continue;

                            topology.DynamicNodesDistribution = enable;

                            oldDatabaseRecord.Modifications = new DynamicJsonValue(oldDatabaseRecord)
                            {
                                [nameof(DatabaseRecord.Topology)] = topology.ToJson()
                            };

                            break;

                        default:
                            throw new RachisInvalidOperationException($"Argument out of range for `{nameof(ToggleDatabasesStateCommand.Value.Type)}`");
                    }

                    using (oldDatabaseRecord)
                    {
                        var updatedDatabaseRecord = context.ReadObject(oldDatabaseRecord, "updated-database-record");
                        toUpdate.Add((Key: key, DatabaseRecord: updatedDatabaseRecord, DatabaseName: databaseName));
                    }
                }
            }

            var items = context.Transaction.InnerTransaction.OpenTable(ItemsSchema, Items);
            ApplyDatabaseRecordUpdates(toUpdate, type, index, index, items, context);
        }

        private void UpdateDatabasesWithServerWideBackupConfiguration(ClusterOperationContext context, string type, ServerWideBackupConfiguration serverWideBackupConfiguration)
        {
            if (serverWideBackupConfiguration == null)
                throw new RachisInvalidOperationException($"Server-wide backup configuration is null for command type: {type}");

            if (serverWideBackupConfiguration.Name == null)
                throw new RachisInvalidOperationException($"`{nameof(ServerWideExternalReplication.Name)}` is null or empty for command type: {type}");

            // the server-wide backup name might have changed
            var serverWideBlittable = context.ReadObject(serverWideBackupConfiguration.ToJson(), "server-wide-configuration");
            var items = context.Transaction.InnerTransaction.OpenTable(ItemsSchema, Items);

            const string dbKey = "db/";
            var toUpdate = new List<(string Key, BlittableJsonReaderObject DatabaseRecord, string DatabaseName)>();
            (long? TaskId, string name) old = default;

            var allServerWideBackupNames = GetSeverWideBackupNames(context);

            using (Slice.From(context.Allocator, dbKey, out var loweredPrefix))
            {
                foreach (var result in items.SeekByPrimaryKeyPrefix(loweredPrefix, Slices.Empty, 0))
                {
                    var (key, oldDatabaseRecord) = GetCurrentItem(context, result.Value);
                    var databaseName = key.Substring(dbKey.Length);
                    var newBackups = new DynamicJsonArray();

                    var periodicBackupConfiguration = JsonDeserializationCluster.PeriodicBackupConfiguration(serverWideBlittable);
                    oldDatabaseRecord.TryGet(nameof(DatabaseRecord.Encrypted), out bool encrypted);
                    PutServerWideBackupConfigurationCommand.UpdateTemplateForDatabase(periodicBackupConfiguration, databaseName, encrypted);

                    if (oldDatabaseRecord.TryGet(nameof(DatabaseRecord.PeriodicBackups), out BlittableJsonReaderArray backups))
                    {
                        var isBackupToEditFound = false;
                        foreach (BlittableJsonReaderObject backup in backups)
                        {
                            //Even though we rebuild the whole configurations list just one should be modified
                            //In addition the same configuration should be modified in all databases
                            if (isBackupToEditFound || IsServerWideBackupToEdit(backup, periodicBackupConfiguration.Name, allServerWideBackupNames) == false)
                            {
                                newBackups.Add(backup);
                                continue;
                            }

                            isBackupToEditFound = true;

                            if (backup.TryGet(nameof(PeriodicBackupConfiguration.TaskId), out long taskId))
                                periodicBackupConfiguration.TaskId = taskId;
                        }
                    }

                    if (serverWideBackupConfiguration.IsExcluded(databaseName) == false)
                    {
                        newBackups.Add(periodicBackupConfiguration.ToJson());
                    }

                    using (oldDatabaseRecord)
                    {
                        oldDatabaseRecord.Modifications = new DynamicJsonValue(oldDatabaseRecord)
                        {
                            [nameof(DatabaseRecord.PeriodicBackups)] = newBackups
                        };

                        var updatedDatabaseRecord = context.ReadObject(oldDatabaseRecord, "updated-database-record");
                        toUpdate.Add((Key: key, DatabaseRecord: updatedDatabaseRecord, DatabaseName: databaseName));
                    }
                }
            }

            ApplyDatabaseRecordUpdates(toUpdate, type, old.TaskId ?? serverWideBackupConfiguration.TaskId, serverWideBackupConfiguration.TaskId, items, context);
        }

        private static bool IsServerWideBackupToEdit(BlittableJsonReaderObject databaseTask, string serverWideTaskName, HashSet<string> allServerWideTasksNames)
        {
            return IsServerWideTaskToEdit(
                databaseTask,
                serverWideTaskName,
                nameof(PeriodicBackupConfiguration.Name),
                ServerWideBackupConfiguration.NamePrefix,
                allServerWideTasksNames);
        }
        
        private static bool IsServerWideExternalReplicationToEdit(BlittableJsonReaderObject databaseTask, string serverWideTaskName, HashSet<string> allServerWideTasksNames)
        {
            return IsServerWideTaskToEdit(
                databaseTask, 
                serverWideTaskName, 
                nameof(ExternalReplication.Name), 
                ServerWideExternalReplication.NamePrefix,
                allServerWideTasksNames);
        }
        
        private static bool IsServerWideTaskToEdit(
            BlittableJsonReaderObject databaseTask, 
            string serverWideTaskName, 
            string propName, 
            string serverWidePrefix, 
            HashSet<string> allServerWideTasksNames)
        {
            if (databaseTask.TryGet(propName, out string taskName) == false || taskName == null)
                return false;

            if (taskName.StartsWith(serverWidePrefix) == false)
                return false;

            if (taskName.Equals(serverWideTaskName, StringComparison.OrdinalIgnoreCase))
            {
                // server-wide task to update when the name wasn't modified
                return true;
            }

            if (allServerWideTasksNames.Contains(taskName) == false)
            {
                // server-wide task to update when the name was modified
                return true;
            }

            return false;
        }

        private static unsafe HashSet<string> GetSeverWideBackupNames(ClusterOperationContext context)
        {
            var items = context.Transaction.InnerTransaction.OpenTable(ItemsSchema, Items);
            using (Slice.From(context.Allocator, ServerWideConfigurationKey.Backup, out Slice valueNameLowered))
            {
                var severWideBackupNames = new HashSet<string>();
                if (items.ReadByKey(valueNameLowered, out var tvr))
                {
                    var ptr = tvr.Read(2, out int size);
                    using var previousValue = new BlittableJsonReaderObject(ptr, size, context);
                    foreach (var backupName in previousValue.GetPropertyNames())
                    {
                        severWideBackupNames.Add(PutServerWideBackupConfigurationCommand.GetTaskName(backupName));
                    }
                }

                return severWideBackupNames;
            }
        }

        private void UpdateDatabasesWithExternalReplication(ClusterOperationContext context, string type, ServerWideExternalReplication serverWideExternalReplication)
        {
            if (serverWideExternalReplication == null)
                throw new RachisInvalidOperationException($"Server-wide external replication is null for command type: {type}");

            if (serverWideExternalReplication.Name == null)
                throw new RachisInvalidOperationException($"`{nameof(ServerWideExternalReplication.Name)}` is null or empty for command type: {type}");

            var topologyDiscoveryUrls = serverWideExternalReplication.TopologyDiscoveryUrls;
            if (topologyDiscoveryUrls == null || topologyDiscoveryUrls.Length == 0)
                throw new RachisInvalidOperationException($"`{nameof(ServerWideExternalReplication.TopologyDiscoveryUrls)}` is null or empty for command type: {type}");

            // the external replication name might have changed
            var serverWideBlittable = context.ReadObject(serverWideExternalReplication.ToJson(), "server-wide-configuration");
            var items = context.Transaction.InnerTransaction.OpenTable(ItemsSchema, Items);

            const string dbKey = "db/";
            var toUpdate = new List<(string Key, BlittableJsonReaderObject DatabaseRecord, string DatabaseName)>();

            using (Slice.From(context.Allocator, dbKey, out var loweredPrefix))
            {
                var allServerWideTaskNames = GetAllSeverWideExternalReplicationNames(context);
                
                foreach (var result in items.SeekByPrimaryKeyPrefix(loweredPrefix, Slices.Empty, 0))
                {
                    var (key, oldDatabaseRecord) = GetCurrentItem(context, result.Value);

                    var externalReplication = JsonDeserializationCluster.ExternalReplication(serverWideBlittable);
                    var databaseName = key.Substring(dbKey.Length);
                    var ravenConnectionString = PutServerWideExternalReplicationCommand.UpdateExternalReplicationTemplateForDatabase(externalReplication, databaseName, topologyDiscoveryUrls);

                    var updatedExternalReplications = new DynamicJsonArray();
                    if (oldDatabaseRecord.TryGet(nameof(DatabaseRecord.ExternalReplications), out BlittableJsonReaderArray databaseExternalReplications))
                    {
                        var isTaskToEditFound = false;

                        foreach (BlittableJsonReaderObject databaseTask in databaseExternalReplications)
                        {
                            //Even though we rebuild the whole configurations list just one should be modified
                            //In addition, the same configuration should be modified in all databases
                            if (isTaskToEditFound || IsServerWideExternalReplicationToEdit(databaseTask, externalReplication.Name, allServerWideTaskNames) == false)
                            {
                                updatedExternalReplications.Add(databaseTask);
                                continue;
                            }

                            isTaskToEditFound = true;

                            if (databaseTask.TryGet(nameof(PeriodicBackupConfiguration.TaskId), out long taskId))
                                externalReplication.TaskId = taskId;
                        }
                    }

                    var hasConnectionStrings = oldDatabaseRecord.TryGet(nameof(DatabaseRecord.RavenConnectionStrings), out BlittableJsonReaderObject ravenConnectionStrings);

                    if (serverWideExternalReplication.IsExcluded(databaseName) == false)
                    {
                        updatedExternalReplications.Add(externalReplication.ToJson());

                        if (hasConnectionStrings)
                        {
                            ravenConnectionStrings.Modifications ??= new DynamicJsonValue();
                            ravenConnectionStrings.Modifications = new DynamicJsonValue
                            {
                                [ravenConnectionString.Name] = ravenConnectionString.ToJson()
                            };

                            ravenConnectionStrings = context.ReadObject(ravenConnectionStrings, nameof(DatabaseRecord.RavenConnectionStrings));
                        }
                        else
                        {
                            var djv = new DynamicJsonValue
                            {
                                [ravenConnectionString.Name] = ravenConnectionString.ToJson()
                            };

                            ravenConnectionStrings = context.ReadObject(djv, nameof(DatabaseRecord.RavenConnectionStrings));
                        }
                    }
                    else if (hasConnectionStrings)
                    {
                        // we need to remove the connection string that we previously created
                        var propertyIndex = ravenConnectionStrings.GetPropertyIndex(ravenConnectionString.Name);
                        if (propertyIndex != -1)
                        {
                            ravenConnectionStrings.Modifications ??= new DynamicJsonValue();
                            ravenConnectionStrings.Modifications.Removals = new HashSet<int>
                            {
                                propertyIndex
                            };

                            ravenConnectionStrings = context.ReadObject(ravenConnectionStrings, nameof(DatabaseRecord.RavenConnectionStrings));
                        }
                    }
                    else
                    {
                        ravenConnectionStrings = context.ReadObject(new DynamicJsonValue(), nameof(DatabaseRecord.RavenConnectionStrings));
                    }

                    using (oldDatabaseRecord)
                    using (ravenConnectionStrings)
                    {
                        oldDatabaseRecord.Modifications = new DynamicJsonValue(oldDatabaseRecord)
                        {
                            [nameof(DatabaseRecord.ExternalReplications)] = updatedExternalReplications,
                            [nameof(DatabaseRecord.RavenConnectionStrings)] = ravenConnectionStrings
                        };

                        var updatedDatabaseRecord = context.ReadObject(oldDatabaseRecord, "updated-database-record");
                        toUpdate.Add((Key: key, DatabaseRecord: updatedDatabaseRecord, DatabaseName: databaseName));
                    }
                }
            }
            var index = serverWideExternalReplication.TaskId;
            ApplyDatabaseRecordUpdates(toUpdate, type, index, index, items, context);
        }

        private static unsafe HashSet<string> GetAllSeverWideExternalReplicationNames(ClusterOperationContext context)
        {
            var items = context.Transaction.InnerTransaction.OpenTable(ItemsSchema, Items);
            using (Slice.From(context.Allocator, ServerWideConfigurationKey.ExternalReplication, out var valueNameLowered))
            {
                var names = new HashSet<string>();
                if (items.ReadByKey(valueNameLowered, out var tvr))
                {
                    var ptr = tvr.Read(2, out var size);
                    using var previousValue = new BlittableJsonReaderObject(ptr, size, context);
                    foreach (var name in previousValue.GetPropertyNames())
                    {
                        names.Add(PutServerWideExternalReplicationCommand.GetTaskName(name));
                    }
                }

                return names;
            }
        }
        
        private void DeleteServerWideBackupConfigurationFromAllDatabases(DeleteServerWideTaskCommand.DeleteConfiguration deleteConfiguration, ClusterOperationContext context, string type, long index)
        {
            if (string.IsNullOrWhiteSpace(deleteConfiguration.TaskName))
                throw new RachisInvalidOperationException($"Task name to delete cannot be null or white space for command type: {type}");

            var items = context.Transaction.InnerTransaction.OpenTable(ItemsSchema, Items);

            const string dbKey = "db/";
            var toUpdate = new List<(string Key, BlittableJsonReaderObject DatabaseRecord, string DatabaseName)>();
            var databaseRecordTaskName = DeleteServerWideTaskCommand.GetDatabaseRecordTaskName(deleteConfiguration);

            using (Slice.From(context.Allocator, dbKey, out var loweredPrefix))
            {
                foreach (var result in items.SeekByPrimaryKeyPrefix(loweredPrefix, Slices.Empty, 0))
                {
                    var (key, oldDatabaseRecord) = GetCurrentItem(context, result.Value);

                    var hasChanges = false;

                    switch (deleteConfiguration.Type)
                    {
                        case OngoingTaskType.Backup:
                            if (oldDatabaseRecord.TryGet(nameof(DatabaseRecord.PeriodicBackups), out BlittableJsonReaderArray backups) == false)
                                continue;

                            var updatedBackups = new DynamicJsonArray();
                            foreach (BlittableJsonReaderObject backup in backups)
                            {
                                if (IsServerWideTaskWithName(backup, nameof(PeriodicBackupConfiguration.Name), databaseRecordTaskName))
                                {
                                    hasChanges = true;
                                    continue;
                                }

                                updatedBackups.Add(backup);
                            }

                            oldDatabaseRecord.Modifications = new DynamicJsonValue(oldDatabaseRecord)
                            {
                                [nameof(DatabaseRecord.PeriodicBackups)] = updatedBackups
                            };

                            break;

                        case OngoingTaskType.Replication:
                            if (oldDatabaseRecord.TryGet(nameof(DatabaseRecord.ExternalReplications), out BlittableJsonReaderArray externalReplications) == false)
                                continue;

                            var updatedExternalReplications = new DynamicJsonArray();
                            foreach (BlittableJsonReaderObject externalReplication in externalReplications)
                            {
                                if (IsServerWideTaskWithName(externalReplication, nameof(PeriodicBackupConfiguration.Name), databaseRecordTaskName))
                                {
                                    hasChanges = true;
                                    continue;
                                }

                                updatedExternalReplications.Add(externalReplication);
                            }

                            if (oldDatabaseRecord.TryGet(nameof(DatabaseRecord.RavenConnectionStrings), out BlittableJsonReaderObject ravenConnectionStrings))
                            {
                                var connectionStringName = PutServerWideExternalReplicationCommand.GetRavenConnectionStringName(deleteConfiguration.TaskName);
                                var propertyIndex = ravenConnectionStrings.GetPropertyIndex(connectionStringName);
                                if (propertyIndex != -1)
                                {
                                    ravenConnectionStrings.Modifications ??= new DynamicJsonValue();
                                    ravenConnectionStrings.Modifications.Removals = new HashSet<int>
                                    {
                                        propertyIndex
                                    };

                                    ravenConnectionStrings = context.ReadObject(ravenConnectionStrings, nameof(DatabaseRecord.RavenConnectionStrings));
                                }
                            }
                            else
                            {
                                ravenConnectionStrings = context.ReadObject(new DynamicJsonValue(), nameof(DatabaseRecord.RavenConnectionStrings));
                            }

                            oldDatabaseRecord.Modifications = new DynamicJsonValue(oldDatabaseRecord)
                            {
                                [nameof(DatabaseRecord.ExternalReplications)] = updatedExternalReplications,
                                [nameof(DatabaseRecord.RavenConnectionStrings)] = ravenConnectionStrings
                            };
                            break;

                        default:
                            throw new ArgumentOutOfRangeException();
                    }

                    if (hasChanges == false)
                        continue;

                    using (oldDatabaseRecord)
                    {
                        var updatedDatabaseRecord = context.ReadObject(oldDatabaseRecord, "updated-database-record");
                        toUpdate.Add((Key: key, DatabaseRecord: updatedDatabaseRecord, DatabaseName: key.Substring(dbKey.Length)));
                    }
                }
            }

            ApplyDatabaseRecordUpdates(toUpdate, type, index, index, items, context);
        }

        private void ToggleServerWideTaskState(BlittableJsonReaderObject cmd, ToggleServerWideTaskStateCommand.Parameters parameters, ClusterOperationContext context, string type)
        {
            if (cmd.TryGet(nameof(ToggleServerWideTaskStateCommand.Name), out string name) == false)
                throw new RachisApplyException($"Failed to get configuration key name for command type '{type}'");

            var configurationsBlittable = Read(context, name);
            if (configurationsBlittable == null)
                throw new RachisApplyException($"Cannot find any server wide tasks of type '{parameters.Type}'");

            if (configurationsBlittable.TryGet(parameters.TaskName, out BlittableJsonReaderObject task) == false)
                throw new RachisApplyException($"Cannot find server wide task of type '{parameters.Type}' with name '{parameters.TaskName}'");

            switch (parameters.Type)
            {
                case OngoingTaskType.Backup:
                    var serverWideBackupConfiguration = JsonDeserializationCluster.ServerWideBackupConfiguration(task);
                    UpdateDatabasesWithServerWideBackupConfiguration(context, type, serverWideBackupConfiguration);
                    break;

                case OngoingTaskType.Replication:
                    var serverWideExternalReplication = JsonDeserializationCluster.ServerWideExternalReplication(task);
                    UpdateDatabasesWithExternalReplication(context, type, serverWideExternalReplication);
                    break;

                default:
                    throw new ArgumentOutOfRangeException();
            }
        }

        private static bool IsServerWideTaskWithName(BlittableJsonReaderObject blittable, string namePropertyName, string taskNameToFind)
        {
            return blittable.TryGet(namePropertyName, out string foundTaskName) &&
                   foundTaskName != null &&
                   taskNameToFind.Equals(foundTaskName, StringComparison.OrdinalIgnoreCase);
        }

        private void ApplyDatabaseRecordUpdates(List<(string Key, BlittableJsonReaderObject DatabaseRecord, string DatabaseName)> toUpdate, string type, long indexForValueChanges, long index, Table items, ClusterOperationContext context)
        {
            var tasks = new List<Func<Task>> { () => Changes.OnValueChanges(indexForValueChanges, type) };

            foreach (var update in toUpdate)
            {
                using (Slice.From(context.Allocator, update.Key, out var valueName))
                using (Slice.From(context.Allocator, update.Key.ToLowerInvariant(), out var valueNameLowered))
                {
                    UpdateValue(index, items, valueNameLowered, valueName, update.DatabaseRecord);
                }

                tasks.Add(() => Changes.OnDatabaseChanges(update.DatabaseName, index, type, DatabasesLandlord.ClusterDatabaseChangeType.RecordChanged, null));
            }

            ExecuteManyOnDispose(context, index, type, tasks);
        }

        public const string SnapshotInstalled = "SnapshotInstalled";

        public override void OnSnapshotInstalled(long lastIncludedIndex, bool fullSnapshot, ServerStore serverStore, CancellationToken token)
        {
            using (_parent.ContextPool.AllocateOperationContext(out ClusterOperationContext context))
            using (context.OpenWriteTransaction())
            {
                // lets read all the certificate keys from the cluster, and delete the matching ones from the local state
                var clusterCertificateKeys = serverStore.Cluster.GetCertificateThumbprintsFromCluster(context);

                foreach (var key in clusterCertificateKeys)
                {
                    using (GetLocalStateByThumbprint(context, key))
                    {
                        DeleteLocalState(context, key);
                    }
                }

                token.ThrowIfCancellationRequested();

                var databases = GetDatabaseNames(context).ToArray();
                context.Transaction.InnerTransaction.LowLevelTransaction.OnDispose += tx =>
                {
                    if (tx is LowLevelTransaction llt && llt.Committed)
                    {
                        // there is potentially a lot of work to be done here so we are responding to the change on a separate task.
                        foreach (var db in databases)
                        {
                            var t = Task.Run(async () =>
                            {
                                await Changes.OnDatabaseChanges(db, lastIncludedIndex, SnapshotInstalled, DatabasesLandlord.ClusterDatabaseChangeType.RecordChanged, null);
                            }, token);
                        }

                        var t2 = Task.Run(async () =>
                        {
                            await Changes.OnValueChanges(lastIncludedIndex, nameof(InstallUpdatedServerCertificateCommand));
                            if (fullSnapshot)
                                await Changes.OnValueChanges(lastIncludedIndex, nameof(PutLicenseCommand));
                        }, token);
                    }
                };

                context.Transaction.Commit();
            }
            token.ThrowIfCancellationRequested();

            _rachisLogIndexNotifications.NotifyListenersAbout(lastIncludedIndex, null);
        }

        protected override RachisVersionValidation InitializeValidator()
        {
            return new ClusterValidator();
        }

        public IEnumerable<BlittableJsonReaderObject> GetReplicationHubCertificateByHub(TransactionOperationContext context, string database, string hub, string filter, int start, int pageSize)
        {
            using var certs = context.Transaction.InnerTransaction.OpenTable(ReplicationCertificatesSchema, ReplicationCertificatesSlice);

            string prefixString = (database + "/" + hub + "/").ToLowerInvariant();
            using var _ = Slice.From(context.Allocator, prefixString, out var prefix);

            foreach (var (key, val) in certs.SeekByPrimaryKeyPrefix(prefix, Slices.Empty, start))
            {
                var blittable = GetReplicationCertificateAccessObject(context, ref val.Reader);
                string thumbprint = key.ToString().Substring(prefixString.Length);

                if (filter != null)
                {
                    blittable.TryGet(nameof(RegisterReplicationHubAccessCommand.Name), out string name);
                    var match =
                        thumbprint.Contains(filter, StringComparison.OrdinalIgnoreCase) ||
                        name?.Contains(filter, StringComparison.OrdinalIgnoreCase) == true;

                    if (match == false)
                        continue;
                }

                if (pageSize-- < 0)
                    break;

                blittable.Modifications = new DynamicJsonValue(blittable)
                {
                    [nameof(DetailedReplicationHubAccess.Certificate)] = GetCertificateAsBase64(val),
                    [nameof(DetailedReplicationHubAccess.Thumbprint)] = thumbprint
                };

                using (blittable)
                {
                    yield return context.ReadObject(blittable, "create replication access blittable");
                }
            }
        }

        public IEnumerable<(string Hub, ReplicationHubAccess Access)> GetReplicationHubCertificateForDatabase(TransactionOperationContext context, string database)
        {
            using var certs = context.Transaction.InnerTransaction.OpenTable(ReplicationCertificatesSchema, ReplicationCertificatesSlice);

            string prefixString = (database + "/").ToLowerInvariant();
            using var _ = Slice.From(context.Allocator, prefixString, out var prefix);

            foreach (var (key, val) in certs.SeekByPrimaryKeyPrefix(prefix, Slices.Empty, 0))
            {
                var blittable = GetReplicationCertificateAccessObject(context, ref val.Reader);

                blittable.TryGet(nameof(RegisterReplicationHubAccessCommand.HubName), out string hub);
                var details = JsonDeserializationCluster.DetailedReplicationHubAccess(blittable);

                string certBase64 = GetCertificateAsBase64(val);

                yield return (hub, new ReplicationHubAccess
                {
                    AllowedHubToSinkPaths = details.AllowedHubToSinkPaths,
                    AllowedSinkToHubPaths = details.AllowedSinkToHubPaths,
                    Name = details.Name,
                    CertificateBase64 = certBase64,
                });
            }
        }

        private unsafe string GetCertificateAsBase64(Table.TableValueHolder val)
        {
            var buffer = val.Reader.Read((int)ReplicationCertificatesTable.Certificate, out var size);
            string certBase64 = Convert.ToBase64String(new ReadOnlySpan<byte>(buffer, size));
            return certBase64;
        }

        private unsafe BlittableJsonReaderObject GetReplicationCertificateAccessObject(TransactionOperationContext context, ref TableValueReader reader)
        {
            return new BlittableJsonReaderObject(reader.Read((int)ReplicationCertificatesTable.Access, out var size), size, context);
        }

        public bool IsReplicationCertificate(TransactionOperationContext context, string database, string hub, X509Certificate2 userCert, out DetailedReplicationHubAccess access)
        {
            using var certs = context.Transaction.InnerTransaction.OpenTable(ReplicationCertificatesSchema, ReplicationCertificatesSlice);

            using var __ = Slice.From(context.Allocator, (database + "/" + hub + "/" + userCert.Thumbprint).ToLowerInvariant(), out var key);

            if (certs.ReadByKey(key, out var v))
            {
                var b = GetReplicationCertificateAccessObject(context, ref v);
                access = JsonDeserializationCluster.DetailedReplicationHubAccess(b);
                return true;
            }

            access = default;

            return false;
        }

        public unsafe bool IsReplicationCertificateByPublicKeyPinningHash(TransactionOperationContext context, string database, string hub, X509Certificate2 userCert,
            out DetailedReplicationHubAccess access)
        {
            using var certs = context.Transaction.InnerTransaction.OpenTable(ReplicationCertificatesSchema, ReplicationCertificatesSlice);
            // maybe we need to check by public key hash?
            string publicKeyPinningHash = userCert.GetPublicKeyPinningHash();

            using var ___ = Slice.From(context.Allocator, (database + "/" + hub + "/" + publicKeyPinningHash).ToLowerInvariant(), out var publicKeyHash);

            access = default;

            foreach (var result in certs.SeekForwardFromPrefix(ReplicationCertificatesSchema.Indexes[ReplicationCertificatesHashSlice], publicKeyHash, publicKeyHash, 0))
            {
                var obj = GetReplicationCertificateAccessObject(context, ref result.Result.Reader);

                // this is a cheap check, not sufficient for real
                if (obj.TryGet(nameof(userCert.Issuer), out string issuer) == false || issuer != userCert.Issuer)
                    continue;

                // now we need to do an actual check

                var p = result.Result.Reader.Read((int)ReplicationCertificatesTable.Certificate, out var size);
                var buffer = new byte[size];
                new Span<byte>(p, size).CopyTo(buffer);
                using var knownCert = new X509Certificate2(buffer);

                var userChain = new X509Chain();
                var knownCertChain = new X509Chain();
                try
                {
                    userChain.Build(userCert);
                }
                catch (Exception)
                {
                    return false;
                }

                try
                {
                    knownCertChain.Build(knownCert);
                }
                catch (Exception)
                {
                    return false;
                }

                if (knownCertChain.ChainElements.Count != userChain.ChainElements.Count)
                {
                    return false;
                }

                for (var i = 0; i < knownCertChain.ChainElements.Count; i++)
                {
                    // We walk the chain and compare the user certificate vs one of the existing certificate with same pinning hash
                    var currentElementPinningHash = userChain.ChainElements[i].Certificate.GetPublicKeyPinningHash();
                    if (currentElementPinningHash != knownCertChain.ChainElements[i].Certificate.GetPublicKeyPinningHash())
                    {
                        return false;
                    }
                }

                access = JsonDeserializationCluster.DetailedReplicationHubAccess(obj);
                return true;
            }

            return false;
        }
    }

    public class RachisLogIndexNotifications : IDisposable
    {
        public long LastModifiedIndex;
        private readonly AsyncManualResetEvent _notifiedListeners;
        private readonly ConcurrentQueue<ErrorHolder> _errors = new ConcurrentQueue<ErrorHolder>();
        private int _numberOfErrors;
        private readonly ConcurrentDictionary<long, TaskCompletionSource<object>> _tasksDictionary = new ConcurrentDictionary<long, TaskCompletionSource<object>>();

        public readonly Queue<RecentLogIndexNotification> RecentNotifications = new Queue<RecentLogIndexNotification>();
        internal Logger Log;

        private class ErrorHolder
        {
            public long Index;
            public ExceptionDispatchInfo Exception;
        }

        public RachisLogIndexNotifications(CancellationToken token)
        {
            _notifiedListeners = new AsyncManualResetEvent(token);
        }

        public void Dispose()
        {
            _notifiedListeners.Dispose();
        }

        public async Task WaitForIndexNotification(long index, CancellationToken token)
        {
            Task<bool> waitAsync;
            while (true)
            {
                // first get the task, then wait on it
                waitAsync = _notifiedListeners.WaitAsync(token);

                if (index <= Interlocked.Read(ref LastModifiedIndex))
                    break;

                if (token.IsCancellationRequested)
                    ThrowCanceledException(index, LastModifiedIndex, isExecution: false);

                if (await waitAsync == false)
                {
                    var copy = Interlocked.Read(ref LastModifiedIndex);
                    if (index <= copy)
                        break;
                }
            }

            if (await WaitForTaskCompletion(index, new Lazy<Task>(waitAsync)))
                return;

            ThrowCanceledException(index, LastModifiedIndex, isExecution: true);
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

            if (await WaitForTaskCompletion(index, new Lazy<Task>(TimeoutManager.WaitFor(timeout))))
                return;

            ThrowTimeoutException(timeout, index, LastModifiedIndex, isExecution: true);
        }

        private async Task<bool> WaitForTaskCompletion(long index, Lazy<Task> waitingTask)
        {
            if (_tasksDictionary.TryGetValue(index, out var tcs) == false)
            {
                // the task has already completed
                // let's check if we had errors in it
                foreach (var error in _errors)
                {
                    if (error.Index == index)
                        error.Exception.Throw(); // rethrow
                }

                return true;
            }

            var task = tcs.Task;

            if (task.IsCompleted)
            {
                if (task.IsFaulted)
                {
                    try
                    {
                        await task; // will throw on error
                    }
                    catch (Exception e)
                    {
                        ThrowApplyException(index, e);
                    }
                }

                if (task.IsCanceled)
                    ThrowCanceledException(index, LastModifiedIndex);

                return true;
            }

            var result = await Task.WhenAny(task, waitingTask.Value);

            if (result.IsFaulted)
                await result; // will throw

            if (result == task)
                return true;
            return false;
        }

        private void ThrowCanceledException(long index, long lastModifiedIndex, bool isExecution = false)
        {
            var openingString = isExecution
                ? $"Cancelled while waiting for task with index {index} to complete. "
                : $"Cancelled while waiting to get an index notification for {index}. ";

            var closingString = isExecution
                ? string.Empty
                : Environment.NewLine +
                  PrintLastNotifications();

            throw new OperationCanceledException(openingString +
                                       $"Last commit index is: {lastModifiedIndex}. " +
                                       $"Number of errors is: {_numberOfErrors}." + closingString);
        }

        private void ThrowTimeoutException(TimeSpan value, long index, long lastModifiedIndex, bool isExecution = false)
        {
            var openingString = isExecution
                ? $"Waited for {value} for task with index {index} to complete. "
                : $"Waited for {value} but didn't get an index notification for {index}. ";

            var closingString = isExecution
                ? string.Empty
                : Environment.NewLine +
                  PrintLastNotifications();

            throw new TimeoutException(openingString +
                                       $"Last commit index is: {lastModifiedIndex}. " +
                                       $"Number of errors is: {_numberOfErrors}." + closingString);
        }

        private void ThrowApplyException(long index, Exception e)
        {
            throw new InvalidOperationException($"Index {index} was successfully committed, but the apply failed.", e);
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

        public void SetTaskCompleted(long index, Exception e)
        {
            if (_tasksDictionary.TryGetValue(index, out var tcs))
            {
                // set the task as finished
                if (e == null)
                {
                    if (tcs.TrySetResult(null) == false)
                        LogFailureToSetTaskResult();
                }
                else
                {
                    if (tcs.TrySetException(e) == false)
                        LogFailureToSetTaskResult();
                }

                void LogFailureToSetTaskResult()
                {
                    if (Log.IsInfoEnabled)
                        Log.Info($"Failed to set result of task with index {index}");
                }
            }

            _tasksDictionary.TryRemove(index, out _);
        }

        private static void InterlockedExchangeMax(ref long location, long newValue)
        {
            long initialValue;

            do
            {
                initialValue = location;
                if (initialValue >= newValue)
                    return;
            } while (Interlocked.CompareExchange(ref location, newValue, initialValue) != initialValue);
        }

        public void AddTask(long index)
        {
            Debug.Assert(_tasksDictionary.TryGetValue(index, out _) == false, $"{nameof(_tasksDictionary)} should not contain task with key {index}");

            _tasksDictionary.TryAdd(index, new TaskCompletionSource<object>(TaskCreationOptions.RunContinuationsAsynchronously));
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

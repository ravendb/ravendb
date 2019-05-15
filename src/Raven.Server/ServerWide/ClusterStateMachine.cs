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
using Raven.Client;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.Documents.Operations.Configuration;
using Raven.Client.Documents.Session;
using Raven.Client.Exceptions.Database;
using Raven.Client.Exceptions.Documents.Subscriptions;
using Raven.Client.Exceptions.Security;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Commands;
using Raven.Client.ServerWide.Operations.Certificates;
using Raven.Client.ServerWide.Operations.Configuration;
using Raven.Client.ServerWide.Tcp;
using Raven.Client.Util;
using Raven.Server.Commercial;
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
using Raven.Server.ServerWide.Commands.Subscriptions;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow;
using Sparrow.Binary;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Logging;
using Sparrow.Utils;
using Voron;
using Voron.Data;
using Voron.Data.BTrees;
using Voron.Data.Tables;
using Voron.Impl;

namespace Raven.Server.ServerWide
{
    public class ClusterStateMachine : RachisStateMachine
    {
        private const string LocalNodeStateTreeName = "LocalNodeState";
        private static readonly StringSegment DatabaseName = new StringSegment("DatabaseName");

        private static readonly TableSchema ItemsSchema;
        private static readonly TableSchema CompareExchangeSchema;
        public static readonly TableSchema TransactionCommandsSchema;

        public enum UniqueItems
        {
            Key,
            Index,
            Value
        }

        private static readonly Slice Items;
        private static readonly Slice CompareExchange;
        public static readonly Slice Identities;
        public static readonly Slice TransactionCommands;
        public static readonly Slice TransactionCommandsCountPerDatabase;

        static ClusterStateMachine()
        {
            using (StorageEnvironment.GetStaticContext(out var ctx))
            {
                Slice.From(ctx, "Items", out Items);
                Slice.From(ctx, "CmpXchg", out CompareExchange);
                Slice.From(ctx, "Identities", out Identities);
                Slice.From(ctx, "TransactionCommands", out TransactionCommands);
                Slice.From(ctx, "TransactionCommandsIndex", out TransactionCommandsCountPerDatabase);
            }
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

            TransactionCommandsSchema = new TableSchema();
            TransactionCommandsSchema.DefineKey(new TableSchema.SchemaIndexDef()
            {
                StartIndex = 0,
                Count = 1, // Database, Separator, Commands count
            });
        }

        public event EventHandler<(string DatabaseName, long Index, string Type, DatabasesLandlord.ClusterDatabaseChangeType ChangeType)> DatabaseChanged;

        public event EventHandler<(long Index, string Type)> ValueChanged;

        private readonly RachisLogIndexNotifications _rachisLogIndexNotifications = new RachisLogIndexNotifications(CancellationToken.None);

        protected override void Apply(TransactionOperationContext context, BlittableJsonReaderObject cmd, long index, Leader leader, ServerStore serverStore)
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
                    case nameof(AddOrUpdateCompareExchangeBatchCommand):
                        ExecuteCompareExchangeBatch(context, cmd, index, type);
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
                            throw new RachisApplyException(errorMessage);

                        SetValueForTypedDatabaseCommand(context, type, cmd, index, leader, out result);
                        leader?.SetStateOf(index, result);
                        UpdateDatabaseRecordEtagForBackup(context, type, cmd, index);
                        break;
                    case nameof(IncrementClusterIdentitiesBatchCommand):
                        if (ValidatePropertyExistence(cmd, nameof(IncrementClusterIdentitiesBatchCommand), nameof(IncrementClusterIdentitiesBatchCommand.DatabaseName), out errorMessage) == false)
                            throw new RachisApplyException(errorMessage);

                        SetValueForTypedDatabaseCommand(context, type, cmd, index, leader, out result);
                        leader?.SetStateOf(index, result);
                        UpdateDatabaseRecordEtagForBackup(context, type, cmd, index);
                        break;
                    case nameof(UpdateClusterIdentityCommand):
                        if (ValidatePropertyExistence(cmd, nameof(UpdateClusterIdentityCommand), nameof(UpdateClusterIdentityCommand.Identities), out errorMessage) == false)
                            throw new RachisApplyException(errorMessage);

                        SetValueForTypedDatabaseCommand(context, type, cmd, index, leader, out result);
                        leader?.SetStateOf(index, result);
                        UpdateDatabaseRecordEtagForBackup(context, type, cmd, index);
                        break;
                    case nameof(PutIndexCommand):
                    case nameof(PutAutoIndexCommand):
                    case nameof(DeleteIndexCommand):
                    case nameof(SetIndexLockCommand):
                    case nameof(SetIndexPriorityCommand):
                    case nameof(SetIndexStateCommand):
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
                        UpdateDatabaseRecordEtagForBackup(context, type, cmd, index);
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
                    case nameof(PutServerWideStudioConfigurationCommand):
                        PutValue<ServerWideStudioConfiguration>(context, type, cmd, index, leader);
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
            }
            catch (Exception e) when (ExpectedException(e))
            {
                if (_parent.Log.IsInfoEnabled)
                    _parent.Log.Info($"Failed to execute command of type '{type}' on database '{DatabaseName}'", e);

                _parent.UpdateHistoryLog(context, index, _parent.CurrentTerm, cmd, null, e);
                NotifyLeaderAboutError(index, leader, e);
            }
            catch (Exception e)
            {
                // IMPORTANT
                // Other exceptions MUST be consistent across the cluster (meaning: if it occured on one node it must occur on the rest also).
                // the exceptions here are machine specific and will cause a jam in the state machine until the exception will be resolved.
                if (_parent.Log.IsInfoEnabled)
                    _parent.Log.Info($"Unrecoverable exception on database '{DatabaseName}' at command type '{type}', execution will be retried later.", e);

                NotifyLeaderAboutError(index, leader, e);
                throw;
            }
            finally
            {
                var executionTime = sw.Elapsed;

                _parent.UpdateHistoryLog(context, index, _parent.CurrentTerm, cmd, result, null);
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

        [Conditional("DEBUG")]
        private static void ValidateGuid(BlittableJsonReaderObject cmd, string type)
        {
            if (cmd.TryGet(nameof(CommandBase.Guid), out string guid) == false)
            {
                throw new ArgumentNullException($"Guid is not provided in the command {type}.");
            }
        }

        private void ExecuteCompareExchangeBatch(TransactionOperationContext context, BlittableJsonReaderObject cmd, long index, string commandType)
        {
            CompareExchangeCommandBase compareExchange = null;
            Exception exception = null;
            try
            {
                if (cmd.TryGet(nameof(AddOrUpdateCompareExchangeBatchCommand.Commands), out BlittableJsonReaderArray commands) == false)
                {
                    throw new RachisApplyException($"'{nameof(AddOrUpdateCompareExchangeBatchCommand.Commands)}' is missing in '{commandType}'.");
                }

                var items = context.Transaction.InnerTransaction.OpenTable(CompareExchangeSchema, CompareExchange);
                foreach (BlittableJsonReaderObject command in commands)
                {
                    if (command.TryGet("Type", out string type) == false || type != nameof(AddOrUpdateCompareExchangeCommand))
                    {
                        throw new RachisApplyException($"Cannot execute {commandType} command, wrong format");
                    }

                    compareExchange = (CompareExchangeCommandBase)JsonDeserializationCluster.Commands[type](command);
                    compareExchange.Execute(context, items, index);
                    UpdateDatabaseRecordEtagForBackup(context, type, command, index);
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
                   e is AuthorizationException;
        }

        private void ClusterStateCleanUp(TransactionOperationContext context, BlittableJsonReaderObject cmd, long index)
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
                    var database = tuple.Key;
                    var commandsCount = tuple.Value;
                    var record = ReadDatabase(context, database);
                    if (record == null)
                        continue;

                    record.TruncatedClusterTransactionCommandsCount = commandsCount;

                    var dbKey = "db/" + tuple.Key;
                    using (Slice.From(context.Allocator, dbKey, out Slice valueName))
                    using (Slice.From(context.Allocator, dbKey.ToLowerInvariant(), out Slice valueNameLowered))
                    {
                        var updatedDatabaseBlittable = EntityToBlittable.ConvertCommandToBlittable(record, context);
                        UpdateValue(index, items, valueNameLowered, valueName, updatedDatabaseBlittable);
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

        private List<string> ExecuteClusterTransaction(TransactionOperationContext context, BlittableJsonReaderObject cmd, long index)
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

                    NotifyDatabaseAboutChanged(context, clusterTransaction.DatabaseName, index, nameof(ClusterTransactionCommand), notify);

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
                LogCommand(nameof(ClusterTransactionCommand), index,exception, clusterTransaction?.AdditionalDebugInformation(exception));
            }
        }

        private void UpdateDatabaseRecordId(TransactionOperationContext context, long index, ClusterTransactionCommand clusterTransaction)
        {
            var record = ReadDatabase(context, clusterTransaction.DatabaseName);
            if (record.Topology.DatabaseTopologyIdBase64 == null)
            {
                var items = context.Transaction.InnerTransaction.OpenTable(ItemsSchema, Items);
                record.Topology.DatabaseTopologyIdBase64 = clusterTransaction.DatabaseRecordId;
                var dbKey = "db/" + clusterTransaction.DatabaseName;
                using (Slice.From(context.Allocator, dbKey, out var valueName))
                using (Slice.From(context.Allocator, dbKey.ToLowerInvariant(), out var valueNameLowered))
                {
                    var updatedDatabaseBlittable = EntityToBlittable.ConvertCommandToBlittable(record, context);
                    UpdateValue(index, items, valueNameLowered, valueName, updatedDatabaseBlittable);
                }
            }
        }

        private void ConfirmReceiptServerCertificate(TransactionOperationContext context, BlittableJsonReaderObject cmd, long index, ServerStore serverStore)
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

        private void InstallUpdatedServerCertificate(TransactionOperationContext context, BlittableJsonReaderObject cmd, long index, ServerStore serverStore)
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

        private void ConfirmServerCertificateReplaced(TransactionOperationContext context, BlittableJsonReaderObject cmd, long index, ServerStore serverStore)
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

        private void RemoveNodeFromCluster(TransactionOperationContext context, BlittableJsonReaderObject cmd, long index, Leader leader)
        {
            RemoveNodeFromClusterCommand removedCmd = null;
            Exception exception = null;
            try
            {
                removedCmd = JsonDeserializationCluster.RemoveNodeFromClusterCommand(cmd);
                var removed = removedCmd.RemovedNode;
                var items = context.Transaction.InnerTransaction.OpenTable(ItemsSchema, Items);

                var actions = new List<Action>();

                foreach (var record in ReadAllDatabases(context))
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
                                DeleteDatabaseRecord(context, index, items, lowerKey, record.DatabaseName);
                                actions.Add(() => DatabaseChanged?.Invoke(this,
                                    (record.DatabaseName, index, nameof(RemoveNodeFromCluster), DatabasesLandlord.ClusterDatabaseChangeType.RecordChanged)));
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
                                DeleteDatabaseRecord(context, index, items, lowerKey, record.DatabaseName);
                                continue;
                            }
                        }

                        var updated = EntityToBlittable.ConvertCommandToBlittable(record, context);

                        UpdateValue(index, items, lowerKey, key, updated);
                    }

                    actions.Add(() => DatabaseChanged?.Invoke(this,
                        (record.DatabaseName, index, nameof(RemoveNodeFromCluster), DatabasesLandlord.ClusterDatabaseChangeType.RecordChanged)));
                }

                ExecuteManyOnDispose(context, index, nameof(RemoveNodeFromCluster), actions);
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

        private void ExecuteManyOnDispose(TransactionOperationContext context, long index, string type, List<Action> actions)
        {
            context.Transaction.InnerTransaction.LowLevelTransaction.OnDispose += transaction =>
            {
                if (transaction is LowLevelTransaction llt && llt.Committed)
                {
                    _rachisLogIndexNotifications.AddTask(index);
                    var count = actions.Count;

                    if (count == 0)
                    {
                        NotifyAndSetCompleted(index);
                        return;
                    }

                    var exceptionAggregator =
                        new ExceptionAggregator(_parent.Log, $"the raft index {index} is committed, but an error occured during executing the {type} command.");
                    foreach (var action in actions)
                    {
                        TaskExecutor.Execute(_ =>
                        {
                            exceptionAggregator.Execute(action);
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
                        }, null);
                    }
                }
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

        private void SetValueForTypedDatabaseCommand(TransactionOperationContext context, string type, BlittableJsonReaderObject cmd, long index, Leader leader, out object result)
        {
            result = null;
            UpdateValueForDatabaseCommand updateCommand = null;
            Exception exception = null;
            try
            {
                var items = context.Transaction.InnerTransaction.OpenTable(ItemsSchema, Items);

                updateCommand = (UpdateValueForDatabaseCommand)JsonDeserializationCluster.Commands[type](cmd);

                var record = ReadDatabase(context, updateCommand.DatabaseName);
                if (record == null)
                    throw new DatabaseDoesNotExistException(
                        $"Cannot set typed value of type {type} for database {updateCommand.DatabaseName}, because it does not exist");

                updateCommand.Execute(context, items, index, record, _parent.CurrentState, out result);
            }
            catch(Exception e)
            {
                exception = e;
                throw;
            }
            finally 
            {
                LogCommand(type, index, exception, updateCommand?.AdditionalDebugInformation(exception));
                NotifyDatabaseAboutChanged(context, updateCommand?.DatabaseName, index, type, DatabasesLandlord.ClusterDatabaseChangeType.ValueChanged);
            }
        }

        public async Task WaitForIndexNotification(long index, TimeSpan? timeout = null)
        {
            await _rachisLogIndexNotifications.WaitForIndexNotification(index, timeout ?? _parent.OperationTimeout);
        }

        private unsafe void RemoveNodeFromDatabase(TransactionOperationContext context, BlittableJsonReaderObject cmd, long index, Leader leader)
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
                    var databaseRecord = JsonDeserializationCluster.DatabaseRecord(doc);

                    if (doc.TryGet(nameof(DatabaseRecord.Topology), out BlittableJsonReaderObject _) == false)
                    {
                        items.DeleteByKey(lowerKey);
                        NotifyDatabaseAboutChanged(context, databaseName, index, nameof(RemoveNodeFromDatabaseCommand),
                            DatabasesLandlord.ClusterDatabaseChangeType.RecordChanged);
                        return;
                    }

                    remove.UpdateDatabaseRecord(databaseRecord, index);

                    if (databaseRecord.DeletionInProgress.Count == 0 && databaseRecord.Topology.Count == 0)
                    {
                        DeleteDatabaseRecord(context, index, items, lowerKey, databaseName);
                        NotifyDatabaseAboutChanged(context, databaseName, index, nameof(RemoveNodeFromDatabaseCommand),
                            DatabasesLandlord.ClusterDatabaseChangeType.RecordChanged);
                        return;
                    }

                    var updated = EntityToBlittable.ConvertCommandToBlittable(databaseRecord, context);

                    UpdateValue(index, items, lowerKey, key, updated);
                }

                NotifyDatabaseAboutChanged(context, databaseName, index, nameof(RemoveNodeFromDatabaseCommand),
                    DatabasesLandlord.ClusterDatabaseChangeType.RecordChanged);
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

        private void DeleteDatabaseRecord(TransactionOperationContext context, long index, Table items, Slice lowerKey, string databaseName)
        {
            // delete database record
            items.DeleteByKey(lowerKey);

            // delete all values linked to database record - for subscription, etl etc.
            CleanupDatabaseRelatedValues(context, items, databaseName);

            var transactionsCommands = context.Transaction.InnerTransaction.OpenTable(TransactionCommandsSchema, TransactionCommands);
            var commandsCountPerDatabase = context.Transaction.InnerTransaction.ReadTree(TransactionCommandsCountPerDatabase);

            using (ClusterTransactionCommand.GetPrefix(context, databaseName, out var prefixSlice))
            {
                commandsCountPerDatabase.Delete(prefixSlice);
                transactionsCommands.DeleteByPrimaryKeyPrefix(prefixSlice);
            }
        }

        private void CleanupDatabaseRelatedValues(TransactionOperationContext context, Table items, string databaseName)
        {
            var dbValuesPrefix = Helpers.ClusterStateMachineValuesPrefix(databaseName).ToLowerInvariant();
            using (Slice.From(context.Allocator, dbValuesPrefix, out var loweredKey))
            {
                items.DeleteByPrimaryKeyPrefix(loweredKey);
            }

            DeleteTreeByPrefix(context, UpdateValueForDatabaseCommand.GetStorageKey(databaseName, null), Identities);

            var databaseLowered = databaseName.ToLowerInvariant();
            using (Slice.From(context.Allocator, databaseLowered + "/", out var databaseSlice))
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

        private unsafe List<string> AddDatabase(TransactionOperationContext context, BlittableJsonReaderObject cmd, long index, Leader leader)
        {
            var addDatabaseCommand = JsonDeserializationCluster.AddDatabaseCommand(cmd);
            Exception exception = null;
            try
            {
                Debug.Assert(addDatabaseCommand.Record.Topology.Count != 0, "Attempt to add database with no nodes");
                var items = context.Transaction.InnerTransaction.OpenTable(ItemsSchema, Items);
                using (Slice.From(context.Allocator, "db/" + addDatabaseCommand.Name, out Slice valueName))
                using (Slice.From(context.Allocator, "db/" + addDatabaseCommand.Name.ToLowerInvariant(), out Slice valueNameLowered))
                using (var databaseRecordAsJson = EntityToBlittable.ConvertCommandToBlittable(addDatabaseCommand.Record, context))
                {
                    if (addDatabaseCommand.RaftCommandIndex != null)
                    {
                        if (items.ReadByKey(valueNameLowered, out TableValueReader reader) == false && addDatabaseCommand.RaftCommandIndex != 0)
                            throw new RachisConcurrencyException("Concurrency violation, the database " + addDatabaseCommand.Name +
                                                                 " does not exists, but had a non zero etag");

                        var actualEtag = Bits.SwapBytes(*(long*)reader.Read(3, out int size));
                        Debug.Assert(size == sizeof(long));

                        if (actualEtag != addDatabaseCommand.RaftCommandIndex.Value)
                            throw new RachisConcurrencyException("Concurrency violation, the database " + addDatabaseCommand.Name + " has etag " + actualEtag +
                                                                 " but was expecting " + addDatabaseCommand.RaftCommandIndex);
                    }

                    UpdateValue(index, items, valueNameLowered, valueName, databaseRecordAsJson);
                    SetDatabaseValues(addDatabaseCommand.DatabaseValues, addDatabaseCommand.Name, context, index, items);
                    return addDatabaseCommand.Record.Topology.Members;
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
                    DatabasesLandlord.ClusterDatabaseChangeType.RecordChanged);
            }
        }

        private static void SetDatabaseValues(
            Dictionary<string, BlittableJsonReaderObject> databaseValues,
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
                using (var value = keyValue.Value.Clone(context))
                {
                    UpdateValue(index, items, databaseValueNameLowered, databaseValueName, value);
                }
            }
        }

        private void DeleteValue(TransactionOperationContext context, string type, BlittableJsonReaderObject cmd, long index, Leader leader)
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

        private unsafe void UpdateValue<T>(TransactionOperationContext context, string type, BlittableJsonReaderObject cmd, long index, Leader leader)
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

                    var newValue = command.GetUpdatedValue(context, previousValue);
                    if (newValue == null)
                        return;

                    UpdateValue(index, items, valueNameLowered, valueName, newValue);
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
                NotifyValueChanged(context, type, index);
            }
        }

        private T PutValue<T>(TransactionOperationContext context, string type, BlittableJsonReaderObject cmd, long index, Leader leader)
        {
            Exception exception = null;
            PutValueCommand<T> command = null;
            try
            {
                var items = context.Transaction.InnerTransaction.OpenTable(ItemsSchema, Items);
                command = (PutValueCommand<T>)CommandBase.CreateFrom(cmd);
                if (command.Name.StartsWith(Constants.Documents.Prefix))
                    throw new RachisApplyException("Cannot set " + command.Name + " using PutValueCommand, only via dedicated database calls");

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
                LogCommand(type, index,exception, command.AdditionalDebugInformation(exception));
                NotifyValueChanged(context, type, index);
            }
        }

        public override void EnsureNodeRemovalOnDeletion(TransactionOperationContext context, long term, string nodeTag)
        {
            var djv = new RemoveNodeFromClusterCommand
            {
                RemovedNode = nodeTag
            }.ToJson(context);

            _parent.InsertToLeaderLog(context, term, context.ReadObject(djv, "remove"), RachisEntryFlags.StateMachineCommand);
        }

        private void NotifyValueChanged(TransactionOperationContext context, string type, long index)
        {
            context.Transaction.InnerTransaction.LowLevelTransaction.OnDispose += transaction =>
            {
                if (transaction is LowLevelTransaction llt && llt.Committed)
                {
                    ExecuteAsyncTask(index, () => ValueChanged?.Invoke(this, (index, type)));
                }
            };
        }

        private void NotifyDatabaseAboutChanged(TransactionOperationContext context, string databaseName, long index, string type, DatabasesLandlord.ClusterDatabaseChangeType change)
        {
            context.Transaction.InnerTransaction.LowLevelTransaction.OnDispose += transaction =>
            {
                if (transaction is LowLevelTransaction llt && llt.Committed)
                {
                    ExecuteAsyncTask(index, () => DatabaseChanged?.Invoke(this, (databaseName, index, type, change)));
                }
            };
        }

        private void ExecuteAsyncTask(long index, Action action)
        {
            // we do this under the write tx lock before we update the last applied index
            _rachisLogIndexNotifications.AddTask(index);

            TaskExecutor.Execute(_ =>
            {
                Exception error = null;

                try
                {
                    action();
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
            }, null);
        }

        private void UpdateDatabase(TransactionOperationContext context, string type, BlittableJsonReaderObject cmd, long index, Leader leader, ServerStore serverStore)
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

                    var databaseRecord = JsonDeserializationCluster.DatabaseRecord(databaseRecordJson);

                    if (updateCommand.RaftCommandIndex != null && etag != updateCommand.RaftCommandIndex.Value)
                        throw new RachisConcurrencyException(
                            $"Concurrency violation at executing {type} command, the database {databaseRecord.DatabaseName} has etag {etag} but was expecting {updateCommand.RaftCommandIndex}");

                    updateCommand.Initialize(serverStore, context);
                    string relatedRecordIdToDelete;

                    try
                    {
                        relatedRecordIdToDelete = updateCommand.UpdateDatabaseRecord(databaseRecord, index);
                    }
                    catch (Exception e)
                    {
                        // We are not using the transaction, so any exception here doesn't involve any kind of corruption
                        // and is consistent across the cluster.
                        throw new RachisApplyException("Failed to update database record.", e);
                    }

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

                    UpdateEtagForBackup(databaseRecord, type, index);
                    var updatedDatabaseBlittable = EntityToBlittable.ConvertCommandToBlittable(databaseRecord, context);
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
                NotifyDatabaseAboutChanged(context, databaseName, index, type, DatabasesLandlord.ClusterDatabaseChangeType.RecordChanged);
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

        private void UpdateDatabaseRecordEtagForBackup(TransactionOperationContext context, string type, BlittableJsonReaderObject cmd, long index)
        {
            Exception exception = null;
            try
            {
                string databaseName;
                switch (type)
                {
                    case nameof(AddOrUpdateCompareExchangeCommand):
                    case nameof(RemoveCompareExchangeCommand):
                        if (cmd.TryGet(new StringSegment("Database"), out databaseName) == false || string.IsNullOrEmpty(databaseName))
                            throw new RachisApplyException("Update database command must contain a DatabaseName property");
                        break;
                    case nameof(IncrementClusterIdentityCommand):
                    case nameof(IncrementClusterIdentitiesBatchCommand):
                    case nameof(UpdateClusterIdentityCommand):
                        if (cmd.TryGet(DatabaseName, out databaseName) == false || string.IsNullOrEmpty(databaseName))
                            throw new RachisApplyException("Update database command must contain a DatabaseName property");
                        break;
                    default:
                        throw new RachisApplyException($"Not supported command type {type}");
                }

                var dbKey = "db/" + databaseName;
                var items = context.Transaction.InnerTransaction.OpenTable(ItemsSchema, Items);
                using (Slice.From(context.Allocator, dbKey, out Slice valueName))
                using (Slice.From(context.Allocator, dbKey.ToLowerInvariant(), out Slice valueNameLowered))
                {
                    var databaseRecordJson = ReadInternal(context, out long etag, valueNameLowered);

                    var databaseRecord = JsonDeserializationCluster.DatabaseRecord(databaseRecordJson);

                    UpdateEtagForBackup(databaseRecord, type, index);
                    var updatedDatabaseBlittable = EntityToBlittable.ConvertCommandToBlittable(databaseRecord, context);
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
                LogCommand(type, index, exception);
            }
        }


        private void UpdateEtagForBackup(DatabaseRecord databaseRecord, string type, long index)
        {
            switch (type)
            {
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
                case nameof(EditExpirationCommand):
                case nameof(AddOrUpdateCompareExchangeCommand):
                case nameof(RemoveCompareExchangeCommand):
                case nameof(AddOrUpdateCompareExchangeBatchCommand):
                case nameof(IncrementClusterIdentityCommand):
                case nameof(IncrementClusterIdentitiesBatchCommand):
                case nameof(UpdateClusterIdentityCommand):
                    databaseRecord.EtagForBackup = index;
                    break;
            }

        }

        public override bool ShouldSnapshot(Slice slice, RootObjectType type)
        {
            var baseVersion = slice.Content.Match(Items.Content)
                            || slice.Content.Match(CompareExchange.Content)
                            || slice.Content.Match(Identities.Content);

            if (ClusterCommandsVersionManager.CurrentClusterMinimalVersion >= ClusterCommandsVersionManager.Base41CommandsVersion)
                return baseVersion
                       || slice.Content.Match(TransactionCommands.Content)
                       || slice.Content.Match(TransactionCommandsCountPerDatabase.Content);

            return baseVersion;
        }

        public override void Initialize(RachisConsensus parent, TransactionOperationContext context)
        {
            base.Initialize(parent, context);
            _rachisLogIndexNotifications.Log = _parent.Log;

            ItemsSchema.Create(context.Transaction.InnerTransaction, Items, 32);
            CompareExchangeSchema.Create(context.Transaction.InnerTransaction, CompareExchange, 32);
            TransactionCommandsSchema.Create(context.Transaction.InnerTransaction, TransactionCommands, 32);
            context.Transaction.InnerTransaction.CreateTree(TransactionCommandsCountPerDatabase);
            context.Transaction.InnerTransaction.CreateTree(LocalNodeStateTreeName);
            context.Transaction.InnerTransaction.CreateTree(Identities);
            parent.StateChanged += OnStateChange;
        }

        private void OnStateChange(object sender, RachisConsensus.StateTransition transition)
        {
            if (transition.From == RachisState.Passive && transition.To == RachisState.LeaderElect)
            {
                // moving from 'passive'->'leader elect', means that we were bootstrapped!  
                using (_parent.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
                {
                    var toDelete = new List<string>();
                    var toShrink = new List<DatabaseRecord>();
                    using (context.OpenReadTransaction())
                    {
                        foreach (var record in ReadAllDatabases(context))
                        {
                            if (record.Topology.RelevantFor(_parent.Tag) == false)
                            {
                                toDelete.Add(record.DatabaseName);
                            }
                            else
                            {
                                record.Topology = new DatabaseTopology();
                                record.Topology.Members.Add(_parent.Tag);
                                toShrink.Add(record);
                            }
                        }
                    }

                    if (toShrink.Count == 0 && toDelete.Count == 0)
                        return;

                    using (context.OpenWriteTransaction())
                    {
                        var items = context.Transaction.InnerTransaction.OpenTable(ItemsSchema, Items);
                        var index = _parent.GetLastCommitIndex(context);

                        foreach (var databaseName in toDelete)
                        {
                            var dbKey = "db/" + databaseName;
                            using (Slice.From(context.Allocator, dbKey.ToLowerInvariant(), out Slice valueNameLowered))
                            {
                                DeleteDatabaseRecord(context, index, items, valueNameLowered, databaseName);
                            }
                        }
                        foreach (var record in toShrink)
                        {
                            var dbKey = "db/" + record.DatabaseName;
                            using (Slice.From(context.Allocator, dbKey, out Slice valueName))
                            using (Slice.From(context.Allocator, dbKey.ToLowerInvariant(), out Slice valueNameLowered))
                            {
                                var updatedDatabaseBlittable = EntityToBlittable.ConvertCommandToBlittable(record, context);
                                UpdateValue(index, items, valueNameLowered, valueName, updatedDatabaseBlittable);
                            }
                        }
                        context.Transaction.Commit();
                    }
                }
            }
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
            BlittableJsonReaderObject localStateBlittable = new BlittableJsonReaderObject(read.Reader.Base, read.Reader.Length, context);

            Transaction.DebugDisposeReaderAfterTransaction(context.Transaction.InnerTransaction, localStateBlittable);
            return localStateBlittable;
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

        private void OnTransactionDispose(TransactionOperationContext context, long index)
        {
            context.Transaction.InnerTransaction.LowLevelTransaction.OnDispose += transaction =>
            {
                if (transaction is LowLevelTransaction llt && llt.Committed)
                {
                    _rachisLogIndexNotifications.AddTask(index);
                    NotifyAndSetCompleted(index);
                }
            };
        }

        public (long Index, BlittableJsonReaderObject Value) GetCompareExchangeValue(TransactionOperationContext context, string key)
        {
            var items = context.Transaction.InnerTransaction.OpenTable(CompareExchangeSchema, CompareExchange);
            using (Slice.From(context.Allocator, key, out Slice keySlice))
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

        public IEnumerable<(string Key, long Index)> GetCompareExchangeIndexes(TransactionOperationContext context, string databaseName, string[] keys)
        {
            var items = context.Transaction.InnerTransaction.OpenTable(CompareExchangeSchema, CompareExchange);
            var prefix = databaseName + "/";
            foreach (var key in keys)
            {
                var dbKey = (prefix + key).ToLowerInvariant();
                using (Slice.From(context.Allocator, dbKey, out Slice keySlice))
                {
                    if (items.ReadByKey(keySlice, out var reader))
                    {
                        yield return (key, ReadCompareExchangeIndex(reader));
                    }
                    else
                    {
                        yield return (key, -1);
                    }
                }
            }
        }

        public IEnumerable<(string Key, long Index, BlittableJsonReaderObject Value)> GetCompareExchangeValuesStartsWith(TransactionOperationContext context,
            string dbName, string prefix, int start = 0, int pageSize = 1024)
        {
            var items = context.Transaction.InnerTransaction.OpenTable(CompareExchangeSchema, CompareExchange);
            using (Slice.From(context.Allocator, prefix, out Slice keySlice))
            {
                foreach (var item in items.SeekByPrimaryKeyPrefix(keySlice, Slices.Empty, start))
                {
                    pageSize--;
                    var key = ReadCompareExchangeKey(item.Value.Reader, dbName);
                    var index = ReadCompareExchangeIndex(item.Value.Reader);
                    var value = ReadCompareExchangeValue(context, item.Value.Reader);
                    yield return (key, index, value);

                    if (pageSize == 0)
                        yield break;
                }
            }
        }

        private static unsafe string ReadCompareExchangeKey(TableValueReader reader, string dbPrefix)
        {
            var ptr = reader.Read((int)UniqueItems.Key, out var size);
            // we need to read only the key from the format: 'databaseName/key'
            return Encodings.Utf8.GetString(ptr, size).Substring(dbPrefix.Length + 1);
        }

        private static unsafe BlittableJsonReaderObject ReadCompareExchangeValue(TransactionOperationContext context, TableValueReader reader)
        {
            BlittableJsonReaderObject compareExchangeValue = new BlittableJsonReaderObject(reader.Read((int)UniqueItems.Value, out var size), size, context);
            Transaction.DebugDisposeReaderAfterTransaction(context.Transaction.InnerTransaction, compareExchangeValue);
            return compareExchangeValue;
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

        private static unsafe (string, BlittableJsonReaderObject) GetCurrentItem(TransactionOperationContext context, Table.TableValueHolder result)
        {
            var ptr = result.Reader.Read(2, out int size);
            var doc = new BlittableJsonReaderObject(ptr, size, context);            
            var key = Encoding.UTF8.GetString(result.Reader.Read(1, out size), size);

            Transaction.DebugDisposeReaderAfterTransaction(context.Transaction.InnerTransaction, doc);
            return (key, doc);
        }

        public DatabaseRecord ReadDatabase(TransactionOperationContext context, string name)
        {
            return ReadDatabase(context, name, out long _);
        }

        public DatabaseRecord ReadDatabase<T>(TransactionOperationContext<T> context, string name, out long etag)
            where T : RavenTransaction
        {
            var doc = ReadRawDatabase(context, name, out etag);
            if (doc == null)
                return null;

            return JsonDeserializationCluster.DatabaseRecord(doc);
        }

        public BlittableJsonReaderObject ReadRawDatabase<T>(TransactionOperationContext<T> context, string name, out long etag)
            where T : RavenTransaction
        {
            return Read(context, "db/" + name.ToLowerInvariant(), out etag);
        }

        public DatabaseTopology ReadDatabaseTopology(BlittableJsonReaderObject rawDatabaseRecord)
        {
            rawDatabaseRecord.TryGet(nameof(DatabaseRecord.Topology), out BlittableJsonReaderObject topology);
            return JsonDeserializationCluster.DatabaseTopology(topology);
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

        public long GetNumberOfIdentities(TransactionOperationContext context, string databaseName)
        {
            var identities = context.Transaction.InnerTransaction.ReadTree(Identities);
            var prefix = UpdateValueForDatabaseCommand.GetStorageKey(databaseName, null);

            return GetNumberOf(identities, prefix, context);
        }

        public long GetNumberOfCompareExchange(TransactionOperationContext context, string databaseName)
        {
            var items = context.Transaction.InnerTransaction.OpenTable(CompareExchangeSchema, CompareExchange);
            var compareExchange = items.GetTree(CompareExchangeSchema.Key);
            var prefix = CompareExchangeCommandBase.GetActualKey(databaseName, null);
            return GetNumberOf(compareExchange, prefix, context);
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
            Transaction.DebugDisposeReaderAfterTransaction(context.Transaction.InnerTransaction, doc);
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

        private static unsafe int GetDataAndEtagTupleFromReader(TransactionOperationContext context, TableValueReader reader, out BlittableJsonReaderObject doc,
            out long etag)
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
            Stream stream = null;
            try
            {
                tcpClient = await TcpUtils.ConnectAsync(info.Url, _parent.TcpConnectionTimeout).ConfigureAwait(false);
                stream = await TcpUtils.WrapStreamWithSslAsync(tcpClient, info, _parent.ClusterCertificate, _parent.TcpConnectionTimeout);

                var parameters = new TcpNegotiateParameters
                {
                    Database = null,
                    Operation = TcpConnectionHeaderMessage.OperationTypes.Cluster,
                    Version = TcpConnectionHeaderMessage.ClusterTcpVersion,
                    ReadResponseAndGetVersionCallback = ClusterReadResponseAndGetVersion,
                    DestinationUrl = info.Url,
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

        public const string SnapshotInstalled = "SnapshotInstalled";

        public override async Task OnSnapshotInstalledAsync(long lastIncludedIndex, ServerStore serverStore, CancellationToken token)
        {
            using (serverStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
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
                token.ThrowIfCancellationRequested();

                // there is potentially a lot of work to be done here so we are responding to the change on a separate task.
                var onDatabaseChanged = DatabaseChanged;
                if (onDatabaseChanged != null)
                {
                    var listOfDatabaseName = GetDatabaseNames(context).ToList();
                    TaskExecutor.Execute(_ =>
                    {
                        foreach (var db in listOfDatabaseName)
                        {
                            onDatabaseChanged.Invoke(this, (db, lastIncludedIndex, SnapshotInstalled, DatabasesLandlord.ClusterDatabaseChangeType.RecordChanged));
                        }
                    }, null);
                }

                var onValueChanged = ValueChanged;
                if (onValueChanged != null)
                {
                    TaskExecutor.Execute(_ =>
                    {
                        onValueChanged.Invoke(this, (lastIncludedIndex, nameof(InstallUpdatedServerCertificateCommand)));
                    }, null);
                }
                context.Transaction.Commit();
            }
            token.ThrowIfCancellationRequested();

            // reload license can send a notification which will open a write tx
            serverStore.LicenseManager.ReloadLicense();
            await serverStore.LicenseManager.CalculateLicenseLimits();

            _rachisLogIndexNotifications.NotifyListenersAbout(lastIncludedIndex, null);
        }

        protected override RachisVersionValidation InitializeValidator()
        {
            return new ClusterValidator();
        }
    }

    public class RachisLogIndexNotifications
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

        public async Task WaitForIndexNotification(long index, CancellationToken token)
        {
            while (true)
            {
                // first get the task, then wait on it
                var waitAsync = _notifiedListeners.WaitAsync(token);

                if (index <= Interlocked.Read(ref LastModifiedIndex))
                    break;

                token.ThrowIfCancellationRequested();

                if (await waitAsync == false)
                {
                    var copy = Interlocked.Read(ref LastModifiedIndex);
                    if (index <= copy)
                        break;
                }
            }
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

            if (_tasksDictionary.TryGetValue(index, out var tcs) == false)
            {
                // the task has already completed
                // let's check if we had errors in it
                foreach (var error in _errors)
                {
                    if (error.Index == index)
                        error.Exception.Throw();// rethrow
                }
                return;
            }

            var task = tcs.Task;

            if (task.IsCompleted)
                return;

            var result = await Task.WhenAny(task, TimeoutManager.WaitFor(timeout));

            if (result.IsFaulted)
                throw result.Exception;

            if (result == task)
                return;

            ThrowTimeoutException(timeout, index, LastModifiedIndex, isExecution: true);
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

// -----------------------------------------------------------------------
//  <copyright file="AdminDatabasesHandler.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Net;
using System.Security.Cryptography.X509Certificates;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http.Features.Authentication;
using Raven.Client;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Smuggler;
using Raven.Client.Exceptions;
using Raven.Client.Exceptions.Cluster;
using Raven.Client.Exceptions.Database;
using Raven.Client.Exceptions.Sharding;
using Raven.Client.Extensions;
using Raven.Client.Http;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;
using Raven.Client.ServerWide.Operations.Migration;
using Raven.Client.Util;
using Raven.Server.Config;
using Raven.Server.Config.Settings;
using Raven.Server.Documents;
using Raven.Server.Documents.Commands;
using Raven.Server.Documents.Handlers.Processors.Stats;
using Raven.Server.Documents.Indexes.Auto;
using Raven.Server.Documents.Operations;
using Raven.Server.Documents.Patch;
using Raven.Server.Documents.PeriodicBackup.Restore;
using Raven.Server.Exceptions;
using Raven.Server.Json;
using Raven.Server.Rachis;
using Raven.Server.Routing;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Commands;
using Raven.Server.ServerWide.Commands.Indexes;
using Raven.Server.ServerWide.Context;
using Raven.Server.Smuggler.Documents;
using Raven.Server.Smuggler.Documents.Data;
using Raven.Server.Smuggler.Migration;
using Raven.Server.Utils;
using Raven.Server.Web.Studio;
using Raven.Server.Web.System.Processors.Databases;
using Sparrow;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Logging;
using Sparrow.Server;
using Voron;
using Voron.Util.Settings;
using BackupUtils = Raven.Server.Utils.BackupUtils;
using Index = Raven.Server.Documents.Indexes.Index;
using Size = Sparrow.Size;

namespace Raven.Server.Web.System
{
    public sealed class AdminDatabasesHandler : ServerRequestHandler
    {
        private static readonly Logger Logger = LoggingSource.Instance.GetLogger<AdminDatabasesHandler>("Server");

        [RavenAction("/admin/databases", "GET", AuthorizationStatus.Operator)]
        public async Task Get()
        {
            using (var processor = new AdminDatabasesHandlerProcessorForGetDatabaseRecord(this))
                await processor.ExecuteAsync();
        }

        // add database to already existing database group
        [RavenAction("/admin/databases/node", "PUT", AuthorizationStatus.Operator)]
        public async Task AddDatabaseNode()
        {
            var name = GetQueryStringValueAndAssertIfSingleAndNotEmpty("name").Trim();
            var node = GetStringQueryString("node", false);
            var mentor = GetStringQueryString("mentor", false);
            var raftRequestId = GetRaftRequestIdFromQuery();

            await ServerStore.EnsureNotPassiveAsync();

            using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            using (context.OpenReadTransaction())
            {
                var databaseRecord = ServerStore.Cluster.ReadDatabase(context, name, out var index);
                if (databaseRecord == null)
                {
                    throw new DatabaseDoesNotExistException("Database Record not found when trying to add a node to the database topology");
                }

                if (databaseRecord.IsSharded)
                    throw new InvalidOperationException($"Can't add database {name} to node because it is a sharded database.");

                var clusterTopology = ServerStore.GetClusterTopology(context);

                if (databaseRecord.Encrypted)
                    ServerStore.LicenseManager.AssertCanCreateEncryptedDatabase();

                Server.ServerStore.LicenseManager.AssertCanUseDocumentsCompression(databaseRecord.DocumentsCompression);

                // the case where an explicit node was requested
                if (string.IsNullOrEmpty(node) == false)
                {
                    if (databaseRecord.Topology.RelevantFor(node))
                        throw new InvalidOperationException($"Can't add node {node} to {name} topology because it is already part of it");

                    ValidateNodeForAddingToDb(name, node, databaseRecord, clusterTopology, Server, baseMessage: $"Can't add node {node} to database '{name}' topology");
                }

                //The case were we don't care where the database will be added to
                else
                {
                    var allNodes = clusterTopology.Members.Keys
                        .Concat(clusterTopology.Promotables.Keys)
                        .Concat(clusterTopology.Watchers.Keys)
                        .ToList();

                    if (Server.AllowEncryptedDatabasesOverHttp == false)
                    {
                        allNodes.RemoveAll(n => databaseRecord.Topology.AllNodes.Contains(n) || (databaseRecord.Encrypted && NotUsingHttps(clusterTopology.GetUrlFromTag(n))));

                        if (databaseRecord.Encrypted && allNodes.Count == 0)
                            throw new InvalidOperationException($"Database {name} is encrypted and requires a node which supports SSL. There is no such node available in the cluster.");
                    }

                    if (allNodes.Count == 0)
                        throw new InvalidOperationException($"Database {name} already exists on all the nodes of the cluster");

                    var rand = new Random().Next();
                    node = allNodes[rand % allNodes.Count];
                }

                databaseRecord.Topology.Promotables.Add(node);
                databaseRecord.Topology.DemotionReasons[node] = "Joined the DB-Group as a new promotable node";
                databaseRecord.Topology.PromotablesStatus[node] = DatabasePromotionStatus.WaitingForFirstPromotion;

                if (mentor != null)
                {
                    if (databaseRecord.Topology.RelevantFor(mentor) == false)
                        throw new ArgumentException($"The node {mentor} is not part of the database group");
                    if (databaseRecord.Topology.Members.Contains(mentor) == false)
                        throw new ArgumentException($"The node {mentor} is not valid for the operation because it is not a member");
                    databaseRecord.Topology.PredefinedMentors.Add(node, mentor);
                }

                databaseRecord.Topology.ReplicationFactor++;

                var update = new UpdateTopologyCommand(name, SystemTime.UtcNow, raftRequestId)
                {
                    Topology = databaseRecord.Topology
                };

                var (newIndex, _) = await ServerStore.SendToLeaderAsync(update);

                try
                {
                    await ServerStore.WaitForExecutionOnSpecificNodeAsync(context, node, newIndex);
                }
                catch (DatabaseLoadFailureException e)
                {
                    // the node was added successfully, but failed to start
                    // in this case we don't want the request executor of the client to fail-over (so we wouldn't create an additional node)
                    throw new InvalidOperationException(e.Message, e);
                }

                HttpContext.Response.StatusCode = (int)HttpStatusCode.Created;

                await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
                {
                    context.Write(writer, new DynamicJsonValue
                    {
                        [nameof(DatabasePutResult.RaftCommandIndex)] = newIndex,
                        [nameof(DatabasePutResult.Name)] = name,
                        [nameof(DatabasePutResult.Topology)] = databaseRecord.Topology.ToJson()
                    });
                }
            }
        }

        public static bool NotUsingHttps(string url)
        {
            return url.StartsWith("https:", StringComparison.OrdinalIgnoreCase) == false;
        }

        [RavenAction("/admin/databases", "PUT", AuthorizationStatus.Operator)]
        public async Task Put()
        {
            var raftRequestId = GetRaftRequestIdFromQuery();
            
            await ServerStore.EnsureNotPassiveAsync();
            using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            {
                var index = GetLongFromHeaders("ETag");
                var replicationFactor = GetIntValueQueryString("replicationFactor", required: false) ?? 1;
                var json = await context.ReadForDiskAsync(RequestBodyStream(), "Database Record");
                var databaseRecord = JsonDeserializationCluster.DatabaseRecord(json);

                if (await ProxyToLeaderIfNeeded(context, databaseRecord, replicationFactor, index))
                    return;

                if (LoggingSource.AuditLog.IsInfoEnabled)
                {
                    LogAuditFor("DbMgmt", "PUT", $"Database '{databaseRecord.DatabaseName}'");
                }

                if (ServerStore.LicenseManager.LicenseStatus.HasDocumentsCompression && databaseRecord.DocumentsCompression == null)
                {
                    databaseRecord.DocumentsCompression = new DocumentsCompressionConfiguration(
                        Server.Configuration.Databases.CompressRevisionsDefault, Server.Configuration.Databases.CompressAllCollectionsDefault);
                }

                if (databaseRecord.Encrypted)
                    ServerStore.LicenseManager.AssertCanCreateEncryptedDatabase();

                ServerStore.LicenseManager.AssertCanUseDocumentsCompression(databaseRecord.DocumentsCompression);

                // Validate Directory
                var dataDirectoryThatWillBeUsed = databaseRecord.Settings.TryGetValue(RavenConfiguration.GetKey(x => x.Core.DataDirectory), out var dir) == false ?
                                                  ServerStore.Configuration.Core.DataDirectory.FullPath :
                                                  new PathSetting(dir, ServerStore.Configuration.Core.DataDirectory.FullPath).FullPath;

                if (string.IsNullOrWhiteSpace(dir) == false)
                {
                    if (ServerStore.Configuration.Core.EnforceDataDirectoryPath)
                    {
                        if (PathUtil.IsSubDirectory(dataDirectoryThatWillBeUsed, ServerStore.Configuration.Core.DataDirectory.FullPath) == false)
                        {
                            throw new ArgumentException($"The administrator has restricted databases to be created only under the DataDir '{ServerStore.Configuration.Core.DataDirectory.FullPath}'" +
                                                        $" but the actual requested path is '{dataDirectoryThatWillBeUsed}'.");
                        }
                    }

                    if (DataDirectoryInfo.CanAccessPath(dataDirectoryThatWillBeUsed, out var error) == false)
                    {
                        throw new InvalidOperationException($"Cannot access path '{dataDirectoryThatWillBeUsed}'. {error}");
                    }
                }

                // Validate Name
                databaseRecord.DatabaseName = databaseRecord.DatabaseName.Trim();
                if (ResourceNameValidator.IsValidResourceName(databaseRecord.DatabaseName, dataDirectoryThatWillBeUsed, out string errorMessage) == false)
                    throw new BadRequestException(errorMessage);

                Server.ServerStore.LicenseManager.AssertCanUseDocumentsCompression(databaseRecord.DocumentsCompression);

                if ((databaseRecord.Topology?.DynamicNodesDistribution ?? false) &&
                    Server.ServerStore.LicenseManager.CanDynamicallyDistributeNodes(withNotification: false, out var licenseLimit) == false)
                {
                    throw licenseLimit;
                }

                if (databaseRecord.Encrypted && databaseRecord.Topology?.DynamicNodesDistribution == true && Server.AllowEncryptedDatabasesOverHttp == false)
                {
                    throw new InvalidOperationException($"Cannot enable '{nameof(DatabaseTopology.DynamicNodesDistribution)}' for encrypted database: " + databaseRecord.DatabaseName);
                }

                if (databaseRecord.Indexes != null && databaseRecord.Indexes.Count > 0)
                {
                    foreach (var kvp in databaseRecord.Indexes)
                    {
                        var indexDefinition = kvp.Value;
                        Server.ServerStore.LicenseManager.AssertCanAddAdditionalAssembliesFromNuGet(indexDefinition);
                    }
                }

                using (var raw = new RawDatabaseRecord(context, json))
                {
                    foreach (var rawDatabaseRecord in raw.AsShardsOrNormal())
                    {
                        if (ServerStore.DatabasesLandlord.IsDatabaseLoaded(rawDatabaseRecord.DatabaseName) == false)
                        {
                            using (await ServerStore.DatabasesLandlord.UnloadAndLockDatabase(rawDatabaseRecord.DatabaseName, "Checking if we need to recreate indexes"))
                                RecreateIndexes(rawDatabaseRecord.DatabaseName, databaseRecord);
                        }
                    }
                }

                var (newIndex, topology, nodeUrlsAddedTo) = await CreateDatabase(databaseRecord.DatabaseName, databaseRecord, context, replicationFactor, index, raftRequestId);

                HttpContext.Response.StatusCode = (int)HttpStatusCode.Created;

                await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
                {
                    context.Write(writer, new DynamicJsonValue
                    {
                        [nameof(DatabasePutResult.RaftCommandIndex)] = newIndex,
                        [nameof(DatabasePutResult.Name)] = databaseRecord.DatabaseName,
                        [nameof(DatabasePutResult.Topology)] = topology.ToJson(),
                        [nameof(DatabasePutResult.NodesAddedTo)] = nodeUrlsAddedTo,
                        [nameof(DatabasePutResult.ShardsDefined)] = databaseRecord.IsSharded
                    });
                }
            }
        }

        private async Task<bool> ProxyToLeaderIfNeeded(JsonOperationContext context, DatabaseRecord databaseRecord, int replicationFactor, long? index)
        {
            var leaderTag = ServerStore.Engine.LeaderTag;
            if (leaderTag == null)
            {
                using (var cts = CreateHttpRequestBoundTimeLimitedOperationToken(TimeSpan.FromSeconds(15)))
                {
                    leaderTag = await ServerStore.Engine.WaitForLeaderChange(leader: null, cts.Token);
                }
            }
            if (leaderTag != ServerStore.NodeTag)
            {
                // proxy the command to the leader
                var leaderRequestExecutor = ServerStore.GetLeaderRequestExecutor(leaderTag);
                var command = new CreateDatabaseOperation.CreateDatabaseCommand(DocumentConventions.Default, databaseRecord, replicationFactor, index);
                await leaderRequestExecutor.ExecuteAsync(command, context);
                HttpContext.Response.StatusCode = (int)HttpStatusCode.Created;

                await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
                {
                    context.Write(writer, new DynamicJsonValue
                    {
                        [nameof(DatabasePutResult.RaftCommandIndex)] = command.Result.RaftCommandIndex,
                        [nameof(DatabasePutResult.Name)] = command.Result.Name,
                        [nameof(DatabasePutResult.Topology)] = command.Result.Topology.ToJson(),
                        [nameof(DatabasePutResult.NodesAddedTo)] = command.Result.NodesAddedTo
                    });
                }
                return true;
            }

            return false;
        }

        private void RecreateIndexes(string databaseName, DatabaseRecord databaseRecord)
        {
            var databaseConfiguration = ServerStore.DatabasesLandlord.CreateDatabaseConfiguration(databaseName, true, true, true, databaseRecord);
            if (databaseConfiguration.Indexing.RunInMemory ||
                Directory.Exists(databaseConfiguration.Indexing.StoragePath.FullPath) == false)
            {
                return;
            }

            var addToInitLog = new Action<LogMode, string>((logMode, txt) =>
            {
                var msg = $"[Recreating indexes] {DateTime.UtcNow} :: Database '{databaseName}' : {txt}";

                switch (logMode)
                {
                    case LogMode.Operations when Logger.IsOperationsEnabled:
                        Logger.Operations(msg);
                        break;
                    case LogMode.Information when Logger.IsInfoEnabled:
                        Logger.Info(msg);
                        break;
                }
            });

            using (var documentDatabase = DatabasesLandlord.CreateDocumentDatabase(databaseName, databaseConfiguration, ServerStore, addToInitLog))
            {
                var options = InitializeOptions.SkipLoadingDatabaseRecord;
                documentDatabase.Initialize(options);

                var indexesPath = databaseConfiguration.Indexing.StoragePath.FullPath;
                var sideBySideIndexes = new Dictionary<string, IndexDefinition>();

                foreach (var indexPath in Directory.GetDirectories(indexesPath))
                {
                    Index index = null;
                    try
                    {
                        if (documentDatabase.DatabaseShutdown.IsCancellationRequested)
                            return;

                        index = Index.Open(indexPath, documentDatabase, generateNewDatabaseId: false, out var _);
                        if (index == null)
                            continue;

                        var definition = index.Definition;
                        switch (index.Type)
                        {
                            case IndexType.AutoMap:
                            case IndexType.AutoMapReduce:
                                var autoIndexDefinition = PutAutoIndexCommand.GetAutoIndexDefinition((AutoIndexDefinitionBaseServerSide)definition, index.Type);
                                databaseRecord.AutoIndexes.Add(autoIndexDefinition.Name, autoIndexDefinition);
                                break;

                            case IndexType.Map:
                            case IndexType.MapReduce:
                            case IndexType.JavaScriptMap:
                            case IndexType.JavaScriptMapReduce:
                                var indexDefinition = index.GetIndexDefinition();
                                if (indexDefinition.Name.StartsWith(Constants.Documents.Indexing.SideBySideIndexNamePrefix, StringComparison.OrdinalIgnoreCase))
                                {
                                    // the side by side index is the last version of this index
                                    // and it's the one that should be stored in the database record
                                    indexDefinition.Name = indexDefinition.Name[Constants.Documents.Indexing.SideBySideIndexNamePrefix.Length..];
                                    sideBySideIndexes[indexDefinition.Name] = indexDefinition;
                                    continue;
                                }

                                databaseRecord.Indexes[indexDefinition.Name] = indexDefinition;
                                break;

                            default:
                                throw new NotSupportedException(index.Type.ToString());
                        }
                    }
                    catch (Exception e)
                    {
                        if (Logger.IsInfoEnabled)
                            Logger.Info($"Could not open index {Path.GetFileName(indexPath)}", e);
                    }
                    finally
                    {
                        index?.Dispose();
                    }
                }

                foreach ((string key, IndexDefinition value) in sideBySideIndexes)
                {
                    databaseRecord.Indexes[key] = value;
                }
            }
        }

        private async Task<(long Index, DatabaseTopology Topology, List<string> Urls)> CreateDatabase(string name, DatabaseRecord databaseRecord, TransactionOperationContext context, int replicationFactor, long? index, string raftRequestId)
        {
            var (newIndex, result) = await ServerStore.WriteDatabaseRecordAsync(name, databaseRecord, index, raftRequestId, replicationFactor: replicationFactor);
            await ServerStore.WaitForCommitIndexChange(RachisConsensus.CommitIndexModification.GreaterOrEqual, newIndex);

            var members = (List<string>)result;
            try
            {
                await ServerStore.WaitForExecutionOnRelevantNodesAsync(context, members, newIndex);
            }
            catch (RaftIndexWaitAggregateException e)
            {
                throw new InvalidDataException(
                    $"The database '{name}' was created but is not accessible, because one or more of the nodes on which this database was supposed to reside on, threw an exception.", e);
            }

            using (context.OpenReadTransaction())
            {
                var clusterTopology = ServerStore.GetClusterTopology(context);
                var nodeUrlsAddedTo = new List<string>();
                foreach (var member in members)
                {
                    nodeUrlsAddedTo.Add(clusterTopology.GetUrlFromTag(member));
                }

                DatabaseTopology topology;
                if (databaseRecord.IsSharded)
                {
                    topology = new DatabaseTopology
                    {
                        Members = members
                    };
                }
                else
                {
                    topology = ServerStore.Cluster.ReadDatabaseTopology(context, name);
                }
                return (newIndex, topology, nodeUrlsAddedTo);
            }
        }

        [RavenAction("/admin/databases/reorder", "POST", AuthorizationStatus.Operator)]
        public async Task Reorder()
        {
            var name = GetStringQueryString("name");
            using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            {
                DatabaseTopology topology;
                using (context.OpenReadTransaction())
                using (var rawRecord = ServerStore.Cluster.ReadRawDatabaseRecord(context, name))
                {
                    if (rawRecord == null)
                        DatabaseDoesNotExistException.Throw(name);

                    topology = rawRecord.IsSharded ? rawRecord.Sharding.Orchestrator.Topology : rawRecord.Topology;
                }

                var json = await context.ReadForMemoryAsync(RequestBodyStream(), "nodes");
                var parameters = JsonDeserializationServer.Parameters.MembersOrder(json);
                var reorderedTopology = DatabaseTopology.Reorder(topology, parameters.MembersOrder);

                topology.Members = reorderedTopology.Members;
                topology.Promotables = reorderedTopology.Promotables;
                topology.Rehabs = reorderedTopology.Rehabs;
                topology.PriorityOrder = parameters.Fixed ? parameters.MembersOrder : null;

                var reorder = new UpdateTopologyCommand(name, SystemTime.UtcNow, GetRaftRequestIdFromQuery())
                {
                    Topology = topology
                };

                var res = await ServerStore.SendToLeaderAsync(reorder);
                await ServerStore.Cluster.WaitForIndexNotification(res.Index);

                NoContentStatus();
            }
        }

        [RavenAction("/admin/restore/points", "POST", AuthorizationStatus.Operator)]
        public async Task GetRestorePoints()
        {
            using (var processor = new DatabasesHandlerProcessorForGetRestorePoints(this))
                await processor.ExecuteAsync();
        }

        [RavenAction("/admin/restore/database", "POST", AuthorizationStatus.Operator)]
        public async Task RestoreDatabase()
        {
            using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            {
                var cancelToken = CreateBackgroundOperationToken();
                var configuration = await context.ReadForMemoryAsync(RequestBodyStream(), "database-restore");
                var restoreConfiguration = RestoreUtils.GetRestoreConfigurationAndSource(ServerStore, configuration, out var restoreSource, cancelToken);

                if (restoreConfiguration.ShardRestoreSettings != null)
                {
                    if (restoreConfiguration.ShardRestoreSettings.Shards == null ||
                        restoreConfiguration.ShardRestoreSettings.Shards.Count == 0)
                        throw new InvalidOperationException(
                            $"Attempting to restore database {restoreConfiguration.DatabaseName} but configuration for field '{nameof(restoreConfiguration.ShardRestoreSettings.Shards)}' is not set.'");

                    foreach (var (shardNumber, shardRestoreSetting) in restoreConfiguration.ShardRestoreSettings.Shards)
                    {
                        if (string.IsNullOrEmpty(shardRestoreSetting.NodeTag))
                            throw new InvalidOperationException(
                                $"Attempting to restore database {restoreConfiguration.DatabaseName} but shard {shardNumber} was not provided a node tag.");

                        if (shardRestoreSetting.ShardNumber != shardNumber)
                            throw new InvalidOperationException(
                                $"Attempting to restore database {restoreConfiguration.DatabaseName} but there is a shard mismatch in the provided restore configuration: Shards[{shardNumber}].ShardNumber = {shardRestoreSetting.ShardNumber}");
                    }
                }

                await ServerStore.EnsureNotPassiveAsync();

                var operationId = ServerStore.Operations.GetNextOperationId();

                _ = ServerStore.Operations.AddLocalOperation(
                    operationId,
                    OperationType.DatabaseRestore,
                    $"Database restore: {restoreConfiguration.DatabaseName}",
                    detailedDescription: null,
                    taskFactory: async onProgress =>
                    {
                        using var restoreBackupTask = await RestoreUtils.CreateBackupTaskAsync(ServerStore, restoreConfiguration, restoreSource, operationId, cancelToken);
                        return await restoreBackupTask.ExecuteAsync(onProgress);
                    },
                    token: cancelToken);

                await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
                {
                    writer.WriteOperationIdAndNodeTag(context, operationId, ServerStore.NodeTag);
                }
            }
        }

        [RavenAction("/admin/backup-task/delay", "POST", AuthorizationStatus.Operator)]
        public async Task DelayBackupTask()
        {
            var id = GetLongQueryString("taskId");
            var delay = GetTimeSpanQueryString("duration");
            if (delay <= TimeSpan.Zero)
                throw new ArgumentOutOfRangeException(nameof(delay));

            var databaseName = GetStringQueryString("database");
            var database = await ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(databaseName).ConfigureAwait(false);
            if (database == null)
                DatabaseDoesNotExistException.Throw(databaseName);

            var delayUntil = DateTime.UtcNow.AddTicks(delay.Value.Ticks);

            using (var token = CreateHttpRequestBoundOperationToken())
            {
                await database.PeriodicBackupRunner.DelayAsync(id, delayUntil, GetCurrentCertificate(), token.Token);
            }

            if (LoggingSource.AuditLog.IsInfoEnabled)
            {
                LogAuditFor(databaseName, "DELAY", $"Backup task with task id '{id}' until '{delayUntil}' UTC");
            }

            NoContentStatus();
        }

        [RavenAction("/admin/databases", "DELETE", AuthorizationStatus.Operator)]
        public async Task Delete()
        {
            await ServerStore.EnsureNotPassiveAsync();

            var waitOnDeletion = new List<string>();
            var pendingDeletes = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            var databasesToDelete = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

            using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            {
                var json = await context.ReadForMemoryAsync(RequestBodyStream(), "docs");
                var parameters = JsonDeserializationServer.Parameters.DeleteDatabasesParameters(json);

                X509Certificate2 clientCertificate = null;

                if (LoggingSource.AuditLog.IsInfoEnabled)
                {
                    LogAuditFor("DbMgmt", "DELETE", $"Attempt to delete database(s) [{string.Join(", ", parameters.DatabaseNames)}] from ({string.Join(", ", parameters.FromNodes ?? Enumerable.Empty<string>())})");
                }

                using (context.OpenReadTransaction())
                {
                    foreach (var databaseName in parameters.DatabaseNames)
                    {
                        var isShard = ShardHelper.TryGetShardNumberAndDatabaseName(databaseName, out string shardedDatabaseName, out int shardNumber);
                        var dbRecordName = isShard ? shardedDatabaseName : databaseName;

                        using (var rawRecord = ServerStore.Cluster.ReadRawDatabaseRecord(context, dbRecordName))
                        {
                            if (rawRecord == null)
                                continue;

                            if (rawRecord.DatabaseState == DatabaseStateStatus.RestoreInProgress)
                                throw new InvalidOperationException($"Can't delete database '{databaseName}' while the restore " +
                                                                    $"process is in progress. In order to delete the database, " +
                                                                    $"you can cancel the restore task from node {rawRecord.Topology.Members[0]}");

                            if (isShard && rawRecord.Sharding.Shards.ContainsKey(shardNumber) == false)
                            {
                                throw new InvalidOperationException(
                                    $"Attempting to delete shard database {databaseName} but shard {shardNumber} doesn't exist for database {shardedDatabaseName}.");
                            }

                            switch (rawRecord.LockMode)
                            {
                                case DatabaseLockMode.Unlock:
                                    databasesToDelete.Add(databaseName);
                                    break;
                                case DatabaseLockMode.PreventDeletesIgnore:
                                    if (Logger.IsOperationsEnabled)
                                    {
                                        clientCertificate ??= GetCurrentCertificate();

                                        Logger.Operations($"Attempt to delete '{databaseName}' database was prevented due to lock mode set to '{rawRecord.LockMode}'. IP: '{HttpContext.Connection.RemoteIpAddress}'. Certificate: {clientCertificate?.Subject} ({clientCertificate?.Thumbprint})");
                                    }

                                    continue;
                                case DatabaseLockMode.PreventDeletesError:
                                    throw new InvalidOperationException($"Database '{databaseName}' cannot be deleted because of the set lock mode ('{rawRecord.LockMode}'). Please consider changing the lock mode before deleting the database.");
                                default:
                                    throw new ArgumentOutOfRangeException(nameof(rawRecord.LockMode));
                            }

                            if (parameters.FromNodes != null && parameters.FromNodes.Length > 0)
                            {
                                if (rawRecord.IsSharded && isShard == false)
                                    throw new InvalidOperationException($"Deleting entire sharded database {rawRecord.DatabaseName} from a specific node is not allowed.");

                                var topology = isShard ? rawRecord.Sharding.Shards[shardNumber] : rawRecord.Topology;

                                foreach (var node in parameters.FromNodes)
                                {
                                    if (topology.RelevantFor(node) == false)
                                    {
                                        throw new InvalidOperationException($"Database '{databaseName}' doesn't reside on node '{node}' so it can't be deleted from it");
                                    }

                                    if (isShard && topology.ReplicationFactor == 1)
                                    {
                                        if (rawRecord.Sharding.DoesShardHaveBuckets(shardNumber))
                                            throw new InvalidOperationException(
                                                $"Database {databaseName} cannot be deleted because it is the last copy of shard {shardNumber} and it contains data that has not been migrated.");
                                        if (rawRecord.Sharding.DoesShardHavePrefixes(shardNumber))
                                            throw new InvalidOperationException(
                                                $"Database {databaseName} cannot be deleted because it is the last copy of shard {shardNumber} and there are prefixes settings for this shard. " +
                                                $"In order to delete shard {shardNumber} from database {databaseName} you need to remove shard {shardNumber} from all prefixes settings in DatabaseRecord.Sharding.Prefixed.");
                                    }

                                    pendingDeletes.Add(node);
                                    topology.RemoveFromTopology(node);
                                }
                            }

                            waitOnDeletion.Add(databaseName);
                        }
                    }
                }

                if (LoggingSource.AuditLog.IsInfoEnabled)
                {
                    LogAuditFor("DbMgmt", "DELETE", $"Database(s) [{string.Join(", ", databasesToDelete)}] from ({string.Join(", ", parameters.FromNodes ?? Enumerable.Empty<string>())})");
                }

                long index = -1;
                foreach (var databaseName in databasesToDelete)
                {
                    var (newIndex, _) = await ServerStore.DeleteDatabaseAsync(databaseName, parameters.HardDelete, parameters.FromNodes, $"{GetRaftRequestIdFromQuery()}/{databaseName}");
                    index = newIndex;
                }

                long actualDeletionIndex = await WaitForDeletionToComplete(context, parameters, index, waitOnDeletion);

                await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
                {
                    context.Write(writer, new DynamicJsonValue
                    {
                        // we only send the successful index here, we might fail to delete the index
                        // because a node is down, and we don't want to cause the client to wait on an
                        // index that doesn't exists in the Raft log
                        [nameof(DeleteDatabaseResult.RaftCommandIndex)] = actualDeletionIndex,
                        [nameof(DeleteDatabaseResult.PendingDeletes)] = new DynamicJsonArray(pendingDeletes)
                    });
                }
            }
        }

        private async Task<long> WaitForDeletionToComplete(TransactionOperationContext context, DeleteDatabasesOperation.Parameters parameters, long index, IList<string> waitOnDeletion)
        {
            var timeToWaitForConfirmation = parameters.TimeToWaitForConfirmation ?? TimeSpan.FromSeconds(15);

            await ServerStore.Cluster.WaitForIndexNotification(index, timeToWaitForConfirmation);

            var fromNodes = parameters.FromNodes is { Length: > 0 };
            long actualDeletionIndex = index;
            var sp = Stopwatch.StartNew();
            int databaseIndex = 0;

            while (waitOnDeletion.Count > databaseIndex)
            {
                var databaseName = waitOnDeletion[databaseIndex];
                using (context.OpenReadTransaction())
                using (var raw = ServerStore.Cluster.ReadRawDatabaseRecord(context, databaseName))
                {
                    if (raw == null)
                    {
                        waitOnDeletion.RemoveAt(databaseIndex);
                        continue;
                    }

                    if (fromNodes)
                    {
                        var allNodesDeleted = true;
                        foreach (var node in parameters.FromNodes)
                        {
                            var key = DatabaseRecord.GetKeyForDeletionInProgress(node, databaseName);
                            if (raw.DeletionInProgress.ContainsKey(key) == false)
                                continue;

                            allNodesDeleted = false;
                            break;
                        }

                        if (allNodesDeleted)
                        {
                            waitOnDeletion.RemoveAt(databaseIndex);
                            continue;
                        }
                    }
                }

                // we'll now wait for the _next_ operation in the cluster
                // since deletion involve multiple operations in the cluster
                // we'll now wait for the next command to be applied and check
                // whatever that removed the db in question
                index++;
                var remaining = timeToWaitForConfirmation - sp.Elapsed;
                try
                {
                    if (remaining < TimeSpan.Zero)
                    {
                        databaseIndex++;
                        continue; // we are done waiting, but still want to locally check the rest of the dbs
                    }

                    await ServerStore.Cluster.WaitForIndexNotification(index, remaining);
                    actualDeletionIndex = index;
                }
                catch (TimeoutException)
                {
                    databaseIndex++;
                }
            }

            if (fromNodes)
            {
                try
                {
                    await ServerStore.WaitForExecutionOnRelevantNodesAsync(context, parameters.FromNodes.ToList(), actualDeletionIndex);
                }
                catch (RaftIndexWaitAggregateException e)
                {
                    throw new InvalidDataException($"Deletion of databases {string.Join(", ", parameters.DatabaseNames)} was performed, but it could not be propagated due to errors on one or more target nodes.", e);
                }
            }

            return actualDeletionIndex;
        }

        [RavenAction("/admin/databases/disable", "POST", AuthorizationStatus.Operator)]
        public async Task DisableDatabases()
        {
            await ToggleDisableDatabases(disable: true);
        }

        [RavenAction("/admin/databases/enable", "POST", AuthorizationStatus.Operator)]
        public async Task EnableDatabases()
        {
            await ToggleDisableDatabases(disable: false);
        }

        [RavenAction("/admin/databases/indexing", "POST", AuthorizationStatus.Operator)]
        public async Task ToggleIndexing()
        {
            var raftRequestId = GetRaftRequestIdFromQuery();
            var enable = GetBoolValueQueryString("enable") ?? true;

            using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            {
                var json = await context.ReadForMemoryAsync(RequestBodyStream(), "indexes/toggle");
                var parameters = JsonDeserializationServer.Parameters.DisableDatabaseToggleParameters(json);

                var (index, _) = await ServerStore.ToggleDatabasesStateAsync(ToggleDatabasesStateCommand.Parameters.ToggleType.Indexes, parameters.DatabaseNames, enable == false, $"{raftRequestId}");
                await ServerStore.Cluster.WaitForIndexNotification(index);

                NoContentStatus();
            }
        }

        [RavenAction("/admin/databases/dynamic-node-distribution", "POST", AuthorizationStatus.Operator)]
        public async Task ToggleDynamicDatabaseDistribution()
        {
            var name = GetQueryStringValueAndAssertIfSingleAndNotEmpty("name");
            var enable = GetBoolValueQueryString("enable") ?? true;
            var raftRequestId = GetRaftRequestIdFromQuery();

            if (enable &&
                Server.ServerStore.LicenseManager.CanDynamicallyDistributeNodes(withNotification: false, out var licenseLimit) == false)
            {
                throw licenseLimit;
            }

            var (index, _) = await ServerStore.ToggleDatabasesStateAsync(ToggleDatabasesStateCommand.Parameters.ToggleType.DynamicDatabaseDistribution, new[] { name }, enable == false, $"{raftRequestId}");
            await ServerStore.Cluster.WaitForIndexNotification(index);

            NoContentStatus();
        }

        private async Task ToggleDisableDatabases(bool disable)
        {
            using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            {
                var json = await context.ReadForMemoryAsync(RequestBodyStream(), "databases/toggle");
                var parameters = JsonDeserializationServer.Parameters.DisableDatabaseToggleParameters(json);

                var resultList = new List<DynamicJsonValue>();
                var raftRequestId = GetRaftRequestIdFromQuery();

                foreach (var name in parameters.DatabaseNames)
                {
                    using (context.OpenReadTransaction())
                    {
                        var databaseExists = ServerStore.Cluster.DatabaseExists(context, name);
                        if (databaseExists == false)
                        {
                            resultList.Add(new DynamicJsonValue
                            {
                                ["Name"] = name,
                                ["Success"] = false,
                                ["Reason"] = "database not found"
                            });
                            continue;
                        }
                    }

                    resultList.Add(new DynamicJsonValue
                    {
                        ["Name"] = name,
                        ["Success"] = true,
                        ["Disabled"] = disable,
                        ["Reason"] = $"Database state={disable} was propagated on the cluster"
                    });
                }

                var (index, _) = await ServerStore.ToggleDatabasesStateAsync(ToggleDatabasesStateCommand.Parameters.ToggleType.Databases, parameters.DatabaseNames, disable, $"{raftRequestId}");
                await ServerStore.Cluster.WaitForIndexNotification(index);

                await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
                {
                    writer.WriteStartObject();
                    writer.WritePropertyName("Status");

                    writer.WriteStartArray();
                    var first = true;
                    foreach (var result in resultList)
                    {
                        if (first == false)
                            writer.WriteComma();
                        first = false;

                        context.Write(writer, result);
                    }

                    writer.WriteEndArray();

                    writer.WriteEndObject();
                }
            }
        }

        [RavenAction("/admin/databases/promote", "POST", AuthorizationStatus.Operator)]
        public async Task PromoteImmediately()
        {
            var name = GetStringQueryString("name");
            var nodeTag = GetStringQueryString("node");

            using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            {
                var (index, _) = await ServerStore.PromoteDatabaseNode(name, nodeTag, GetRaftRequestIdFromQuery());
                await ServerStore.Cluster.WaitForIndexNotification(index);

                HttpContext.Response.StatusCode = (int)HttpStatusCode.OK;

                await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
                {
                    context.Write(writer, new DynamicJsonValue
                    {
                        [nameof(DatabasePutResult.Name)] = name,
                        [nameof(DatabasePutResult.RaftCommandIndex)] = index
                    });
                }
            }
        }

        [RavenAction("/admin/console", "POST", AuthorizationStatus.ClusterAdmin)]
        public async Task AdminConsole()
        {
            var name = GetStringQueryString("database", false);
            var isServerScript = GetBoolValueQueryString("serverScript", false) ?? false;
            var feature = HttpContext.Features.Get<IHttpAuthenticationFeature>() as RavenServer.AuthenticateConnection;
            var clientCert = feature?.Certificate?.FriendlyName;

            using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            {
                var content = await context.ReadForMemoryAsync(RequestBodyStream(), "read-admin-script");
                if (content.TryGet(nameof(AdminJsScript.Script), out string _) == false)
                {
                    throw new InvalidDataException("Field " + nameof(AdminJsScript.Script) + " was not found.");
                }

                var adminJsScript = JsonDeserializationCluster.AdminJsScript(content);
                string result;

                if (isServerScript)
                {
                    var console = new AdminJsConsole(Server, null);
                    if (console.Log.IsOperationsEnabled)
                    {
                        console.Log.Operations($"The certificate that was used to initiate the operation: {clientCert ?? "None"}");
                    }
                    if (LoggingSource.AuditLog.IsInfoEnabled)
                        LogAuditFor("Server", "Execute", $"AdminJSConsole Script: \"{adminJsScript.Script}\"");

                    result = console.ApplyScript(adminJsScript);
                }
                else if (string.IsNullOrWhiteSpace(name) == false)
                {
                    //database script
                    var database = await ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(name);
                    if (database == null)
                    {
                        DatabaseDoesNotExistException.Throw(name);
                    }

                    var console = new AdminJsConsole(Server, database);
                    if (console.Log.IsOperationsEnabled)
                    {
                        console.Log.Operations($"The certificate that was used to initiate the operation: {clientCert ?? "None"}");
                    }
                    if (LoggingSource.AuditLog.IsInfoEnabled)
                        LogAuditFor("Database", "Execute", $"AdminJSConsole Script: \"{adminJsScript.Script}\"");

                    result = console.ApplyScript(adminJsScript);
                }
                else
                {
                    throw new InvalidOperationException("'database' query string parameter not found, and 'serverScript' query string is not found. Don't know what to apply this script on");
                }

                HttpContext.Response.StatusCode = (int)HttpStatusCode.OK;
                await using (var textWriter = new StreamWriter(ResponseBodyStream()))
                {
                    await textWriter.WriteAsync(result);
                    await textWriter.FlushAsync();
                }
            }
        }

        [RavenAction("/admin/replication/conflicts/solver", "POST", AuthorizationStatus.ValidUser, EndpointType.Write)]
        public async Task UpdateConflictSolver()
        {
            var name = GetQueryStringValueAndAssertIfSingleAndNotEmpty("name");

            if (await CanAccessDatabaseAsync(name, requireAdmin: true, requireWrite: true) == false)
                return;

            await ServerStore.EnsureNotPassiveAsync();
            using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            {
                var json = await context.ReadForMemoryAsync(RequestBodyStream(), "read-conflict-resolver");
                var conflictResolver = DocumentConventions.DefaultForServer.Serialization.DefaultConverter.FromBlittable<ConflictSolver>(json, "convert-conflict-resolver");

                var (index, _) = await ServerStore.ModifyConflictSolverAsync(name, conflictResolver, GetRaftRequestIdFromQuery());
                await ServerStore.Cluster.WaitForIndexNotification(index);

                using (context.OpenReadTransaction())
                using (var rawRecord = ServerStore.Cluster.ReadRawDatabaseRecord(context, name))
                {
                    HttpContext.Response.StatusCode = (int)HttpStatusCode.Created;
                    var conflictSolverConfig = rawRecord.ConflictSolverConfiguration;
                    if (conflictSolverConfig == null)
                        throw new InvalidOperationException($"Database record doesn't have {nameof(DatabaseRecord.ConflictSolverConfig)} property.");

                    await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
                    {
                        context.Write(writer, new DynamicJsonValue
                        {
                            ["RaftCommandIndex"] = index,
                            ["Key"] = name,
                            [nameof(DatabaseRecord.ConflictSolverConfig)] = conflictSolverConfig.ToJson()
                        });
                    }
                }
            }
        }

        [RavenAction("/admin/compact", "POST", AuthorizationStatus.Operator, DisableOnCpuCreditsExhaustion = true)]
        public async Task CompactDatabase()
        {
            using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            {
                var compactSettingsJson = await context.ReadForMemoryAsync(RequestBodyStream(), string.Empty);

                var compactSettings = JsonDeserializationServer.CompactSettings(compactSettingsJson);

                if (string.IsNullOrEmpty(compactSettings.DatabaseName))
                    throw new InvalidOperationException($"{nameof(compactSettings.DatabaseName)} is a required field when compacting a database.");

                if (compactSettings.Documents == false && (compactSettings.Indexes == null || compactSettings.Indexes.Length == 0))
                    throw new InvalidOperationException($"{nameof(compactSettings.Documents)} is false in compact settings and no indexes were supplied. Nothing to compact.");

                using (context.OpenReadTransaction())
                using (var rawRecord = ServerStore.Cluster.ReadRawDatabaseRecord(context, compactSettings.DatabaseName))
                {
                    if (rawRecord == null)
                        throw new InvalidOperationException($"Cannot compact database {compactSettings.DatabaseName}, it doesn't exist.");

                    if (rawRecord.IsSharded)
                        throw new NotSupportedInShardingException($"Cannot compact database {compactSettings.DatabaseName} directly, it is a sharded database. Please compact each shard individually.");

                    if (rawRecord.Topology.RelevantFor(ServerStore.NodeTag) == false)
                        throw new InvalidOperationException($"Cannot compact database {compactSettings.DatabaseName} on node {ServerStore.NodeTag}, because it doesn't reside on this node.");
                }

                var database = await ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(compactSettings.DatabaseName).ConfigureAwait(false);
                var token = CreateBackgroundOperationToken();
                var compactDatabaseTask = new CompactDatabaseTask(
                    ServerStore,
                    compactSettings.DatabaseName,
                    token.Token);

                var operationId = ServerStore.Operations.GetNextOperationId();

                var t = ServerStore.Operations.AddLocalOperation(
                    operationId,
                    OperationType.DatabaseCompact,
                    "Compacting database: " + compactSettings.DatabaseName,
                    detailedDescription: null,
                    taskFactory: onProgress => Task.Run(async () =>
                    {
                        try
                        {
                            using (token)
                            {
                                var storageSize = await CalculateStorageSize(compactSettings.DatabaseName);
                                var before = storageSize.GetValue(SizeUnit.Megabytes);
                                var overallResult = new CompactionResult(compactSettings.DatabaseName);

                                if (compactSettings.Indexes != null && compactSettings.Indexes.Length > 0)
                                {
                                    using (database.PreventFromUnloadingByIdleOperations())
                                    using (var indexCts = CancellationTokenSource.CreateLinkedTokenSource(token.Token, database.DatabaseShutdown))
                                    {
                                        // first fill in data 
                                        foreach (var indexName in compactSettings.Indexes)
                                        {
                                            var indexCompactionResult = new CompactionResult(indexName);
                                            overallResult.IndexesResults.Add(indexName, indexCompactionResult);
                                        }

                                        // then do actual compaction
                                        foreach (var indexName in compactSettings.Indexes)
                                        {
                                            indexCts.Token.ThrowIfCancellationRequested();

                                            var index = database.IndexStore.GetIndex(indexName);
                                            var indexCompactionResult = (CompactionResult)overallResult.IndexesResults[indexName];

                                            if (index == null)
                                            {
                                                indexCompactionResult.Skipped = true;
                                                indexCompactionResult.Processed = true;

                                                indexCompactionResult.AddInfo($"Index '{indexName}' does not exist.");
                                                continue;
                                            }

                                            // we want to send progress of entire operation (indexes and documents), but we should update stats only for index compaction
                                            index.Compact(progress => onProgress(overallResult.Progress), indexCompactionResult, compactSettings.SkipOptimizeIndexes, indexCts.Token);
                                            indexCompactionResult.Processed = true;
                                        }
                                    }
                                }

                                if (compactSettings.Documents == false)
                                {
                                    overallResult.Skipped = true;
                                    overallResult.Processed = true;
                                    return overallResult;
                                }

                                await compactDatabaseTask.Execute(onProgress, overallResult);
                                overallResult.Processed = true;

                                storageSize = await CalculateStorageSize(compactSettings.DatabaseName);
                                overallResult.SizeAfterCompactionInMb = storageSize.GetValue(SizeUnit.Megabytes);
                                overallResult.SizeBeforeCompactionInMb = before;

                                return (IOperationResult)overallResult;
                            }
                        }
                        catch (Exception e)
                        {
                            if (Logger.IsOperationsEnabled)
                                Logger.Operations("Compaction process failed", e);

                            throw;
                        }
                    }, token.Token),
                    token: token);

                await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
                {
                    writer.WriteOperationIdAndNodeTag(context, operationId, ServerStore.NodeTag);
                }
            }
        }

        private async Task<Size> CalculateStorageSize(string databaseName)
        {
            var database = await ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(databaseName);
            if (database == null)
                throw new InvalidOperationException($"Could not load database '{databaseName}'.");

            using (database.PreventFromUnloadingByIdleOperations())
                return new Size(database.GetSizeOnDisk().Data.SizeInBytes, SizeUnit.Bytes);
        }

        [RavenAction("/admin/databases/unused-ids", "POST", AuthorizationStatus.Operator)]
        public async Task SetUnusedDatabaseIds()
        {
            var database = GetStringQueryString("name");
            var validate = GetBoolValueQueryString("validate", required: false) ?? false;

            await ServerStore.EnsureNotPassiveAsync();

            HashSet<string> unusedIds;

            using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            using (var json = await context.ReadForDiskAsync(RequestBodyStream(), "unused-databases-ids"))
            {
                var parameters = JsonDeserializationServer.Parameters.UnusedDatabaseParameters(json);
                unusedIds = parameters.DatabaseIds;
                validate |= parameters.Validate;
            }

            if (validate)
            {
                foreach (var id in unusedIds)
                    ValidateDatabaseIdFormat(id);
                
                using (var token = CreateHttpRequestBoundTimeLimitedOperationToken(ServerStore.Configuration.Cluster.OperationTimeout.AsTimeSpan))
                    await ValidateUnusedIdsAsync(unusedIds, database, token.Token);
            }

            var command = new UpdateUnusedDatabaseIdsCommand(database, unusedIds, GetRaftRequestIdFromQuery());
            await ServerStore.SendToLeaderAsync(command);

            NoContentStatus();
        }

        private static unsafe void ValidateDatabaseIdFormat(string id)
        {
            const int fixedLength = StorageEnvironment.Base64IdLength + StorageEnvironment.Base64IdLength % 4;

            if (id is not { Length: StorageEnvironment.Base64IdLength })
            {
                throw new InvalidOperationException($"Database ID '{id}' isn't valid because its length ({id.Length}) isn't {StorageEnvironment.Base64IdLength}.");
            }

            Span<byte> bytes = stackalloc byte[fixedLength / 3 * 4];
            char* buffer = stackalloc char[fixedLength];
            fixed (char* str = id)
            {
                Buffer.MemoryCopy(str, buffer, 24 * sizeof(char), StorageEnvironment.Base64IdLength * sizeof(char));
                for (int i = StorageEnvironment.Base64IdLength; i < fixedLength; i++)
                    buffer[i] = '=';

                if (Convert.TryFromBase64Chars(new ReadOnlySpan<char>(buffer, fixedLength), bytes, out _) == false)
                {
                    throw new InvalidOperationException($"Database ID '{id}' isn't valid because it isn't Base64Id (it contains chars which cannot be in Base64String).");
                }
            }
        }

        private async Task ValidateUnusedIdsAsync(HashSet<string> unusedIds, string database, CancellationToken token = default)
        {
            string[] nodesUrls;

            using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            using (context.OpenReadTransaction())
            {
                nodesUrls = ServerStore.GetClusterTopology(context).AllNodes.Values.ToArray();
            }

            using (var requestExecutor = RequestExecutor.CreateForServer(nodesUrls, database, Server.Certificate.Certificate, DocumentConventions.Default))
            using (requestExecutor.ContextPool.AllocateOperationContext(out var context))
            {
                var cmd = new ValidateUnusedIdsCommand(
                    new ValidateUnusedIdsCommand.Parameters { DatabaseIds = unusedIds });

                await requestExecutor.ExecuteAsync(cmd, context, token: token);
            }
        }


        [RavenAction("/admin/migrate", "POST", AuthorizationStatus.Operator, DisableOnCpuCreditsExhaustion = true)]
        public async Task MigrateDatabases()
        {
            using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            {
                var migrationConfiguration = await context.ReadForMemoryAsync(RequestBodyStream(), "migration-configuration");
                var migrationConfigurationJson = JsonDeserializationServer.DatabasesMigrationConfiguration(migrationConfiguration);

                if (string.IsNullOrWhiteSpace(migrationConfigurationJson.ServerUrl))
                    throw new ArgumentException("Url cannot be null or empty");

                var migrator = new Migrator(migrationConfigurationJson, ServerStore);

                await migrator.MigrateDatabases(migrationConfigurationJson.Databases, AuthorizationStatus.Operator);

                NoContentStatus();
            }
        }

        [RavenAction("/admin/migrate/offline", "POST", AuthorizationStatus.Operator, DisableOnCpuCreditsExhaustion = true)]
        public async Task MigrateDatabaseOffline()
        {
            await ServerStore.EnsureNotPassiveAsync();

            OfflineMigrationConfiguration configuration;
            using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            {
                var migrationConfiguration = await context.ReadForMemoryAsync(RequestBodyStream(), "migration-configuration");
                configuration = JsonDeserializationServer.OfflineMigrationConfiguration(migrationConfiguration);
            }

            var dataDir = configuration.DataDirectory;
            var dataDirectoryThatWillBeUsed = string.IsNullOrWhiteSpace(dataDir) ?
                                               ServerStore.Configuration.Core.DataDirectory.FullPath :
                                               new PathSetting(dataDir, ServerStore.Configuration.Core.DataDirectory.FullPath).FullPath;

            OfflineMigrationConfiguration.ValidateDataDirectory(dataDirectoryThatWillBeUsed);

            var dataExporter = OfflineMigrationConfiguration.EffectiveDataExporterFullPath(configuration.DataExporterFullPath);
            OfflineMigrationConfiguration.ValidateExporterPath(dataExporter);

            if (IOExtensions.EnsureReadWritePermissionForDirectory(dataDirectoryThatWillBeUsed) == false)
                throw new IOException($"Could not access {dataDirectoryThatWillBeUsed}");

            var databaseName = configuration.DatabaseRecord.DatabaseName;
            if (ResourceNameValidator.IsValidResourceName(databaseName, dataDirectoryThatWillBeUsed, out string errorMessage) == false)
                throw new BadRequestException(errorMessage);

            using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            {
                await CreateDatabase(databaseName, configuration.DatabaseRecord, context, 1, null, RaftIdGenerator.NewId());
            }

            var database = await ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(databaseName, ignoreDisabledDatabase: true);
            if (database == null)
            {
                throw new DatabaseDoesNotExistException($"Can't import into database {databaseName} because it doesn't exist.");
            }
            var options = new DatabaseSmugglerOptionsServerSide(GetAuthorizationStatusForSmuggler(databaseName));
            var (commandline, tmpFile) = configuration.GenerateExporterCommandLine();
            var processStartInfo = new ProcessStartInfo(dataExporter, commandline);

            var token = new OperationCancelToken(database.DatabaseShutdown);

            Task timeout = null;
            if (configuration.Timeout.HasValue)
            {
                timeout = Task.Delay((int)configuration.Timeout.Value.TotalMilliseconds, token.Token);
            }
            processStartInfo.RedirectStandardOutput = true;
            processStartInfo.RedirectStandardInput = true;
            var process = new Process
            {
                StartInfo = processStartInfo,
                EnableRaisingEvents = true
            };

            var processDone = new AsyncManualResetEvent(token.Token);
            process.Exited += (sender, e) =>
            {
                try
                {
                    processDone.Set();
                }
                catch (OperationCanceledException)
                {
                    // This is an expected exception during manually started operation cancellation
                }
            };

            process.Start();
            var result = new OfflineMigrationResult();
            var overallProgress = result.Progress as SmugglerResult.SmugglerProgress;
            var operationId = ServerStore.Operations.GetNextOperationId();

            // send new line to avoid issue with read key
            await process.StandardInput.WriteLineAsync();

            // don't await here - this operation is async - all we return is operation id
            var t = ServerStore.Operations.AddLocalOperation(
                operationId,
                OperationType.MigrationFromLegacyData,
                $"Migration of {dataDir} to {databaseName}",
                detailedDescription: null,
                onProgress =>
                {
                    return Task.Run(async () =>
                    {
                        try
                        {
                            using (database.PreventFromUnloadingByIdleOperations())
                            {
                                // send some initial progress so studio can open details
                                result.AddInfo("Starting migration");
                                result.AddInfo($"Path of temporary export file: {tmpFile}");
                                onProgress(overallProgress);
                                while (true)
                                {
                                    var (hasTimeout, readMessage) = await ReadLineOrTimeout(process, timeout, configuration, token.Token);
                                    if (readMessage == null)
                                    {
                                        // reached end of stream
                                        break;
                                    }

                                    if (token.Token.IsCancellationRequested)
                                        throw new TaskCanceledException("Was requested to cancel the offline migration task");
                                    if (hasTimeout)
                                    {
                                        //renewing the timeout so not to spam timeouts once the timeout is reached
                                        timeout = Task.Delay(configuration.Timeout.Value, token.Token);
                                    }

                                    result.AddInfo(readMessage);
                                    onProgress(overallProgress);
                                }

                                var ended = await processDone.WaitAsync(configuration.Timeout ?? TimeSpan.MaxValue);
                                if (ended == false)
                                {
                                    if (token.Token.IsCancellationRequested)
                                        throw new TaskCanceledException("Was requested to cancel the offline migration process midway");
                                    token.Cancel(); //To release the MRE
                                    throw new TimeoutException($"After waiting for {configuration.Timeout.HasValue} the export tool didn't exit, aborting.");
                                }

                                if (process.ExitCode != 0)
                                    throw new ApplicationException($"The data export tool have exited with code {process.ExitCode}.");

                                result.DataExporter.Processed = true;

                                if (File.Exists(configuration.OutputFilePath) == false)
                                    throw new FileNotFoundException($"Was expecting the output file to be located at {configuration.OutputFilePath}, but it is not there.");

                                result.AddInfo("Starting the import phase of the migration");
                                onProgress(overallProgress);
                                using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                                await using (var reader = File.OpenRead(configuration.OutputFilePath))
                                await using (var stream = await BackupUtils.GetDecompressionStreamAsync(reader))
                                using (var source = new StreamSource(stream, context, database.Name, options))
                                {
                                    var destination = database.Smuggler.CreateDestination();
                                    var smuggler = database.Smuggler.Create(source, destination, context,
                                        options,
                                        result: result, onProgress: onProgress, token: token.Token);

                                    await smuggler.ExecuteAsync();
                                }
                            }
                        }
                        catch (Exception e)
                        {
                            if (e is OperationCanceledException || e is ObjectDisposedException)
                            {
                                var killed = ProcessExtensions.TryKill(process);
                                result.AddError($"Exception: {e}, process pid: {process.Id}, killed: {killed}");
                                throw;
                            }
                            else
                            {
                                string errorString;
                                try
                                {
                                    var processErrorString = await ProcessExtensions.ReadOutput(process.StandardError).ConfigureAwait(false);
                                    errorString = $"Error occurred during migration. Process error: {processErrorString}, exception: {e}";
                                }
                                catch
                                {
                                    errorString = $"Error occurred during migration. Exception: {e}.";
                                }
                                result.AddError($"{errorString}");
                                onProgress.Invoke(result.Progress);

                                var killed = ProcessExtensions.TryKill(process);
                                throw new InvalidOperationException($"{errorString} Process pid: {process.Id}, killed: {killed}");
                            }
                        }
                        finally
                        {
                            if (process.HasExited && string.IsNullOrEmpty(tmpFile) == false)
                                IOExtensions.DeleteFile(tmpFile);
                            else if (process.HasExited == false && string.IsNullOrEmpty(tmpFile) == false)
                            {
                                if (ProcessExtensions.TryKill(process))
                                    IOExtensions.DeleteFile(tmpFile);
                                else
                                {
                                    var errorString = $"Error occurred during closing the tool. Process pid: {process.Id}, please close manually.";
                                    result.AddError($"{errorString}");
                                    throw new InvalidOperationException($"{errorString}");
                                }
                            }
                        }
                        return (IOperationResult)result;
                    });
                },
                token: token);

            using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
            {
                writer.WriteOperationIdAndNodeTag(context, operationId, ServerStore.NodeTag);
            }
        }

        [RavenAction("/admin/databases/set-lock", "POST", AuthorizationStatus.Operator)]
        public async Task SetLockMode()
        {
            var raftRequestId = GetRaftRequestIdFromQuery();
            using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            {
                var json = await context.ReadForMemoryAsync(RequestBodyStream(), "index/set-lock");
                var parameters = JsonDeserializationServer.Parameters.SetDatabaseLockParameters(json);

                if (parameters.DatabaseNames == null || parameters.DatabaseNames.Length == 0)
                    throw new ArgumentNullException(nameof(parameters.DatabaseNames));

                var databasesToUpdate = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
                using (context.OpenReadTransaction())
                {
                    foreach (var databaseName in parameters.DatabaseNames)
                    {
                        var record = ServerStore.Cluster.ReadRawDatabaseRecord(context, databaseName, out long index);
                        if (record == null)
                            DatabaseDoesNotExistException.Throw(databaseName);

                        if (record.LockMode == parameters.Mode)
                            continue;

                        databasesToUpdate.Add(databaseName);
                    }
                }

                if (databasesToUpdate.Count > 0)
                {
                    long index = 0;
                    foreach (var databaseName in databasesToUpdate)
                    {
                        var result = await ServerStore.SendToLeaderAsync(new EditLockModeCommand(databaseName, parameters.Mode, $"{databaseName}/{raftRequestId}"));
                        index = result.Index;
                    }

                    await ServerStore.Cluster.WaitForIndexNotification(index);
                }
            }

            NoContentStatus();
        }

        private static async Task<(bool HasTimeout, string Line)> ReadLineOrTimeout(Process process, Task timeout, OfflineMigrationConfiguration configuration, CancellationToken token)
        {
            var readline = process.StandardOutput.ReadLineAsync();
            string progressLine = null;
            if (timeout != null)
            {
                var finishedTask = await Task.WhenAny(readline, timeout);
                if (finishedTask == timeout)
                {
                    return (true, $"Export is taking more than the configured timeout {configuration.Timeout.Value}");
                }
            }
            else
            {
                progressLine = await readline.WithCancellation(token);
            }
            return (false, progressLine);
        }

    }
}

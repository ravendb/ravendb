// -----------------------------------------------------------------------
//  <copyright file="AdminDatabasesHandler.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http.Features.Authentication;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Session;
using Raven.Client.Documents.Smuggler;
using Raven.Client.Exceptions;
using Raven.Client.Exceptions.Database;
using Raven.Client.Http;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;
using Raven.Server.Documents;
using Raven.Server.Json;
using Raven.Server.Routing;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Raven.Server.Documents.Patch;
using Raven.Server.Rachis;
using Raven.Server.Smuggler.Migration;
using Raven.Server.ServerWide.Commands;
using Raven.Server.Smuggler.Documents;
using Raven.Client.Extensions;
using Raven.Client.ServerWide.Operations.Migration;
using Raven.Server.Config;
using Raven.Server.Documents.Indexes;
using Raven.Server.Documents.Indexes.Auto;
using Raven.Server.Documents.PeriodicBackup.Restore;
using Raven.Server.Extensions;
using Raven.Server.ServerWide.Commands.Indexes;
using Raven.Server.Utils;
using Sparrow.Logging;
using Sparrow;
using Voron.Util.Settings;
using Constants = Raven.Client.Constants;
using DatabaseSmuggler = Raven.Server.Smuggler.Documents.DatabaseSmuggler;

namespace Raven.Server.Web.System
{
    public class AdminDatabasesHandler : RequestHandler
    {
        private static readonly Logger Logger = LoggingSource.Instance.GetLogger<AdminDatabasesHandler>("Server");

        [RavenAction("/admin/databases", "GET", AuthorizationStatus.Operator)]
        public Task Get()
        {
            var name = GetQueryStringValueAndAssertIfSingleAndNotEmpty("name");

            using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            {
                var dbId = Constants.Documents.Prefix + name;
                using (context.OpenReadTransaction())
                using (var dbDoc = ServerStore.Cluster.Read(context, dbId, out long etag))
                {
                    if (dbDoc == null)
                    {
                        HttpContext.Response.StatusCode = (int)HttpStatusCode.NotFound;
                        HttpContext.Response.Headers["Database-Missing"] = name;
                        using (var writer = new BlittableJsonTextWriter(context, HttpContext.Response.Body))
                        {
                            context.Write(writer,
                                new DynamicJsonValue
                                {
                                    ["Type"] = "Error",
                                    ["Message"] = "Database " + name + " wasn't found"
                                });
                        }
                        return Task.CompletedTask;
                    }

                    using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
                    {
                        writer.WriteStartObject();
                        writer.WriteDocumentPropertiesWithoutMetadata(context, new Document
                        {
                            Data = dbDoc
                        });
                        writer.WriteComma();
                        writer.WritePropertyName("Etag");
                        writer.WriteInteger(etag);
                        writer.WriteEndObject();
                    }
                }
            }

            return Task.CompletedTask;
        }

        // add database to already existing database group
        [RavenAction("/admin/databases/node", "PUT", AuthorizationStatus.Operator)]
        public async Task AddDatabaseNode()
        {
            var name = GetQueryStringValueAndAssertIfSingleAndNotEmpty("name").Trim();
            var node = GetStringQueryString("node", false);
            var mentor = GetStringQueryString("mentor", false);

            string errorMessage;
            if (ResourceNameValidator.IsValidResourceName(name, ServerStore.Configuration.Core.DataDirectory.FullPath, out errorMessage) == false)
                throw new BadRequestException(errorMessage);

            ServerStore.EnsureNotPassive();
            using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            using (context.OpenReadTransaction())
            {
                var databaseRecord = ServerStore.Cluster.ReadDatabase(context, name, out var index);

                var clusterTopology = ServerStore.GetClusterTopology(context);

                if (databaseRecord.Encrypted)
                    ServerStore.LicenseManager.AssertCanCreateEncryptedDatabase();

                // the case where an explicit node was requested 
                if (string.IsNullOrEmpty(node) == false)
                {
                    if (databaseRecord.Topology.RelevantFor(node))
                        throw new InvalidOperationException($"Can't add node {node} to {name} topology because it is already part of it");

                    var url = clusterTopology.GetUrlFromTag(node);
                    if (url == null)
                        throw new InvalidOperationException($"Can't add node {node} to {name} topology because node {node} is not part of the cluster");

                    if (databaseRecord.Encrypted && NotUsingHttps(url))
                        throw new InvalidOperationException($"Can't add node {node} to database {name} topology because database {name} is encrypted but node {node} doesn't have an SSL certificate.");

                    databaseRecord.Topology.Promotables.Add(node);
                    databaseRecord.Topology.DemotionReasons[node] = "Joined the DB-Group as a new promotable node";
                    databaseRecord.Topology.PromotablesStatus[node] = DatabasePromotionStatus.WaitingForFirstPromotion;
                }

                //The case were we don't care where the database will be added to
                else
                {
                    var allNodes = clusterTopology.Members.Keys
                        .Concat(clusterTopology.Promotables.Keys)
                        .Concat(clusterTopology.Watchers.Keys)
                        .ToList();

                    allNodes.RemoveAll(n => databaseRecord.Topology.AllNodes.Contains(n) || (databaseRecord.Encrypted && NotUsingHttps(clusterTopology.GetUrlFromTag(n))));

                    if (databaseRecord.Encrypted && allNodes.Count == 0)
                        throw new InvalidOperationException($"Database {name} is encrypted and requires a node which supports SSL. There is no such node available in the cluster.");

                    if (allNodes.Count == 0)
                        throw new InvalidOperationException($"Database {name} already exists on all the nodes of the cluster");

                    var rand = new Random().Next();
                    node = allNodes[rand % allNodes.Count];

                    databaseRecord.Topology.Promotables.Add(node);
                    databaseRecord.Topology.DemotionReasons[node] = "Joined the DB-Group as a new promotable node";
                    databaseRecord.Topology.PromotablesStatus[node] = DatabasePromotionStatus.WaitingForFirstPromotion;
                }

                if (mentor != null)
                {
                    if (databaseRecord.Topology.RelevantFor(mentor) == false)
                        throw new ArgumentException($"The node {mentor} is not part of the database group");
                    if (databaseRecord.Topology.Members.Contains(mentor) == false)
                        throw new ArgumentException($"The node {mentor} is not valid for the operation because it is not a member");
                    databaseRecord.Topology.PredefinedMentors.Add(node, mentor);
                }

                databaseRecord.Topology.ReplicationFactor++;
                var (newIndex, _) = await ServerStore.WriteDatabaseRecordAsync(name, databaseRecord, index);

                await WaitForExecutionOnSpecificNode(context, clusterTopology, node, newIndex);

                HttpContext.Response.StatusCode = (int)HttpStatusCode.Created;

                using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
                {
                    context.Write(writer, new DynamicJsonValue
                    {
                        [nameof(DatabasePutResult.RaftCommandIndex)] = newIndex,
                        [nameof(DatabasePutResult.Name)] = name,
                        [nameof(DatabasePutResult.Topology)] = databaseRecord.Topology.ToJson()
                    });
                    writer.Flush();
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
            var name = GetQueryStringValueAndAssertIfSingleAndNotEmpty("name").Trim();
            if (ResourceNameValidator.IsValidResourceName(name, ServerStore.Configuration.Core.DataDirectory.FullPath, out string errorMessage) == false)
                throw new BadRequestException(errorMessage);

            if (LoggingSource.AuditLog.IsInfoEnabled)
            {
                var clientCert = GetCurrentCertificate();

                var auditLog = LoggingSource.AuditLog.GetLogger("DbMgmt", "Audit");
                auditLog.Info($"Database {name} PUT by {clientCert?.Subject} ({clientCert?.Thumbprint})");
            }


            ServerStore.EnsureNotPassive();
            using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            {
                context.OpenReadTransaction();

                var index = GetLongFromHeaders("ETag");
                var replicationFactor = GetIntValueQueryString("replicationFactor", required: false) ?? 1;
                var json = context.ReadForDisk(RequestBodyStream(), name);
                var databaseRecord = JsonDeserializationCluster.DatabaseRecord(json);
                if (string.IsNullOrWhiteSpace(databaseRecord.DatabaseName))
                    throw new ArgumentException("DatabaseName property has invalid value (null, empty or whitespace only)");
                databaseRecord.DatabaseName = databaseRecord.DatabaseName.Trim();

                if (ServerStore.Configuration.Core.EnforceDataDirectoryPath
                    && databaseRecord.Settings.TryGetValue(RavenConfiguration.GetKey(x => x.Core.DataDirectory), out var dir))
                {
                    var requestedDirectory = PathUtil.ToFullPath(dir, ServerStore.Configuration.Core.DataDirectory.FullPath);

                    if (PathUtil.IsSubDirectory(requestedDirectory, ServerStore.Configuration.Core.DataDirectory.FullPath) == false)
                    {
                        throw new ArgumentException($"The administrator has restricted databases to be created only under the DataDir '{ServerStore.Configuration.Core.DataDirectory.FullPath}' but the actual requested path is '{requestedDirectory}'.");
                    }
                }

                if ((databaseRecord.Topology?.DynamicNodesDistribution ?? false) &&
                    Server.ServerStore.LicenseManager.CanDynamicallyDistributeNodes(out var licenseLimit) == false)
                {
                    throw licenseLimit;
                }

                if (ServerStore.DatabasesLandlord.IsDatabaseLoaded(name) == false)
                {
                    using (await ServerStore.DatabasesLandlord.UnloadAndLockDatabase(name, "Checking if we need to recreate indexes"))
                        RecreateIndexes(databaseRecord);
                }

                var (newIndex, topology, nodeUrlsAddedTo) = await CreateDatabase(name, databaseRecord, context, replicationFactor, index);

                HttpContext.Response.StatusCode = (int)HttpStatusCode.Created;

                using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
                {
                    context.Write(writer, new DynamicJsonValue
                    {
                        [nameof(DatabasePutResult.RaftCommandIndex)] = newIndex,
                        [nameof(DatabasePutResult.Name)] = name,
                        [nameof(DatabasePutResult.Topology)] = topology.ToJson(),
                        [nameof(DatabasePutResult.NodesAddedTo)] = nodeUrlsAddedTo
                    });
                    writer.Flush();
                }
            }
        }

        private void RecreateIndexes(DatabaseRecord databaseRecord)
        {
            var databaseConfiguration = ServerStore.DatabasesLandlord.CreateDatabaseConfiguration(databaseRecord.DatabaseName, true, true, true, databaseRecord);
            if (databaseConfiguration.Indexing.RunInMemory ||
                Directory.Exists(databaseConfiguration.Indexing.StoragePath.FullPath) == false)
            {
                return;
            }

            var addToInitLog = new Action<string>(txt =>
            {
                var msg = $"[Recreating indexes] {DateTime.UtcNow} :: Database '{databaseRecord.DatabaseName}' : {txt}";
                if (Logger.IsInfoEnabled)
                    Logger.Info(msg);
            });

            using (var documentDatabase = new DocumentDatabase(databaseRecord.DatabaseName, databaseConfiguration, ServerStore, addToInitLog))
            {
                var options = InitializeOptions.SkipLoadingDatabaseRecord;
                documentDatabase.Initialize(options);

                var indexesPath = databaseConfiguration.Indexing.StoragePath.FullPath;
                foreach (var indexPath in Directory.GetDirectories(indexesPath))
                {
                    Index index = null;
                    try
                    {
                        if (documentDatabase.DatabaseShutdown.IsCancellationRequested)
                            return;

                        index = Index.Open(indexPath, documentDatabase);
                        if (index == null)
                            continue;

                        var definition = index.Definition;
                        switch (index.Type)
                        {
                            case IndexType.AutoMap:
                            case IndexType.AutoMapReduce:
                                var autoIndexDefinition = PutAutoIndexCommand.GetAutoIndexDefinition((AutoIndexDefinitionBase)definition, index.Type);
                                databaseRecord.AutoIndexes.Add(autoIndexDefinition.Name, autoIndexDefinition);
                                break;

                            case IndexType.Map:
                            case IndexType.MapReduce:
                            case IndexType.JavaScriptMap:
                            case IndexType.JavaScriptMapReduce:
                                var indexDefinition = index.GetIndexDefinition();
                                databaseRecord.Indexes.Add(indexDefinition.Name, indexDefinition);
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

            }

        }

        private async Task<(long, DatabaseTopology, List<string>)> CreateDatabase(string name, DatabaseRecord databaseRecord, TransactionOperationContext context, int replicationFactor, long? index)
        {
            var existingDatabaseRecord = ServerStore.Cluster.ReadDatabase(context, name, out long _);

            if (index.HasValue && existingDatabaseRecord == null)
                throw new BadRequestException($"Attempted to modify non-existing database: '{name}'");

            if (existingDatabaseRecord != null && index.HasValue == false)
                throw new ConcurrencyException($"Database '{name}' already exists!");

            if (replicationFactor <= 0)
                throw new ArgumentException("Replication factor must be greater than 0.");

            try
            {
                DatabaseHelper.Validate(name, databaseRecord, Server.Configuration);
            }
            catch (Exception e)
            {
                throw new BadRequestException("Database document validation failed.", e);
            }
            var clusterTopology = ServerStore.GetClusterTopology(context);
            ValidateClusterMembers(clusterTopology, databaseRecord);

            if (databaseRecord.Topology?.Members?.Count > 0)
            {
                var topology = databaseRecord.Topology;
                foreach (var member in topology.Members)
                {
                    if (clusterTopology.Contains(member) == false)
                        throw new ArgumentException($"Failed to add node {member}, because we don't have it in the cluster.");
                }
                topology.ReplicationFactor = topology.Members.Count;
            }
            else
            {
                if (databaseRecord.Topology == null)
                    databaseRecord.Topology = new DatabaseTopology();

                databaseRecord.Topology.ReplicationFactor = Math.Min(replicationFactor, clusterTopology.AllNodes.Count);
            }

            var (newIndex, result) = await ServerStore.WriteDatabaseRecordAsync(name, databaseRecord, index);
            await ServerStore.WaitForCommitIndexChange(RachisConsensus.CommitIndexModification.GreaterOrEqual, newIndex);

            var members = (List<string>)result;
            await WaitForExecutionOnRelevantNodes(context, name, clusterTopology, members, newIndex);

            var nodeUrlsAddedTo = new List<string>();
            foreach (var member in members)
            {
                nodeUrlsAddedTo.Add(clusterTopology.GetUrlFromTag(member));
            }

            using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext ctx))
            using (ctx.OpenReadTransaction())
            {
                var record = ServerStore.Cluster.ReadDatabase(ctx, name);
                return (newIndex, record.Topology, nodeUrlsAddedTo);
            }
        }

        [RavenAction("/admin/databases/reorder", "POST", AuthorizationStatus.Operator)]
        public async Task Reorder()
        {
            var name = GetStringQueryString("name");
            using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            {
                var record = ServerStore.LoadDatabaseRecord(name, out var _);
                if (record == null)
                {
                    DatabaseDoesNotExistException.Throw(name);
                }
                var json = await context.ReadForMemoryAsync(RequestBodyStream(), "nodes");
                var parameters = JsonDeserializationServer.Parameters.MembersOrder(json);

                var reorderedTopology = DatabaseTopology.Reorder(record.Topology, parameters.MembersOrder);

                record.Topology.Members = reorderedTopology.Members;
                record.Topology.Promotables = reorderedTopology.Promotables;
                record.Topology.Rehabs = reorderedTopology.Rehabs;

                var reorder = new UpdateTopologyCommand
                {
                    DatabaseName = name,
                    Topology = record.Topology
                };

                var res = await ServerStore.SendToLeaderAsync(reorder);
                await ServerStore.WaitForCommitIndexChange(RachisConsensus.CommitIndexModification.GreaterOrEqual, res.Index);

                NoContentStatus();
            }
        }

        private void ValidateClusterMembers(ClusterTopology clusterTopology, DatabaseRecord databaseRecord)
        {
            var topology = databaseRecord.Topology;

            if (topology == null)
                return;

            if (topology.Members?.Count == 1 && topology.Members[0] == "?")
            {
                // this is a special case where we pass '?' as member.
                topology.Members.Clear();
            }

            foreach (var node in topology.AllNodes)
            {
                var url = clusterTopology.GetUrlFromTag(node);
                if (databaseRecord.Encrypted && NotUsingHttps(url))
                    throw new InvalidOperationException($"{databaseRecord.DatabaseName} is encrypted but node {node} with url {url} doesn't use HTTPS. This is not allowed.");
            }
        }

        [RavenAction("/admin/restore/points", "POST", AuthorizationStatus.Operator)]
        public async Task GetRestorePoints()
        {
            using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            {
                var restorePathBlittable = await context.ReadForMemoryAsync(RequestBodyStream(), "database-restore-path");
                var restorePathJson = JsonDeserializationServer.DatabaseRestorePath(restorePathBlittable);

                var restorePoints = new RestorePoints();

                try
                {
                    Directory.GetLastAccessTime(restorePathJson.Path);
                }
                catch (UnauthorizedAccessException)
                {
                    throw new InvalidOperationException($"Unauthorized access to path: {restorePathJson.Path}");
                }

                if (Directory.Exists(restorePathJson.Path) == false)
                    throw new InvalidOperationException($"Path '{restorePathJson.Path}' doesn't exist");

                var sortedList = new SortedList<DateTime, RestorePoint>(new RestoreUtils.DescendedDateComparer());
                var directories = Directory.GetDirectories(restorePathJson.Path).OrderBy(x => x).ToList();
                if (directories.Count == 0)
                {
                    // no folders in directory
                    // will scan the directory for backup files
                    RestoreUtils.FetchRestorePoints(restorePathJson.Path, sortedList, context, assertLegacyBackups: true);
                }
                else
                {
                    foreach (var directory in directories)
                    {
                        RestoreUtils.FetchRestorePoints(directory, sortedList, context);
                    }
                }

                restorePoints.List = sortedList.Values.ToList();
                if (restorePoints.List.Count == 0)
                    throw new InvalidOperationException("Couldn't locate any backup files!");

                using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
                {
                    var blittable = EntityToBlittable.ConvertCommandToBlittable(restorePoints, context);
                    context.Write(writer, blittable);
                    writer.Flush();
                }
            }
        }

        [RavenAction("/admin/restore/database", "POST", AuthorizationStatus.Operator)]
        public async Task RestoreDatabase()
        {
            using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            {
                var restoreConfiguration = await context.ReadForMemoryAsync(RequestBodyStream(), "database-restore");
                var restoreConfigurationJson = JsonDeserializationCluster.RestoreBackupConfiguration(restoreConfiguration);

                var databaseName = restoreConfigurationJson.DatabaseName;
                if (string.IsNullOrWhiteSpace(databaseName))
                    throw new ArgumentException("Database name can't be null or empty");

                if (ResourceNameValidator.IsValidResourceName(databaseName, ServerStore.Configuration.Core.DataDirectory.FullPath, out string errorMessage) == false)
                    throw new BadRequestException(errorMessage);

                using (context.OpenReadTransaction())
                {
                    if (ServerStore.Cluster.ReadDatabase(context, databaseName) != null)
                        throw new ArgumentException($"Cannot restore data to an existing database named {databaseName}");

                    var clusterTopology = ServerStore.GetClusterTopology(context);

                    if (string.IsNullOrWhiteSpace(restoreConfigurationJson.EncryptionKey) == false)
                    {
                        var key = Convert.FromBase64String(restoreConfigurationJson.EncryptionKey);
                        if (key.Length != 256 / 8)
                            throw new InvalidOperationException($"The size of the key must be 256 bits, but was {key.Length * 8} bits.");

                        var isEncrypted = string.IsNullOrWhiteSpace(restoreConfigurationJson.EncryptionKey) == false;
                        if (isEncrypted && NotUsingHttps(clusterTopology.GetUrlFromTag(ServerStore.NodeTag)))
                            throw new InvalidOperationException("Cannot restore an encrypted database to a node which doesn't support SSL!");
                    }
                }

                var operationId = ServerStore.Operations.GetNextOperationId();
                var cancelToken = new OperationCancelToken(ServerStore.ServerShutdown);
                var restoreBackupTask = new RestoreBackupTask(
                    ServerStore,
                    restoreConfigurationJson,
                    ServerStore.NodeTag,
                    cancelToken);

                var t = ServerStore.Operations.AddOperation(
                    null,
                    $"Database restore: {databaseName}",
                    Documents.Operations.Operations.OperationType.DatabaseRestore,
                    taskFactory: onProgress => Task.Run(async () => await restoreBackupTask.Execute(onProgress), cancelToken.Token),
                    id: operationId, token: cancelToken);

                using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
                {
                    writer.WriteOperationId(context, operationId);
                }
            }
        }

        [RavenAction("/admin/databases", "DELETE", AuthorizationStatus.Operator)]
        public async Task Delete()
        {
            ServerStore.EnsureNotPassive();

            var waitOnRecordDeletion = new List<string>();
            var deletedDatabases = new List<string>();
            using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            {
                var json = await context.ReadForMemoryAsync(RequestBodyStream(), "docs");
                var parameters = JsonDeserializationServer.Parameters.DeleteDatabasesParameters(json);

                if (LoggingSource.AuditLog.IsInfoEnabled)
                {
                    var clientCert = GetCurrentCertificate();

                    var auditLog = LoggingSource.AuditLog.GetLogger("DbMgmt", "Audit");
                    auditLog.Info($"Delete [{string.Join(", ", parameters.DatabaseNames)}] database from ({string.Join(", ", parameters.FromNodes)}) by {clientCert?.Subject} ({clientCert?.Thumbprint})");
                }

                if (parameters.FromNodes != null && parameters.FromNodes.Length > 0)
                {
                    using (context.OpenReadTransaction())
                    {
                        foreach (var databaseName in parameters.DatabaseNames)
                        {
                            var record = ServerStore.Cluster.ReadDatabase(context, databaseName);
                            if (record == null)
                                continue;

                            foreach (var node in parameters.FromNodes)
                            {
                                if (record.Topology.RelevantFor(node) == false)
                                {
                                    throw new InvalidOperationException($"Database '{databaseName}' doesn't reside on node '{node}' so it can't be deleted from it");
                                }
                                deletedDatabases.Add(node);
                                record.Topology.RemoveFromTopology(node);
                            }

                            if (record.Topology.Count == 0)
                                waitOnRecordDeletion.Add(databaseName);
                        }
                    }
                }
                else
                {
                    foreach (var databaseName in parameters.DatabaseNames)
                    {
                        waitOnRecordDeletion.Add(databaseName);
                    }
                }

                long index = -1;
                foreach (var name in parameters.DatabaseNames)
                {
                    var (newIndex, _) = await ServerStore.DeleteDatabaseAsync(name, parameters.HardDelete, parameters.FromNodes);
                    index = newIndex;
                }
                await ServerStore.Cluster.WaitForIndexNotification(index);

                long actualDeletionIndex = index;

                var timeToWaitForConfirmation = parameters.TimeToWaitForConfirmation ?? TimeSpan.FromSeconds(15);

                var sp = Stopwatch.StartNew();
                int databaseIndex = 0;
                while (waitOnRecordDeletion.Count > databaseIndex)
                {
                    var databaseName = waitOnRecordDeletion[databaseIndex];
                    using (context.OpenReadTransaction())
                    {
                        var record = ServerStore.Cluster.ReadDatabase(context, databaseName);
                        if (record == null)
                        {
                            waitOnRecordDeletion.RemoveAt(databaseIndex);
                            continue;
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

                using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
                {
                    context.Write(writer, new DynamicJsonValue
                    {
                        // we only send the successful index here, we might fail to delete the index
                        // because a node is down, and we don't want to cause the client to wait on an
                        // index that doesn't exists in the Raft log
                        [nameof(DeleteDatabaseResult.RaftCommandIndex)] = actualDeletionIndex,
                        [nameof(DeleteDatabaseResult.PendingDeletes)] = new DynamicJsonArray(deletedDatabases)
                    });
                }
            }
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

        [RavenAction("/admin/databases/dynamic-node-distribution", "POST", AuthorizationStatus.Operator)]
        public async Task ToggleDynamicDatabaseDistribution()
        {
            var name = GetQueryStringValueAndAssertIfSingleAndNotEmpty("name");
            var enable = GetBoolValueQueryString("enable") ?? true;

            using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            {
                DatabaseRecord databaseRecord;
                long index;
                using (context.OpenReadTransaction())
                    databaseRecord = ServerStore.Cluster.ReadDatabase(context, name, out index);

                if (enable == databaseRecord.Topology.DynamicNodesDistribution)
                    return;

                if (enable &&
                    Server.ServerStore.LicenseManager.CanDynamicallyDistributeNodes(out var licenseLimit) == false)
                {
                    throw licenseLimit;
                }

                databaseRecord.Topology.DynamicNodesDistribution = enable;

                var (commandResultIndex, _) = await ServerStore.WriteDatabaseRecordAsync(name, databaseRecord, index);
                await ServerStore.Cluster.WaitForIndexNotification(commandResultIndex);

                NoContentStatus();
            }
        }

        private async Task ToggleDisableDatabases(bool disable)
        {
            using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            {
                var json = await context.ReadForMemoryAsync(RequestBodyStream(), "databases/toggle");
                var parameters = JsonDeserializationServer.Parameters.DisableDatabaseToggleParameters(json);

                using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
                {
                    writer.WriteStartObject();
                    writer.WritePropertyName("Status");

                    writer.WriteStartArray();
                    var first = true;
                    foreach (var name in parameters.DatabaseNames)
                    {
                        if (first == false)
                            writer.WriteComma();
                        first = false;

                        DatabaseRecord databaseRecord;
                        using (context.OpenReadTransaction())
                            databaseRecord = ServerStore.Cluster.ReadDatabase(context, name);

                        if (databaseRecord == null)
                        {
                            context.Write(writer, new DynamicJsonValue
                            {
                                ["Name"] = name,
                                ["Success"] = false,
                                ["Reason"] = "database not found"
                            });
                            continue;
                        }

                        if (databaseRecord.Disabled == disable)
                        {
                            var state = disable ? "disabled" : "enabled";
                            context.Write(writer, new DynamicJsonValue
                            {
                                ["Name"] = name,
                                ["Success"] = true, //even if we have nothing to do, no reason to return failure status
                                ["Disabled"] = disable,
                                ["Reason"] = $"Database already {state}"
                            });
                            continue;
                        }

                        databaseRecord.Disabled = disable;

                        var (index, _) = await ServerStore.WriteDatabaseRecordAsync(name, databaseRecord, null);
                        await ServerStore.Cluster.WaitForIndexNotification(index);

                        context.Write(writer, new DynamicJsonValue
                        {
                            ["Name"] = name,
                            ["Success"] = true,
                            ["Disabled"] = disable,
                            ["Reason"] = $"Database state={databaseRecord.Disabled} was propagated on the cluster"
                        });
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
                var (index, _) = await ServerStore.PromoteDatabaseNode(name, nodeTag);
                await ServerStore.Cluster.WaitForIndexNotification(index);

                HttpContext.Response.StatusCode = (int)HttpStatusCode.OK;

                using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
                {
                    context.Write(writer, new DynamicJsonValue
                    {
                        [nameof(DatabasePutResult.Name)] = name,
                        [nameof(DatabasePutResult.RaftCommandIndex)] = index
                    });
                    writer.Flush();
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
                    result = console.ApplyScript(adminJsScript);
                }

                else
                {
                    throw new InvalidOperationException("'database' query string parameter not found, and 'serverScript' query string is not found. Don't know what to apply this script on");
                }

                HttpContext.Response.StatusCode = (int)HttpStatusCode.OK;
                using (var textWriter = new StreamWriter(ResponseBodyStream()))
                {
                    textWriter.Write(result);
                    await textWriter.FlushAsync();
                }
            }
        }

        [RavenAction("/admin/replication/conflicts/solver", "POST", AuthorizationStatus.DatabaseAdmin)]
        public async Task UpdateConflictSolver()
        {
            var name = GetQueryStringValueAndAssertIfSingleAndNotEmpty("name");

            if (TryGetAllowedDbs(name, out var _, requireAdmin: true) == false)
                return;

            if (ResourceNameValidator.IsValidResourceName(name, ServerStore.Configuration.Core.DataDirectory.FullPath, out string errorMessage) == false)
                throw new BadRequestException(errorMessage);

            ServerStore.EnsureNotPassive();
            using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            {
                var json = await context.ReadForMemoryAsync(RequestBodyStream(), "read-conflict-resolver");
                var conflictResolver = (ConflictSolver)EntityToBlittable.ConvertToEntity(typeof(ConflictSolver), "convert-conflict-resolver", json, DocumentConventions.Default);

                using (context.OpenReadTransaction())
                {
                    var databaseRecord = ServerStore.Cluster.ReadDatabase(context, name, out _);

                    var (index, _) = await ServerStore.ModifyConflictSolverAsync(name, conflictResolver);
                    await ServerStore.Cluster.WaitForIndexNotification(index);

                    HttpContext.Response.StatusCode = (int)HttpStatusCode.Created;

                    using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
                    {
                        context.Write(writer, new DynamicJsonValue
                        {
                            ["RaftCommandIndex"] = index,
                            ["Key"] = name,
                            [nameof(DatabaseRecord.ConflictSolverConfig)] = databaseRecord.ConflictSolverConfig?.ToJson()
                        });
                        writer.Flush();
                    }
                }
            }
        }

        [RavenAction("/admin/compact", "POST", AuthorizationStatus.Operator)]
        public async Task CompactDatabase()
        {
            using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            {
                var compactSettingsJson = context.ReadForDisk(RequestBodyStream(), string.Empty);

                var compactSettings = JsonDeserializationServer.CompactSettings(compactSettingsJson);

                if (string.IsNullOrEmpty(compactSettings.DatabaseName))
                    throw new InvalidOperationException($"{nameof(compactSettings.DatabaseName)} is a required field when compacting a database.");

                if (compactSettings.Documents == false && compactSettings.Indexes.Length == 0)
                    throw new InvalidOperationException($"{nameof(compactSettings.Documents)} is false in compact settings and no indexes were supplied. Nothing to compact.");

                using (context.OpenReadTransaction())
                {
                    var record = ServerStore.Cluster.ReadDatabase(context, compactSettings.DatabaseName);
                    if (record == null)
                        throw new InvalidOperationException($"Cannot compact database {compactSettings.DatabaseName}, it doesn't exist.");
                    if (record.Topology.RelevantFor(ServerStore.NodeTag) == false)
                        throw new InvalidOperationException($"Cannot compact database {compactSettings.DatabaseName} on node {ServerStore.NodeTag}, because it doesn't reside on this node.");
                }

                var database = await ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(compactSettings.DatabaseName).ConfigureAwait(false);

                var token = new OperationCancelToken(ServerStore.ServerShutdown);
                var compactDatabaseTask = new CompactDatabaseTask(
                    ServerStore,
                    compactSettings.DatabaseName,
                    token.Token);

                var operationId = ServerStore.Operations.GetNextOperationId();

                var t = ServerStore.Operations.AddOperation(
                    null,
                    "Compacting database: " + compactSettings.DatabaseName,
                    Documents.Operations.Operations.OperationType.DatabaseCompact,
                    taskFactory: onProgress => Task.Run(async () =>
                    {
                        try
                        {
                            using (token)
                            using (var indexCts = CancellationTokenSource.CreateLinkedTokenSource(token.Token, database.DatabaseShutdown))
                            {
                                var before = (await CalculateStorageSize(compactSettings.DatabaseName)).GetValue(SizeUnit.Megabytes);
                                var overallResult = new CompactionResult(compactSettings.DatabaseName);

                                // first fill in data 
                                foreach (var indexName in compactSettings.Indexes)
                                {
                                    var indexCompactionResult = new CompactionResult(indexName);
                                    overallResult.IndexesResults.Add(indexName, indexCompactionResult);
                                }

                                // then do actual compaction
                                foreach (var indexName in compactSettings.Indexes)
                                {
                                    var index = database.IndexStore.GetIndex(indexName);
                                    var indexCompactionResult = overallResult.IndexesResults[indexName];
                                    index.Compact(onProgress, (CompactionResult)indexCompactionResult, indexCts.Token);
                                    indexCompactionResult.Processed = true;
                                }

                                if (compactSettings.Documents == false)
                                {
                                    overallResult.Skipped = true;
                                    overallResult.Processed = true;
                                    return overallResult;
                                }

                                await compactDatabaseTask.Execute(onProgress, overallResult);
                                overallResult.Processed = true;

                                overallResult.SizeAfterCompactionInMb = (await CalculateStorageSize(compactSettings.DatabaseName)).GetValue(SizeUnit.Megabytes);
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
                    id: operationId, token: token);

                using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
                {
                    writer.WriteOperationId(context, operationId);
                }
            }
        }

        private async Task<Size> CalculateStorageSize(string databaseName)
        {
            var database = await ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(databaseName);

            return new Size(database.GetSizeOnDisk().Data.SizeInBytes, SizeUnit.Bytes);
        }

        [RavenAction("/admin/migrate", "POST", AuthorizationStatus.ClusterAdmin)]
        public async Task MigrateDatabases()
        {
            using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            {
                var migrationConfiguration = await context.ReadForMemoryAsync(RequestBodyStream(), "migration-configuration");
                var migrationConfigurationJson = JsonDeserializationServer.DatabasesMigrationConfiguration(migrationConfiguration);

                if (string.IsNullOrWhiteSpace(migrationConfigurationJson.ServerUrl))
                    throw new ArgumentException("Url cannot be null or empty");

                var migrator = new Migrator(migrationConfigurationJson, ServerStore);
                await migrator.MigrateDatabases(migrationConfigurationJson.Databases);

                NoContentStatus();
            }
        }

        [RavenAction("/admin/migrate/offline", "POST", AuthorizationStatus.ClusterAdmin)]
        public async Task MigrateDatabaseOffline()
        {
            ServerStore.EnsureNotPassive();

            OfflineMigrationConfiguration configuration;
            using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            {
                var migrationConfiguration = await context.ReadForMemoryAsync(RequestBodyStream(), "migration-configuration");
                configuration = JsonDeserializationServer.OfflineMigrationConfiguration(migrationConfiguration);
            }

            var dataDir = configuration.DataDirectory;
            OfflineMigrationConfiguration.ValidateDataDirectory(dataDir);
            var dataExporter = OfflineMigrationConfiguration.EffectiveDataExporterFullPath(configuration.DataExporterFullPath);
            OfflineMigrationConfiguration.ValidateExporterPath(dataExporter);

            if (IOExtensions.EnsureReadWritePermissionForDirectory(dataDir) == false)
                throw new IOException($"Could not access {dataDir}");

            var databaseName = configuration.DatabaseRecord.DatabaseName;
            if (ResourceNameValidator.IsValidResourceName(databaseName, ServerStore.Configuration.Core.DataDirectory.FullPath, out string errorMessage) == false)
                throw new BadRequestException(errorMessage);

            using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            {
                context.OpenReadTransaction();
                await CreateDatabase(databaseName, configuration.DatabaseRecord, context, 1, null);
            }

            var database = await ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(databaseName, true);
            if (database == null)
            {
                throw new DatabaseDoesNotExistException($"Can't import into database {databaseName} because it doesn't exist.");
            }
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
            process.StandardInput.WriteLine();

            // don't await here - this operation is async - all we return is operation id 
            var t = ServerStore.Operations.AddOperation(null, $"Migration of {dataDir} to {databaseName}",
                Documents.Operations.Operations.OperationType.MigrationFromLegacyData,
                onProgress =>
                {
                    return Task.Run(async () =>
                    {
                        try
                        {
                            using (database.PreventFromUnloading())
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
                                using (var reader = File.OpenRead(configuration.OutputFilePath))
                                using (var stream = new GZipStream(reader, CompressionMode.Decompress))
                                using (var source = new StreamSource(stream, context, database))
                                {
                                    var destination = new DatabaseDestination(database);
                                    var smuggler = new DatabaseSmuggler(database, source, destination, database.Time, result: result, onProgress: onProgress,
                                        token: token.Token);

                                    smuggler.Execute();
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
                }, operationId, token: token);

            using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
            {
                writer.WriteOperationId(context, operationId);
            }
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

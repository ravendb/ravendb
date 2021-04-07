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
using System.Threading;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http.Features.Authentication;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.Documents.Smuggler;
using Raven.Client.Exceptions;
using Raven.Client.Exceptions.Database;
using Raven.Client.Extensions;
using Raven.Client.Http;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;
using Raven.Client.ServerWide.Operations.Migration;
using Raven.Client.Util;
using Raven.Server.Config;
using Raven.Server.Config.Categories;
using Raven.Server.Config.Settings;
using Raven.Server.Documents;
using Raven.Server.Documents.Indexes.Auto;
using Raven.Server.Documents.Patch;
using Raven.Server.Documents.PeriodicBackup;
using Raven.Server.Documents.PeriodicBackup.Restore;
using Raven.Server.Json;
using Raven.Server.Rachis;
using Raven.Server.Routing;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Commands;
using Raven.Server.ServerWide.Commands.Indexes;
using Raven.Server.ServerWide.Context;
using Raven.Server.Smuggler.Documents;
using Raven.Server.Smuggler.Migration;
using Raven.Server.Utils;
using Raven.Server.Web.Studio;
using Sparrow;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Logging;
using Sparrow.Server;
using Voron.Util.Settings;
using Constants = Raven.Client.Constants;
using DatabaseSmuggler = Raven.Server.Smuggler.Documents.DatabaseSmuggler;
using Index = Raven.Server.Documents.Indexes.Index;
using Size = Sparrow.Size;

namespace Raven.Server.Web.System
{
    public class AdminDatabasesHandler : ServerRequestHandler
    {
        private static readonly Logger Logger = LoggingSource.Instance.GetLogger<AdminDatabasesHandler>("Server");

        [RavenAction("/admin/databases", "GET", AuthorizationStatus.Operator)]
        public async Task Get()
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
                        await using (var writer = new AsyncBlittableJsonTextWriter(context, HttpContext.Response.Body))
                        {
                            context.Write(writer,
                                new DynamicJsonValue
                                {
                                    ["Type"] = "Error",
                                    ["Message"] = "Database " + name + " wasn't found"
                                });
                        }

                        return;
                    }

                    await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
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

            ConcurrencyException ce = null;

            for (int i = 0; i < 5; i++)
            {
                using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
                using (context.OpenReadTransaction())
                {
                    var databaseRecord = ServerStore.Cluster.ReadDatabase(context, name, out var index);
                    if (databaseRecord == null)
                    {
                        throw new DatabaseDoesNotExistException("Database Record not found when trying to add a node to the database topology");
                    }

                    var clusterTopology = ServerStore.GetClusterTopology(context);

                    if (databaseRecord.Encrypted)
                        ServerStore.LicenseManager.AssertCanCreateEncryptedDatabase();

                    Server.ServerStore.LicenseManager.AssertCanUseDocumentsCompression(databaseRecord.DocumentsCompression);

                    // the case where an explicit node was requested
                    if (string.IsNullOrEmpty(node) == false)
                    {
                        if (databaseRecord.Topology.RelevantFor(node))
                            throw new InvalidOperationException($"Can't add node {node} to {name} topology because it is already part of it");

                        var databaseIsBeenDeleted = databaseRecord.DeletionInProgress != null &&
                                                    databaseRecord.DeletionInProgress.TryGetValue(node, out var deletionInProgress) &&
                                                    deletionInProgress != DeletionInProgressStatus.No;
                        if (databaseIsBeenDeleted)
                            throw new InvalidOperationException($"Can't add node {node} to database '{name}' topology because it is currently being deleted from node '{node}'");

                        var url = clusterTopology.GetUrlFromTag(node);
                        if (url == null)
                            throw new InvalidOperationException($"Can't add node {node} to database '{name}' topology because node {node} is not part of the cluster");

                        if (databaseRecord.Encrypted && NotUsingHttps(url))
                            throw new InvalidOperationException($"Can't add node {node} to database '{name}' topology because database {name} is encrypted but node {node} doesn't have an SSL certificate.");
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
                    long newIndex;
                    try
                    {
                        newIndex = (await ServerStore.WriteDatabaseRecordAsync(name, databaseRecord, index, raftRequestId)).Index;
                    }
                    catch (ConcurrencyException e)
                    {
                        // something changed the database record while we were reading it? Let's try again
                        ce = e;
                        await Task.Delay(5 * i);
                        continue;
                    }

                    try
                    {
                        await WaitForExecutionOnSpecificNode(context, clusterTopology, node, newIndex);
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

                    return;
                }
            }
            throw new ConcurrencyException("Failed to update database record after 5 tries", ce);
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
            using (context.OpenReadTransaction())
            {
                var index = GetLongFromHeaders("ETag");
                var replicationFactor = GetIntValueQueryString("replicationFactor", required: false) ?? 1;
                var json = await context.ReadForDiskAsync(RequestBodyStream(), "Database Record");
                var databaseRecord = JsonDeserializationCluster.DatabaseRecord(json);

                if (LoggingSource.AuditLog.IsInfoEnabled)
                {
                    var clientCert = GetCurrentCertificate();

                    var auditLog = LoggingSource.AuditLog.GetLogger("DbMgmt", "Audit");
                    auditLog.Info($"Database {databaseRecord.DatabaseName} PUT by {clientCert?.Subject} ({clientCert?.Thumbprint})");
                }

                if (ServerStore.LicenseManager.LicenseStatus.HasDocumentsCompression &&
                   Server.Configuration.Core.FeaturesAvailability == FeaturesAvailability.Experimental &&
                   databaseRecord.DocumentsCompression == null)
                {
                    databaseRecord.DocumentsCompression = new DocumentsCompressionConfiguration(
                        Server.Configuration.Databases.CompressRevisionsDefault);
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

                if (databaseRecord.Encrypted && databaseRecord.Topology?.DynamicNodesDistribution == true)
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

                if (ServerStore.DatabasesLandlord.IsDatabaseLoaded(databaseRecord.DatabaseName) == false)
                {
                    using (await ServerStore.DatabasesLandlord.UnloadAndLockDatabase(databaseRecord.DatabaseName, "Checking if we need to recreate indexes"))
                        RecreateIndexes(databaseRecord);
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
                        [nameof(DatabasePutResult.NodesAddedTo)] = nodeUrlsAddedTo
                    });
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

        private async Task<(long, DatabaseTopology, List<string>)> CreateDatabase(string name, DatabaseRecord databaseRecord, TransactionOperationContext context, int replicationFactor, long? index, string raftRequestId)
        {
            var dbRecordExist = ServerStore.Cluster.DatabaseExists(context, name);
            if (index.HasValue && dbRecordExist == false)
                throw new BadRequestException($"Attempted to modify non-existing database: '{name}'");

            if (dbRecordExist && index.HasValue == false)
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

            if (databaseRecord.Topology?.Count > 0)
            {
                var topology = databaseRecord.Topology;
                foreach (var node in topology.AllNodes)
                {
                    if (clusterTopology.Contains(node) == false)
                        throw new ArgumentException($"Failed to add node {node}, because we don't have it in the cluster.");
                }
                topology.ReplicationFactor = Math.Min(topology.Count, clusterTopology.AllNodes.Count);
            }
            else
            {
                if (databaseRecord.Topology == null)
                    databaseRecord.Topology = new DatabaseTopology();

                databaseRecord.Topology.ReplicationFactor = Math.Min(replicationFactor, clusterTopology.AllNodes.Count);
            }

            var (newIndex, result) = await ServerStore.WriteDatabaseRecordAsync(name, databaseRecord, index, raftRequestId);
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
                var topology = ServerStore.Cluster.ReadDatabaseTopology(ctx, name);
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

                    topology = rawRecord.Topology;
                }

                var json = await context.ReadForMemoryAsync(RequestBodyStream(), "nodes");
                var parameters = JsonDeserializationServer.Parameters.MembersOrder(json);
                var reorderedTopology = DatabaseTopology.Reorder(topology, parameters.MembersOrder);

                topology.Members = reorderedTopology.Members;
                topology.Promotables = reorderedTopology.Promotables;
                topology.Rehabs = reorderedTopology.Rehabs;
                topology.PriorityOrder = parameters.Fixed ? parameters.MembersOrder : null;

                var reorder = new UpdateTopologyCommand(name, GetRaftRequestIdFromQuery())
                {
                    Topology = topology
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

            var unique = new HashSet<string>();
            foreach (var node in topology.AllNodes)
            {
                if (unique.Add(node) == false)
                    throw new InvalidOperationException($"node '{node}' already exists. This is not allowed.");

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
                PeriodicBackupConnectionType connectionType;
                var type = GetStringValuesQueryString("type", false).FirstOrDefault();
                if (type == null)
                {
                    //Backward compatibility
                    connectionType = PeriodicBackupConnectionType.Local;
                }
                else if (Enum.TryParse(type, out connectionType) == false)
                {
                    throw new ArgumentException($"Query string '{type}' was not recognized as valid type");
                }

                var restorePathBlittable = await context.ReadForMemoryAsync(RequestBodyStream(), "restore-info");
                var restorePoints = new RestorePoints();
                var sortedList = new SortedList<DateTime, RestorePoint>(new RestorePointsBase.DescendedDateComparer());

                switch (connectionType)
                {
                    case PeriodicBackupConnectionType.Local:
                        var localSettings = JsonDeserializationServer.LocalSettings(restorePathBlittable);
                        var directoryPath = localSettings.FolderPath;

                        try
                        {
                            Directory.GetLastAccessTime(directoryPath);
                        }
                        catch (UnauthorizedAccessException)
                        {
                            throw new InvalidOperationException($"Unauthorized access to path: {directoryPath}");
                        }

                        if (Directory.Exists(directoryPath) == false)
                            throw new InvalidOperationException($"Path '{directoryPath}' doesn't exist");

                        var localRestoreUtils = new LocalRestorePoints(sortedList, context);
                        await localRestoreUtils.FetchRestorePoints(directoryPath);

                        break;

                    case PeriodicBackupConnectionType.S3:
                        var s3Settings = JsonDeserializationServer.S3Settings(restorePathBlittable);
                        using (var s3RestoreUtils = new S3RestorePoints(sortedList, context, s3Settings))
                        {
                            await s3RestoreUtils.FetchRestorePoints(s3Settings.RemoteFolderName);
                        }

                        break;

                    case PeriodicBackupConnectionType.Azure:
                        var azureSettings = JsonDeserializationServer.AzureSettings(restorePathBlittable);
                        using (var azureRestoreUtils = new AzureRestorePoints(sortedList, context, azureSettings))
                        {
                            await azureRestoreUtils.FetchRestorePoints(azureSettings.RemoteFolderName);
                        }
                        break;

                    case PeriodicBackupConnectionType.GoogleCloud:
                        var googleCloudSettings = JsonDeserializationServer.GoogleCloudSettings(restorePathBlittable);
                        using (var googleCloudRestoreUtils = new GoogleCloudRestorePoints(sortedList, context, googleCloudSettings))
                        {
                            await googleCloudRestoreUtils.FetchRestorePoints(googleCloudSettings.RemoteFolderName);
                        }
                        break;

                    default:
                        throw new ArgumentOutOfRangeException();
                }

                restorePoints.List = sortedList.Values.ToList();
                if (restorePoints.List.Count == 0)
                    throw new InvalidOperationException("Couldn't locate any backup files.");

                await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
                {
                    var blittable = DocumentConventions.DefaultForServer.Serialization.DefaultConverter.ToBlittable(restorePoints, context);
                    context.Write(writer, blittable);
                }
            }
        }

        [RavenAction("/admin/restore/database", "POST", AuthorizationStatus.Operator)]
        public async Task RestoreDatabase()
        {
            using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            {
                var restoreConfiguration = await context.ReadForMemoryAsync(RequestBodyStream(), "database-restore");
                await ServerStore.EnsureNotPassiveAsync();

                RestoreType restoreType;
                if (restoreConfiguration.TryGet("Type", out string typeAsString))
                {
                    if (Enum.TryParse(typeAsString, out restoreType) == false)
                        throw new ArgumentException($"{typeAsString} is unknown backup type.");
                }
                else
                {
                    restoreType = RestoreType.Local;
                }
                var operationId = ServerStore.Operations.GetNextOperationId();
                var cancelToken = CreateOperationToken();
                RestoreBackupTaskBase restoreBackupTask;
                switch (restoreType)
                {
                    case RestoreType.Local:
                        var localConfiguration = JsonDeserializationCluster.RestoreBackupConfiguration(restoreConfiguration);
                        restoreBackupTask = new RestoreFromLocal(
                            ServerStore,
                            localConfiguration,
                            ServerStore.NodeTag,
                            cancelToken);
                        break;

                    case RestoreType.S3:
                        var s3Configuration = JsonDeserializationCluster.RestoreS3BackupConfiguration(restoreConfiguration);
                        restoreBackupTask = new RestoreFromS3(
                            ServerStore,
                            s3Configuration,
                            ServerStore.NodeTag,
                            cancelToken);
                        break;

                    case RestoreType.Azure:
                        var azureConfiguration = JsonDeserializationCluster.RestoreAzureBackupConfiguration(restoreConfiguration);
                        restoreBackupTask = new RestoreFromAzure(
                            ServerStore,
                            azureConfiguration,
                            ServerStore.NodeTag,
                            cancelToken);
                        break;

                    case RestoreType.GoogleCloud:
                        var googlCloudConfiguration = JsonDeserializationCluster.RestoreGoogleCloudBackupConfiguration(restoreConfiguration);
                        restoreBackupTask = new RestoreFromGoogleCloud(
                            ServerStore,
                            googlCloudConfiguration,
                            ServerStore.NodeTag,
                            cancelToken);
                        break;

                    default:
                        throw new InvalidOperationException($"No matching backup type was found for {restoreType}");
                }

                var t = ServerStore.Operations.AddOperation(
                    null,
                    $"Database restore: {restoreBackupTask.RestoreFromConfiguration.DatabaseName}",
                    Documents.Operations.Operations.OperationType.DatabaseRestore,
                    taskFactory: onProgress => Task.Run(async () => await restoreBackupTask.Execute(onProgress), cancelToken.Token),
                    id: operationId, token: cancelToken);

                await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
                {
                    writer.WriteOperationIdAndNodeTag(context, operationId, ServerStore.NodeTag);
                }
            }
        }

        [RavenAction("/admin/databases", "DELETE", AuthorizationStatus.Operator)]
        public async Task Delete()
        {
            await ServerStore.EnsureNotPassiveAsync();

            var waitOnRecordDeletion = new List<string>();
            var pendingDeletes = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            var databasesToDelete = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

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

                using (context.OpenReadTransaction())
                {
                    var fromNodes = parameters.FromNodes != null && parameters.FromNodes.Length > 0;

                    foreach (var databaseName in parameters.DatabaseNames)
                    {
                        DatabaseTopology topology = null;
                        using (var rawRecord = ServerStore.Cluster.ReadRawDatabaseRecord(context, databaseName))
                        {
                            if (rawRecord == null)
                                continue;

                            switch (rawRecord.LockMode)
                            {
                                case DatabaseLockMode.Unlock:
                                    databasesToDelete.Add(databaseName);
                                    break;
                                case DatabaseLockMode.PreventDeletesIgnore:
                                    continue;
                                case DatabaseLockMode.PreventDeletesError:
                                    throw new InvalidOperationException($"Database '{databaseName}' cannot be deleted because of the set lock mode ('{rawRecord.LockMode}'). Please consider changing the lock mode before deleting the database.");
                                default:
                                    throw new ArgumentOutOfRangeException(nameof(rawRecord.LockMode));
                            }

                            if (fromNodes)
                                topology = rawRecord.Topology;
                        }

                        if (fromNodes)
                        {
                            foreach (var node in parameters.FromNodes)
                            {
                                if (topology.RelevantFor(node) == false)
                                {
                                    throw new InvalidOperationException($"Database '{databaseName}' doesn't reside on node '{node}' so it can't be deleted from it");
                                }
                                pendingDeletes.Add(node);
                                topology.RemoveFromTopology(node);
                            }

                            if (topology.Count == 0)
                                waitOnRecordDeletion.Add(databaseName);

                            continue;
                        }

                        waitOnRecordDeletion.Add(databaseName);
                    }
                }

                long index = -1;
                foreach (var databaseName in databasesToDelete)
                {
                    var (newIndex, _) = await ServerStore.DeleteDatabaseAsync(databaseName, parameters.HardDelete, parameters.FromNodes, $"{GetRaftRequestIdFromQuery()}/{databaseName}");
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
                        if (ServerStore.Cluster.DatabaseExists(context, databaseName) == false)
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

            using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            {
                var (index, _) = await ServerStore.ToggleDatabasesStateAsync(ToggleDatabasesStateCommand.Parameters.ToggleType.DynamicDatabaseDistribution, new[] { name }, enable == false, $"{raftRequestId}");
                await ServerStore.Cluster.WaitForIndexNotification(index);

                NoContentStatus();
            }
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
                var compactSettingsJson = await context.ReadForDiskAsync(RequestBodyStream(), string.Empty);

                var compactSettings = JsonDeserializationServer.CompactSettings(compactSettingsJson);

                if (string.IsNullOrEmpty(compactSettings.DatabaseName))
                    throw new InvalidOperationException($"{nameof(compactSettings.DatabaseName)} is a required field when compacting a database.");

                if (compactSettings.Documents == false && compactSettings.Indexes.Length == 0)
                    throw new InvalidOperationException($"{nameof(compactSettings.Documents)} is false in compact settings and no indexes were supplied. Nothing to compact.");

                using (context.OpenReadTransaction())
                using (var rawRecord = ServerStore.Cluster.ReadRawDatabaseRecord(context, compactSettings.DatabaseName))
                {
                    if (rawRecord == null)
                        throw new InvalidOperationException($"Cannot compact database {compactSettings.DatabaseName}, it doesn't exist.");

                    if (rawRecord.Topology.RelevantFor(ServerStore.NodeTag) == false)
                        throw new InvalidOperationException($"Cannot compact database {compactSettings.DatabaseName} on node {ServerStore.NodeTag}, because it doesn't reside on this node.");
                }

                var database = await ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(compactSettings.DatabaseName).ConfigureAwait(false);

                var token = CreateOperationToken();
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

                                    // we want to send progress of entire operation (indexes and documents), but we should update stats only for index compaction
                                    index.Compact(progress => onProgress(overallResult.Progress), indexCompactionResult, indexCts.Token);
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

                await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
                {
                    writer.WriteOperationIdAndNodeTag(context, operationId, ServerStore.NodeTag);
                }
            }
        }

        private async Task<Size> CalculateStorageSize(string databaseName)
        {
            var database = await ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(databaseName);

            return new Size(database.GetSizeOnDisk().Data.SizeInBytes, SizeUnit.Bytes);
        }

        [RavenAction("/admin/databases/unused-ids", "POST", AuthorizationStatus.Operator)]
        public async Task SetUnusedDatabaseIds()
        {
            var database = GetStringQueryString("name");
            await ServerStore.EnsureNotPassiveAsync();

            using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            using (var json = await context.ReadForDiskAsync(RequestBodyStream(), "unused-databases-ids"))
            {
                var parameters = JsonDeserializationServer.Parameters.UnusedDatabaseParameters(json);
                var command = new UpdateUnusedDatabaseIdsCommand(database, parameters.DatabaseIds, GetRaftRequestIdFromQuery());
                await ServerStore.SendToLeaderAsync(command);
            }

            NoContentStatus();
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
                await migrator.MigrateDatabases(migrationConfigurationJson.Databases);

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
                context.OpenReadTransaction();
                await CreateDatabase(databaseName, configuration.DatabaseRecord, context, 1, null, RaftIdGenerator.NewId());
            }

            var database = await ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(databaseName, ignoreDisabledDatabase: true);
            if (database == null)
            {
                throw new DatabaseDoesNotExistException($"Can't import into database {databaseName} because it doesn't exist.");
            }
            var (commandline, tmpFile) = configuration.GenerateExporterCommandLine();
            var processStartInfo = new ProcessStartInfo(dataExporter, commandline);
            var token = new OperationCancelToken(database.DatabaseShutdown, HttpContext.RequestAborted);
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
                                await using (var reader = File.OpenRead(configuration.OutputFilePath))
                                await using (var stream = new GZipStream(reader, CompressionMode.Decompress))
                                using (var source = new StreamSource(stream, context, database))
                                {
                                    var destination = new DatabaseDestination(database);
                                    var smuggler = new DatabaseSmuggler(database, source, destination, database.Time, result: result, onProgress: onProgress,
                                        token: token.Token);

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
                }, operationId, token: token);

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

                var databasesToUpdate = new List<string>();
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
                        //var result = await ServerStore.WriteDatabaseRecordAsync(kvp.DatabaseRecord.DatabaseName, kvp.DatabaseRecord, kvp.Index, raftRequestId);
                        var result = await ServerStore.SendToLeaderAsync(new EditLockModeCommand(databaseName, parameters.Mode, raftRequestId));
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

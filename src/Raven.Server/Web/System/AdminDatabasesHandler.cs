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
using System.Threading;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http.Features.Authentication;
using NCrontab.Advanced;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Session;
using Raven.Client.Exceptions;
using Raven.Client.Exceptions.Database;
using Raven.Client.Http;
using Raven.Client.Json.Converters;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Commands;
using Raven.Client.ServerWide.ETL;
using Raven.Client.ServerWide.Operations;
using Raven.Client.ServerWide.Operations.ConnectionStrings;
using Raven.Client.ServerWide.PeriodicBackup;
using Raven.Server.Documents;
using Raven.Server.Json;
using Raven.Server.Routing;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Raven.Server.Documents.Patch;
using Raven.Server.Documents.PeriodicBackup;
using Raven.Server.Documents.PeriodicBackup.Aws;
using Raven.Server.Documents.PeriodicBackup.Azure;
using Sparrow.Utils;
using Constants = Raven.Client.Constants;

namespace Raven.Server.Web.System
{
    public class AdminDatabasesHandler : RequestHandler
    {
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
                        writer.WriteDocumentPropertiesWithoutMetdata(context, new Document
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
            var name = GetQueryStringValueAndAssertIfSingleAndNotEmpty("name");
            var node = GetStringQueryString("node", false);

            string errorMessage;
            if (ResourceNameValidator.IsValidResourceName(name, ServerStore.Configuration.Core.DataDirectory.FullPath, out errorMessage) == false)
                throw new BadRequestException(errorMessage);

            ServerStore.EnsureNotPassive();
            TransactionOperationContext context;
            using (ServerStore.ContextPool.AllocateOperationContext(out context))
            using (context.OpenReadTransaction())
            {
                var databaseRecord = ServerStore.Cluster.ReadDatabase(context, name, out var index);
                var clusterTopology = ServerStore.GetClusterTopology(context);

                //The case where an explicit node was requested 
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
                    databaseRecord.Topology.DemotionReasons[node] = "Joined the Db-Group as a new promotable node";
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
                    databaseRecord.Topology.DemotionReasons[node] = "Joined the Db-Group as a new promotable node";
                    databaseRecord.Topology.PromotablesStatus[node] = DatabasePromotionStatus.WaitingForFirstPromotion;
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

        public bool NotUsingHttps(string url)
        {
            return url.StartsWith("https:", StringComparison.OrdinalIgnoreCase) == false;
        }

        [RavenAction("/admin/databases", "PUT", AuthorizationStatus.Operator)]
        public async Task Put()
        {
            var name = GetQueryStringValueAndAssertIfSingleAndNotEmpty("name");
            var nodeUrlsAddedTo = new List<string>();

            if (ResourceNameValidator.IsValidResourceName(name, ServerStore.Configuration.Core.DataDirectory.FullPath, out string errorMessage) == false)
                throw new BadRequestException(errorMessage);

            ServerStore.EnsureNotPassive();
            using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            {
                context.OpenReadTransaction();

                var index = GetLongFromHeaders("ETag");
                var existingDatabaseRecord = ServerStore.Cluster.ReadDatabase(context, name, out long _);

                if (index.HasValue && existingDatabaseRecord == null)
                    throw new BadRequestException($"Attempted to modify non-existing database: '{name}'");

                if (existingDatabaseRecord != null && index.HasValue == false)
                    throw new ConcurrencyException($"Database '{name}' already exists!");

                var json = context.ReadForDisk(RequestBodyStream(), name);
                var databaseRecord = JsonDeserializationCluster.DatabaseRecord(json);

                try
                {
                    DatabaseHelper.Validate(name, databaseRecord);
                }
                catch (Exception e)
                {
                    throw new BadRequestException("Database document validation failed.", e);
                }
                var clusterTopology = ServerStore.GetClusterTopology(context);

                DatabaseTopology topology;
                if (databaseRecord.Topology?.Members?.Count > 0)
                {
                    topology = databaseRecord.Topology;
                    ValidateClusterMembers(context, topology, databaseRecord);
                    foreach (var member in topology.Members)
                    {
                        nodeUrlsAddedTo.Add(clusterTopology.GetUrlFromTag(member));
                    }
                }
                else
                {
                    var factor = Math.Max(1, GetIntValueQueryString("replication-factor", required: false) ?? 0);
                    databaseRecord.Topology = topology = AssignNodesToDatabase(context, factor, name, databaseRecord.Encrypted, out nodeUrlsAddedTo);
                }
                topology.ReplicationFactor = topology.Members.Count;
                var (newIndex, _) = await ServerStore.WriteDatabaseRecordAsync(name, databaseRecord, index);

                await WaitForExecutionOnRelevantNodes(context, name, clusterTopology, databaseRecord.Topology.Members, newIndex);

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

        private async Task WaitForExecutionOnRelevantNodes(JsonOperationContext context, string database, ClusterTopology clusterTopology, List<string> members, long index)
        {
            await ServerStore.Cluster.WaitForIndexNotification(index); // first let see if we commit this in the leader
            var executors = new List<ClusterRequestExecutor>();
            var timeoutTask = TimeoutManager.WaitFor(TimeSpan.FromMilliseconds(10000));
            var waitingTasks = new List<Task>
            {
                timeoutTask
            };
            var cts = CancellationTokenSource.CreateLinkedTokenSource(ServerStore.ServerShutdown);
            try
            {
                foreach (var member in members)
                {
                    var url = clusterTopology.GetUrlFromTag(member);
                    var requester = ClusterRequestExecutor.CreateForSingleNode(url, ServerStore.RavenServer.ClusterCertificateHolder.Certificate);
                    executors.Add(requester);
                    waitingTasks.Add(requester.ExecuteAsync(new WaitForRaftIndexCommand(index), context, cts.Token));
                }

                while (true)
                {
                    var task = await Task.WhenAny(waitingTasks);
                    if (task == timeoutTask)
                        throw new TimeoutException($"Waited too long for the raft command (number {index}) to be executed on any of the relevant nodes to this command.");
                    if (task.IsCompletedSuccessfully)
                    {
                        break;
                    }
                    waitingTasks.Remove(task);
                    if (waitingTasks.Count == 1) // only the timeout task is left
                        throw new InvalidDataException($"The database '{database}' was create but is not accessible, because all of the nodes on which this database was supose to be created, had thrown an exception.", task.Exception);
                }
            }
            finally
            {
                cts.Cancel();
                foreach (var clusterRequestExecutor in executors)
                {
                    clusterRequestExecutor.Dispose();
                }
                cts.Dispose();
            }
        }

        private async Task WaitForExecutionOnSpecificNode(TransactionOperationContext context, ClusterTopology clusterTopology, string node, long index)
        {
            await ServerStore.Cluster.WaitForIndexNotification(index); // first let see if we commit this in the leader

            using (var requester = ClusterRequestExecutor.CreateForSingleNode(clusterTopology.GetUrlFromTag(node), ServerStore.RavenServer.ClusterCertificateHolder.Certificate))
            {
                await requester.ExecuteAsync(new WaitForRaftIndexCommand(index), context);
            }
        }

        private DatabaseTopology AssignNodesToDatabase(
            TransactionOperationContext context,
            int factor,
            string name,
            bool isEncrypted,
            out List<string> nodeUrlsAddedTo)
        {
            var topology = new DatabaseTopology();

            var clusterTopology = ServerStore.GetClusterTopology(context);

            var allNodes = clusterTopology.Members.Keys
                .Concat(clusterTopology.Promotables.Keys)
                .Concat(clusterTopology.Watchers.Keys)
                .ToList();

            if (isEncrypted)
            {
                allNodes.RemoveAll(n => NotUsingHttps(clusterTopology.GetUrlFromTag(n)));
                if (allNodes.Count == 0)
                    throw new InvalidOperationException($"Database {name} is encrypted and requires a node which supports SSL. There is no such node available in the cluster.");
            }

            var offset = new Random().Next();
            nodeUrlsAddedTo = new List<string>();

            for (int i = 0; i < Math.Min(allNodes.Count, factor); i++)
            {
                var selectedNode = allNodes[(i + offset) % allNodes.Count];
                var url = clusterTopology.GetUrlFromTag(selectedNode);
                topology.Members.Add(selectedNode);
                nodeUrlsAddedTo.Add(url);
            }

            return topology;
        }

        private void ValidateClusterMembers(TransactionOperationContext context, DatabaseTopology topology, DatabaseRecord databaseRecord)
        {
            var clusterTopology = ServerStore.GetClusterTopology(context);

            foreach (var node in topology.AllNodes)
            {
                var url = clusterTopology.GetUrlFromTag(node);
                if (databaseRecord.Encrypted && NotUsingHttps(url))
                    throw new InvalidOperationException($"{databaseRecord.DatabaseName} is encrypted but node {node} with url {url} doesn't use HTTPS. This is not allowed.");
            }
        }

        [RavenAction("/admin/expiration/config", "POST", AuthorizationStatus.DatabaseAdmin)]
        public async Task ConfigExpiration()
        {
            await DatabaseConfigurations(ServerStore.ModifyDatabaseExpiration, "read-expiration-config");
        }

        [RavenAction("/admin/revisions/config", "POST", AuthorizationStatus.DatabaseAdmin)]
        public async Task ConfigRevisions()
        {
            await DatabaseConfigurations(ServerStore.ModifyDatabaseRevisions, "read-revisions-config");
        }

        [RavenAction("/admin/periodic-backup", "POST", AuthorizationStatus.DatabaseAdmin)]
        public async Task UpdatePeriodicBackup()
        {
            await DatabaseConfigurations(ServerStore.ModifyPeriodicBackup,
                "update-periodic-backup",
                beforeSetupConfiguration: readerObject =>
                {
                    readerObject.TryGet(
                        nameof(PeriodicBackupConfiguration.FullBackupFrequency),
                        out string fullBackupFrequency);
                    readerObject.TryGet(
                        nameof(PeriodicBackupConfiguration.IncrementalBackupFrequency),
                        out string incrementalBackupFrequency);

                    var parsedFullBackupFrequency = VerifyBackupFrequency(fullBackupFrequency);
                    var parsedIncrementalBackupFrequency = VerifyBackupFrequency(incrementalBackupFrequency);
                    if (parsedFullBackupFrequency == null &&
                        parsedIncrementalBackupFrequency == null)
                    {
                        throw new ArgumentException("Couldn't parse the cron expressions for both full and incremental backups. " +
                                                    $"full backup cron expression: {fullBackupFrequency}, " +
                                                    $"incremental backup cron expression: {incrementalBackupFrequency}");
                    }

                    readerObject.TryGet(nameof(PeriodicBackupConfiguration.LocalSettings),
                        out BlittableJsonReaderObject localSettings);

                    if (localSettings == null)
                        return;

                    localSettings.TryGet(nameof(LocalSettings.Disabled), out bool disabled);
                    if (disabled)
                        return;

                    localSettings.TryGet(nameof(LocalSettings.FolderPath), out string folderPath);
                    if (string.IsNullOrWhiteSpace(folderPath))
                        throw new ArgumentException("Backup directory cannot be null or empty");

                    var originalFolderPath = folderPath;
                    while (true)
                    {
                        var directoryInfo = new DirectoryInfo(folderPath);
                        if (directoryInfo.Exists == false)
                        {
                            if (directoryInfo.Parent == null)
                                throw new ArgumentException($"Path {originalFolderPath} cannot be accessed " +
                                                            $"because '{folderPath}' doesn't exist");
                            folderPath = directoryInfo.Parent.FullName;
                            continue;
                        }

                        if (directoryInfo.Attributes.HasFlag(FileAttributes.ReadOnly))
                            throw new ArgumentException($"Cannot write to directory path: {originalFolderPath}");

                        break;
                    }
                },
                fillJson: (json, readerObject, index) =>
                {
                    var taskIdName = nameof(PeriodicBackupConfiguration.TaskId);
                    readerObject.TryGet(taskIdName, out long taskId);
                    if (taskId == 0)
                        taskId = index;
                    json[taskIdName] = taskId;
                });
        }

        private static CrontabSchedule VerifyBackupFrequency(string backupFrequency)
        {
            if (string.IsNullOrWhiteSpace(backupFrequency))
                return null;

            return CrontabSchedule.Parse(backupFrequency);
        }

        [RavenAction("/admin/periodic-backup/test-credentials", "POST", AuthorizationStatus.DatabaseAdmin)]
        public async Task TestPerioidicBackupCredentials()
        {
            // here we explictily don't care what db I'm an admin of, since it is just a test endpoint

            var type = GetQueryStringValueAndAssertIfSingleAndNotEmpty("type");

            if (Enum.TryParse(type, out PeriodicBackupTestConnectionType connectionType) == false)
                throw new ArgumentException($"Unkown backup connection: {type}");

            DynamicJsonValue result;
            
            using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            {
                try {
                var connectionInfo = await context.ReadForMemoryAsync(RequestBodyStream(), "test-connection");
                switch (connectionType)
                {
                    case PeriodicBackupTestConnectionType.S3:
                        var s3Settings = JsonDeserializationClient.S3Settings(connectionInfo);
                        using (var awsClient = new RavenAwsS3Client(
                            s3Settings.AwsAccessKey, s3Settings.AwsSecretKey, s3Settings.BucketName,
                            s3Settings.AwsRegionName, cancellationToken: ServerStore.ServerShutdown))
                        {
                            await awsClient.TestConnection();
                        }
                        break;
                    case PeriodicBackupTestConnectionType.Glacier:
                        var glacierSettings = JsonDeserializationClient.GlacierSettings(connectionInfo);
                        using (var galcierClient = new RavenAwsGlacierClient(
                            glacierSettings.AwsAccessKey, glacierSettings.AwsSecretKey,
                            glacierSettings.AwsRegionName, glacierSettings.VaultName,
                            cancellationToken: ServerStore.ServerShutdown))
                        {
                            await galcierClient.TestConnection();
                        }
                        break;
                    case PeriodicBackupTestConnectionType.Azure:
                        var azureSettings = JsonDeserializationClient.AzureSettings(connectionInfo);
                        using (var azureClient = new RavenAzureClient(
                            azureSettings.AccountName, azureSettings.AccountKey,
                            azureSettings.StorageContainer, cancellationToken: ServerStore.ServerShutdown))
                        {
                            await azureClient.TestConnection();
                        }
                        break;
                    case PeriodicBackupTestConnectionType.FTP:
                        var ftpSettings = JsonDeserializationClient.FtpSettings(connectionInfo);
                        using (var ftpClient = new RavenFtpClient(ftpSettings.Url, ftpSettings.Port, ftpSettings.UserName,
                            ftpSettings.Password, ftpSettings.CertificateAsBase64, ftpSettings.CertificateFileName))
                        {
                            await ftpClient.TestConnection();
                        }
                        break;
                    case PeriodicBackupTestConnectionType.Local:
                    case PeriodicBackupTestConnectionType.None:
                    default:
                        throw new ArgumentOutOfRangeException();
                }
                    
                    result = new DynamicJsonValue
                    {
                        [nameof(NodeConnectionTestResult.Success)] = true,
                    };
            }
                catch (Exception e)
                {
                    result = new DynamicJsonValue
                    {
                        [nameof(NodeConnectionTestResult.Success)] = false,
                        [nameof(NodeConnectionTestResult.Error)] = e.ToString()
                    };
                }

                using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
                {
                    context.Write(writer, result);
        }
            }
        }

        [RavenAction("/admin/get-restore-points", "POST", AuthorizationStatus.Operator)]
        public async Task GetRestorePoints()
        {
            using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            {
                var restorePathBlittable = await context.ReadForMemoryAsync(RequestBodyStream(), "database-restore-path");
                var restorePathJson = JsonDeserializationServer.DatabaseRestorePath(restorePathBlittable);

                var restorePoints = new RestorePoints();

                var directories = Directory.GetDirectories(restorePathJson.Path).OrderBy(x => x).ToList();
                if (directories.Count == 0)
                {
                    // no folders in directory
                    // will scan the directory for backup files
                    Restore.FetchRestorePoints(restorePathJson.Path, restorePoints.List);
                }

                foreach (var directory in directories)
                {
                    Restore.FetchRestorePoints(directory, restorePoints.List);
                }

                using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
                {
                    var blittable = EntityToBlittable.ConvertEntityToBlittable(restorePoints, DocumentConventions.Default, context);
                    context.Write(writer, blittable);
                    writer.Flush();
                }
            }
        }

        [RavenAction("/admin/database-restore", "POST", AuthorizationStatus.Operator)]
        public async Task RestoreDatabase()
        {
            // we don't dispose this as operation is async
            var returnContextToPool = ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context);

            try
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
                var token = new OperationCancelToken(ServerStore.ServerShutdown);
                var restoreBackupTask = new RestoreBackupTask(
                    ServerStore,
                    restoreConfigurationJson,
                    context,
                    ServerStore.NodeTag,
                    token.Token);

                var task = ServerStore.Operations.AddOperation(
                    null,
                    "Restoring database: " + databaseName,
                    Documents.Operations.Operations.OperationType.DatabaseRestore,
                    taskFactory: onProgress => Task.Run(async () => await restoreBackupTask.Execute(onProgress), token.Token),
                    id: operationId, token: token);

#pragma warning disable 4014
                task.ContinueWith(_ =>
#pragma warning restore 4014
                {
                    using (returnContextToPool)
                    using (token)
                    { }
                });

                using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
                {
                    writer.WriteOperationId(context, operationId);
                }
            }
            catch (Exception)
            {
                returnContextToPool.Dispose();
                throw;
            }
        }

        private async Task DatabaseConfigurations(Func<TransactionOperationContext, string,
            BlittableJsonReaderObject, Task<(long, object)>> setupConfigurationFunc,
            string debug,
            Action<BlittableJsonReaderObject> beforeSetupConfiguration = null,
            Action<DynamicJsonValue, BlittableJsonReaderObject, long> fillJson = null)
        {
            var name = GetQueryStringValueAndAssertIfSingleAndNotEmpty("name");

            if (TryGetAllowedDbs(name, out var _, requireAdmin: true) == false)
                return;

            if (ResourceNameValidator.IsValidResourceName(name, ServerStore.Configuration.Core.DataDirectory.FullPath, out string errorMessage) == false)
                throw new BadRequestException(errorMessage);


            ServerStore.EnsureNotPassive();
            using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            {
                var configurationJson = await context.ReadForMemoryAsync(RequestBodyStream(), debug);
                beforeSetupConfiguration?.Invoke(configurationJson);

                var (index, _) = await setupConfigurationFunc(context, name, configurationJson);
                DatabaseRecord dbRecord;
                using (context.OpenReadTransaction())
                {
                    //TODO: maybe have a timeout here for long loading operations
                    dbRecord = ServerStore.Cluster.ReadDatabase(context, name);
                }
                if (dbRecord.Topology.RelevantFor(ServerStore.NodeTag))
                {
                    var db = await ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(name);
                    await db.RachisLogIndexNotifications.WaitForIndexNotification(index);
                }
                else
                {
                    await ServerStore.Cluster.WaitForIndexNotification(index);
                }
                HttpContext.Response.StatusCode = (int)HttpStatusCode.OK;

                using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
                {
                    var json = new DynamicJsonValue
                    {
                        ["RaftCommandIndex"] = index
                    };
                    fillJson?.Invoke(json, configurationJson, index);
                    context.Write(writer, json);
                    writer.Flush();
                }
            }
        }

        }
        [RavenAction("/admin/databases", "DELETE", AuthorizationStatus.Operator)]
        public async Task Delete()
        {
            var names = GetStringValuesQueryString("name");
            var fromNodes = GetStringValuesQueryString("from-node", required: false);
            var isHardDelete = GetBoolValueQueryString("hard-delete", required: false) ?? false;
            var confirmationTimeoutInSec = GetIntValueQueryString("confirmationTimeoutInSec", required: false) ?? 15;

            var waitOnRecordDeletion = new List<string>();
            using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            {
                if (string.IsNullOrEmpty(fromNodes) == false)
                {
                    using (context.OpenReadTransaction())
                    {
                        foreach (var databaseName in names)
                        {
                            var record = ServerStore.Cluster.ReadDatabase(context, databaseName);
                            if (record == null)
                                continue;

                            foreach (var node in fromNodes)
                            {
                                if (record.Topology.RelevantFor(node) == false)
                                {
                                    throw new InvalidOperationException($"Database={databaseName} doesn't reside in node={fromNodes} so it can't be deleted from it");
                                }
                                record.Topology.RemoveFromTopology(node);
                            }

                            if (record.Topology.Count == 0)
                                waitOnRecordDeletion.Add(databaseName);
                        }
                    }
                }

                long index = -1;
                foreach (var name in names)
                {
                    var (newIndex, _) = await ServerStore.DeleteDatabaseAsync(name, isHardDelete, fromNodes);
                    index = newIndex;
                }
                await ServerStore.Cluster.WaitForIndexNotification(index);

                if (confirmationTimeoutInSec > 0)
                {
                    var sp = Stopwatch.StartNew();
                    var timeout = TimeSpan.FromSeconds(confirmationTimeoutInSec);
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
                        var remaining = timeout - sp.Elapsed;
                        try
                        {
                            if (remaining < TimeSpan.Zero)
                            {
                                databaseIndex++;
                                continue; // we are done waiting, but still want to locally check the rest of the dbs
                            }

                            await ServerStore.Cluster.WaitForIndexNotification(index, remaining);
                        }
                        catch (TimeoutException)
                        {
                            databaseIndex++;
                        }
                    }

                    HttpContext.Response.StatusCode = (int)HttpStatusCode.OK;

                    using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
                    {
                        context.Write(writer, new DynamicJsonValue
                        {
                            [nameof(DeleteDatabaseResult.RaftCommandIndex)] = index,
                            [nameof(DeleteDatabaseResult.PendingDeletes)] = new DynamicJsonArray(waitOnRecordDeletion)
                        });
                        writer.Flush();
                    }
                }
            }
        }

        [RavenAction("/admin/databases/disable", "POST", AuthorizationStatus.Operator)]
        public async Task DisableDatabases()
        {
            await ToggleDisableDatabases(disableRequested: true);
        }

        [RavenAction("/admin/databases/enable", "POST", AuthorizationStatus.Operator)]
        public async Task EnableDatabases()
        {
            await ToggleDisableDatabases(disableRequested: false);
        }

        [RavenAction("/admin/databases/dynamic-node-distribution", "POST", AuthorizationStatus.Operator)]
        public async Task ToggleDynamicNodeDistribution()
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

                databaseRecord.Topology.DynamicNodesDistribution = enable;

                var (commandResultIndex, _) = await ServerStore.WriteDatabaseRecordAsync(name, databaseRecord, index);
                await ServerStore.Cluster.WaitForIndexNotification(commandResultIndex);

                NoContentStatus();
            }
        }


        private async Task ToggleDisableDatabases(bool disableRequested)
        {
            var names = GetStringValuesQueryString("name");

            using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
            {
                writer.WriteStartObject();
                writer.WritePropertyName("Status");

                writer.WriteStartArray();
                var first = true;
                foreach (var name in names)
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

                    if (databaseRecord.Disabled == disableRequested)
                    {
                        var state = disableRequested ? "disabled" : "enabled";
                        context.Write(writer, new DynamicJsonValue
                        {
                            ["Name"] = name,
                            ["Success"] = true, //even if we have nothing to do, no reason to return failure status
                            ["Disabled"] = disableRequested,
                            ["Reason"] = $"Database already {state}"
                        });
                        continue;
                    }

                    databaseRecord.Disabled = disableRequested;

                    var (index, _) = await ServerStore.WriteDatabaseRecordAsync(name, databaseRecord, null);
                    await ServerStore.Cluster.WaitForIndexNotification(index);

                    context.Write(writer, new DynamicJsonValue
                    {
                        ["Name"] = name,
                        ["Success"] = true,
                        ["Disabled"] = disableRequested,
                        ["Reason"] = $"Database state={databaseRecord.Disabled} was propagated on the cluster"
                    });
                }

                writer.WriteEndArray();

                writer.WriteEndObject();
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

        [RavenAction("/admin/etl", "PUT", AuthorizationStatus.DatabaseAdmin)]
        public async Task AddEtl()
        {

            var id = GetLongQueryString("id", required: false);

            if (id == null)
            {
                await DatabaseConfigurations((_, databaseName, etlConfiguration) => ServerStore.AddEtl(_, databaseName, etlConfiguration), "etl-add",
                    fillJson: (json, _, index) => json[nameof(EtlConfiguration<ConnectionString>.TaskId)] = index);

                return;
            }

            await DatabaseConfigurations((_, databaseName, etlConfiguration) => ServerStore.UpdateEtl(_, databaseName, id.Value, etlConfiguration), "etl-update",
                fillJson: (json, _, index) => json[nameof(EtlConfiguration<ConnectionString>.TaskId)] = index);
        }

        [RavenAction("/admin/console", "POST", AuthorizationStatus.ClusterAdmin)]
        public async Task AdminConsole()
        {
            var name = GetStringQueryString("database", false);
            var isServerScript = GetBoolValueQueryString("server-script", false) ?? false;
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
                    throw new InvalidOperationException("'database' query string parmater not found, and 'server-script' query string is not found. Don't know what to apply this script on");
                }

                HttpContext.Response.StatusCode = (int)HttpStatusCode.OK;
                using (var textWriter = new StreamWriter(ResponseBodyStream()))
                {
                    textWriter.Write(result);
                    await textWriter.FlushAsync();
                }
            }
        }

        [RavenAction("/admin/connection-strings", "PUT", AuthorizationStatus.DatabaseAdmin)]
        public async Task PutConnectionString()
        {
            await DatabaseConfigurations((_, databaseName, connectionString) => ServerStore.PutConnectionString(_, databaseName, connectionString), "put-connection-string");
        }

        [RavenAction("/admin/connection-strings", "DELETE", AuthorizationStatus.DatabaseAdmin)]
        public async Task RemoveConnectionString()
        {
            var dbName = GetQueryStringValueAndAssertIfSingleAndNotEmpty("name");

            if (TryGetAllowedDbs(dbName, out var _, requireAdmin: true) == false)
                return;

            if (ResourceNameValidator.IsValidResourceName(dbName, ServerStore.Configuration.Core.DataDirectory.FullPath, out string errorMessage) == false)
                throw new BadRequestException(errorMessage);

            var connectionStringName = GetQueryStringValueAndAssertIfSingleAndNotEmpty("connectionString");
            var type = GetQueryStringValueAndAssertIfSingleAndNotEmpty("type");

            ServerStore.EnsureNotPassive();

            var (index, _) = await ServerStore.RemoveConnectionString(dbName, connectionStringName, type);
            await ServerStore.Cluster.WaitForIndexNotification(index);
            HttpContext.Response.StatusCode = (int)HttpStatusCode.OK;

            using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            {
                using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
                {
                    context.Write(writer, new DynamicJsonValue
                    {
                        ["RaftCommandIndex"] = index
                    });
                    writer.Flush();
                }
            }
        }

        [RavenAction("/admin/connection-strings", "GET", AuthorizationStatus.DatabaseAdmin)]
        public Task GetConnectionStrings()
        {
            var dbName = GetQueryStringValueAndAssertIfSingleAndNotEmpty("name");
            if (ResourceNameValidator.IsValidResourceName(dbName, ServerStore.Configuration.Core.DataDirectory.FullPath, out string errorMessage) == false)
                throw new BadRequestException(errorMessage);

            if (TryGetAllowedDbs(dbName, out var allowedDbs, true) == false)
                return Task.CompletedTask;

            ServerStore.EnsureNotPassive();
            HttpContext.Response.StatusCode = (int)HttpStatusCode.OK;

            using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            {
                DatabaseRecord record;
                using (context.OpenReadTransaction())
                {
                    record = ServerStore.Cluster.ReadDatabase(context, dbName);
                }
                var ravenConnectionString = record.RavenConnectionStrings;
                var sqlConnectionstring = record.SqlConnectionStrings;

                using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
                {
                    var result = new GetConnectionStringsResult
                    {
                        RavenConnectionStrings = ravenConnectionString,
                        SqlConnectionStrings = sqlConnectionstring
                    };
                    context.Write(writer, result.ToJson());
                    writer.Flush();
                }
            }

            return Task.CompletedTask;
        }

        [RavenAction("/admin/connection-string", "GET", AuthorizationStatus.DatabaseAdmin)]
        public Task GetConnectionString()
        {
            var dbName = GetQueryStringValueAndAssertIfSingleAndNotEmpty("name");
            var connectionStringName = GetQueryStringValueAndAssertIfSingleAndNotEmpty("connectionStringName");
            var type = GetQueryStringValueAndAssertIfSingleAndNotEmpty("type");

            if (ResourceNameValidator.IsValidResourceName(dbName, ServerStore.Configuration.Core.DataDirectory.FullPath, out string errorMessage) == false)
                throw new BadRequestException(errorMessage);

            if (TryGetAllowedDbs(dbName, out var allowedDbs, true) == false)
                return Task.CompletedTask;

            ServerStore.EnsureNotPassive();
            HttpContext.Response.StatusCode = (int)HttpStatusCode.OK;

            using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            {
                DatabaseRecord record;
                using (context.OpenReadTransaction())
                {
                    record = ServerStore.Cluster.ReadDatabase(context, dbName);
                }

                if (Enum.TryParse<ConnectionStringType>(type, true, out var connectionStringType) == false)
                    throw new NotSupportedException($"Unknown connection string type: {connectionStringType}");

                ConnectionString result;
                switch (connectionStringType)
                {
                    case ConnectionStringType.Raven:
                        record.RavenConnectionStrings.TryGetValue(connectionStringName, out var ravenConnectionString);
                        result = new RavenConnectionString
                        {
                            Name = ravenConnectionString.Name,
                            Url = ravenConnectionString.Url,
                            Database = ravenConnectionString.Database
                        };
                        break;

                    case ConnectionStringType.Sql:
                        record.SqlConnectionStrings.TryGetValue(connectionStringName, out var sqlConnectionString);
                        result = new SqlConnectionString
                        {
                            Name = sqlConnectionString.Name,
                            ConnectionString = sqlConnectionString.ConnectionString
                        };
                        break;

                    default:
                        throw new NotSupportedException($"Unknown connection string type: {connectionStringType}");
                }

                using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
                {
                    context.Write(writer, result.ToJson());
                    writer.Flush();
                }
            }

            return Task.CompletedTask;
        }

        [RavenAction("/admin/update-resolver", "POST", AuthorizationStatus.DatabaseAdmin)]
        public async Task UpdateConflictResolver()
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
                            [nameof(DatabaseRecord.ConflictSolverConfig)] = databaseRecord.ConflictSolverConfig.ToJson()
                        });
                        writer.Flush();
                    }
                }
            }
        }

        [RavenAction("/admin/compact", "POST", AuthorizationStatus.Operator)]
        public Task CompactDatabase()
        {
            var name = GetQueryStringValueAndAssertIfSingleAndNotEmpty("name");
            var operationId = GetLongQueryString("operationId");

            using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            using (context.OpenReadTransaction())
            {
                var record = ServerStore.Cluster.ReadDatabase(context, name);
                if (record == null)
                    throw new InvalidOperationException($"Cannot compact database {name}, it doesn't exist.");
                if (record.Topology.RelevantFor(ServerStore.NodeTag) == false)
                    throw new InvalidOperationException($"Cannot compact database {name} on node {ServerStore.NodeTag}, because it doesn't reside on this node.");
            }

            var token = new OperationCancelToken(ServerStore.ServerShutdown);
            var compactDatabaseTask = new CompactDatabaseTask(
                ServerStore,
                name,
                token.Token);

            ServerStore.Operations.AddOperation(
                null,
                "Compacting database: " + name,
                Documents.Operations.Operations.OperationType.DatabaseCompact,
                taskFactory: onProgress => Task.Run(async () =>
                {
                    using (token)
                        return await compactDatabaseTask.Execute(onProgress);
                }, token.Token),
                id: operationId, token: token);

            using (ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext context))
            using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
            {
                writer.WriteOperationId(context, operationId);
            }

            return Task.CompletedTask;
        }
    }
}

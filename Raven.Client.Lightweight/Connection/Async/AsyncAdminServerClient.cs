// -----------------------------------------------------------------------
//  <copyright file="AdminAsyncServerClient.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Threading;
using System.Threading.Tasks;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Util;
using Raven.Client.Document;
using Raven.Client.Extensions;
using Raven.Client.Indexes;
using Raven.Imports.Newtonsoft.Json;
using Raven.Json.Linq;

namespace Raven.Client.Connection.Async
{
    public class AsyncAdminServerClient : IAsyncAdminDatabaseCommands, IAsyncGlobalAdminDatabaseCommands
    {
        internal readonly AsyncServerClient innerAsyncServerClient;
        private readonly AdminRequestCreator adminRequest;

        public AsyncAdminServerClient(AsyncServerClient asyncServerClient)
        {
            innerAsyncServerClient = asyncServerClient;
            adminRequest =
                new AdminRequestCreator((url, method) => innerAsyncServerClient.ForSystemDatabase().CreateRequest(url, method),
                                        (currentServerUrl, requestUrl, method) => innerAsyncServerClient.CreateReplicationAwareRequest(currentServerUrl, requestUrl, method));
        }

        public async Task CreateDatabaseAsync(DatabaseDocument databaseDocument, CancellationToken token = default (CancellationToken))
        {
            RavenJObject doc;
            using (var req = adminRequest.CreateDatabase(databaseDocument, out doc))
            {
                await req.WriteAsync(doc.ToString(Formatting.Indented)).WithCancellation(token).ConfigureAwait(false);
            }
        }

        public async Task DeleteDatabaseAsync(string databaseName, bool hardDelete = false, CancellationToken token = default (CancellationToken))
        {
            using (var req = adminRequest.DeleteDatabase(databaseName, hardDelete))
            {
                await req.ExecuteRequestAsync().WithCancellation(token).ConfigureAwait(false);
            }
        }

        public async Task<Operation> CompactDatabaseAsync(string databaseName, CancellationToken token = default (CancellationToken))
        {
            using (var req = adminRequest.CompactDatabase(databaseName))
            {
                var json = await req.ReadResponseJsonAsync().WithCancellation(token).ConfigureAwait(false);
                return new Operation((AsyncServerClient)innerAsyncServerClient.ForSystemDatabase(), json.Value<long>("OperationId"));
            }
        }

        public Task StopIndexingAsync(CancellationToken token = default (CancellationToken))
        {
            return innerAsyncServerClient.ExecuteWithReplication(HttpMethods.Post, async (operationMetadata, requestTimeMetric) =>
            {
                using (var req = adminRequest.StopIndexing(operationMetadata.Url))
                {
                    await req.ExecuteRequestAsync().WithCancellation(token).ConfigureAwait(false);
                }
            }, token);
        }

        public Task StartIndexingAsync(int? maxNumberOfParallelIndexTasks = null, CancellationToken token = default (CancellationToken))
        {
            return innerAsyncServerClient.ExecuteWithReplication(HttpMethods.Post, async (operationMetadata, requestTimeMetric) =>
            {
                using (var req = adminRequest.StartIndexing(operationMetadata.Url, maxNumberOfParallelIndexTasks))
                {
                    await req.ExecuteRequestAsync().WithCancellation(token).ConfigureAwait(false);
                }
            }, token);
        }

        public Task<BuildNumber> GetBuildNumberAsync(CancellationToken token = default (CancellationToken))
        {
            return innerAsyncServerClient.GetBuildNumberAsync(token);
        }

        public Task<string[]> GetDatabaseNamesAsync(int pageSize, int start = 0, CancellationToken token = default (CancellationToken))
        {
            return adminRequest.GetDatabaseNamesAsync(pageSize, start, token);
        }

        public async Task<AdminStatistics> GetStatisticsAsync(CancellationToken token = default (CancellationToken))
        {
            using (var req = adminRequest.AdminStats())
            {
                var json = (RavenJObject)await req.ReadResponseJsonAsync().WithCancellation(token).ConfigureAwait(false);
                return json.Deserialize<AdminStatistics>(innerAsyncServerClient.convention);
            }
        }

        public async Task<Operation> StartBackupAsync(string backupLocation, DatabaseDocument databaseDocument, bool incremental, string databaseName, CancellationToken token = default (CancellationToken))
        {
            using (var request = adminRequest.StartBackup(backupLocation, databaseDocument, databaseName, incremental))
            {
                await request.WriteAsync(RavenJObject.FromObject(new DatabaseBackupRequest
                {
                    BackupLocation = backupLocation,
                    DatabaseDocument = databaseDocument
                })).WithCancellation(token).ConfigureAwait(false);

                var jsonResponse = await request.ReadResponseJsonAsync().WithCancellation(token).ConfigureAwait(false);

                return new Operation((AsyncServerClient)innerAsyncServerClient.ForSystemDatabase(), jsonResponse.Value<long>("OperationId"));
            }
        }

        public async Task<Operation> StartRestoreAsync(DatabaseRestoreRequest restoreRequest, CancellationToken token = default (CancellationToken))
        {
            using (var request = adminRequest.CreateRestoreRequest())
            {
                await request.WriteAsync(RavenJObject.FromObject(restoreRequest)).WithCancellation(token).ConfigureAwait(false);

                var jsonResponse = await request.ReadResponseJsonAsync().WithCancellation(token).ConfigureAwait(false);

                return new Operation((AsyncServerClient)innerAsyncServerClient.ForSystemDatabase(), jsonResponse.Value<long>("OperationId"));
            }
        }

        public Task<IndexingStatus> GetIndexingStatusAsync(CancellationToken token = default (CancellationToken))
        {
            return innerAsyncServerClient.ExecuteWithReplication(HttpMethods.Get, async (operationMetadata, requestTimeMetric) =>
            {
                using (var request = adminRequest.IndexingStatus(operationMetadata.Url))
                {
                    var result = (RavenJObject)await request.ReadResponseJsonAsync().WithCancellation(token).ConfigureAwait(false);
                    return result.Deserialize<IndexingStatus>(innerAsyncServerClient.convention);
                }
            }, token);
        }

        public Task<RavenJObject> GetDatabaseConfigurationAsync(CancellationToken token = default (CancellationToken))
        {
            return innerAsyncServerClient.ExecuteWithReplication(HttpMethods.Get, async (operationMetadata, requestTimeMetric) =>
            {
                using (var request = adminRequest.GetDatabaseConfiguration(operationMetadata.Url))
                {
                    return (RavenJObject)await request.ReadResponseJsonAsync().WithCancellation(token).ConfigureAwait(false);
                }
            }, token);
        }

        public async Task EnsureDatabaseExistsAsync(string name, bool ignoreFailures = false, CancellationToken token = default (CancellationToken))
        {
            var serverClient = (AsyncServerClient)(innerAsyncServerClient.ForSystemDatabase());

            var doc = MultiDatabase.CreateDatabaseDocument(name);

            serverClient.ForceReadFromMaster();

            try
            {
                var get = await serverClient.GetAsync(doc.Id, token).ConfigureAwait(false);
                if (get != null)
                    return;

                await serverClient.GlobalAdmin.CreateDatabaseAsync(doc, token).ConfigureAwait(false);

                try
                {
                    await new RavenDocumentsByEntityName().ExecuteAsync(serverClient.ForDatabase(name), new DocumentConvention(), token).ConfigureAwait(false);
                }
                catch (Exception)
                {
                    // this is a courtesy, not required, and can happen if we don't have permissions to the new db
                }
            }
            catch (Exception)
            {
                if (ignoreFailures == false)
                    throw;
            }
        }

        public IAsyncDatabaseCommands Commands
        {
            get
            {
                return innerAsyncServerClient;
            }
        }
    }
}

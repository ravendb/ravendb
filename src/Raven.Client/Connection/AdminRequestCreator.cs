// -----------------------------------------------------------------------
//  <copyright file="AdminDatabaseCommandsBase.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Globalization;
using System.Linq;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Util;
using Raven.Client.Connection.Implementation;
using Raven.Client.Extensions;
using Raven.Json.Linq;

namespace Raven.Client.Connection
{
    public class AdminRequestCreator
    {
        // url, method
        private readonly Func<string, HttpMethod, HttpJsonRequest> createRequestForSystemDatabase;

        // currentServerUrl, operationUrl, method
        private readonly Func<string, string, HttpMethod, HttpJsonRequest> createReplicationAwareRequest;

        public AdminRequestCreator(Func<string, HttpMethod, HttpJsonRequest> createRequestForSystemDatabase, Func<string, string, HttpMethod, HttpJsonRequest> createReplicationAwareRequest)
        {
            this.createRequestForSystemDatabase = createRequestForSystemDatabase;
            this.createReplicationAwareRequest = createReplicationAwareRequest;
        }

        public HttpJsonRequest CreateDatabase(DatabaseDocument databaseDocument, out RavenJObject doc)
        {
            MultiDatabase.AssertValidName(databaseDocument.Id);
            doc = RavenJObject.FromObject(databaseDocument);

            return createRequestForSystemDatabase("/admin/databases?name=" + Uri.EscapeDataString(databaseDocument.Id), HttpMethods.Put);
        }

        public HttpJsonRequest DeleteDatabase(string databaseName, bool hardDelete)
        {
            var deleteUrl = "/admin/databases?name=" + Uri.EscapeDataString(databaseName);

            if (hardDelete)
                deleteUrl += "&hard-delete=true";
            else
                deleteUrl += "&hard-delete=false";

            return createRequestForSystemDatabase(deleteUrl, HttpMethods.Delete);
        }

        public HttpJsonRequest StartIndex(string serverUrl, string name)
        {
            return createReplicationAwareRequest(serverUrl, "/admin/indexes/start?name=" + name, HttpMethods.Post);
        }

        public HttpJsonRequest StopIndex(string serverUrl, string name)
        {
            return createReplicationAwareRequest(serverUrl, "/admin/indexes/stop?name=" + name, HttpMethods.Post);
        }

        public HttpJsonRequest StopIndexing(string serverUrl)
        {
            return createReplicationAwareRequest(serverUrl, "/admin/indexes/stop", HttpMethods.Post);
        }

        public HttpJsonRequest StartIndexing(string serverUrl, int? maxNumberOfParallelIndexTasks)
        {
            var url = "/admin/indexes/start";
            if (maxNumberOfParallelIndexTasks.HasValue)
            {
                throw new NotImplementedException();
                // url += "?concurrency=" + maxNumberOfParallelIndexTasks.Value;
            }

            return createReplicationAwareRequest(serverUrl, url, HttpMethods.Post);
        }

        public HttpJsonRequest CompactIndex(string serverUrl, string name)
        {
            var url = $"/admin/indexes/compact?name={name}";

            return createReplicationAwareRequest(serverUrl, url, HttpMethods.Post);
        }

        public HttpJsonRequest EnableIndexAsync(string serverUrl, string name)
        {
            var url = $"/admin/indexes/enable?name={name}";

            return createReplicationAwareRequest(serverUrl, url, HttpMethods.Post);
        }

        public HttpJsonRequest DisableIndexAsync(string serverUrl, string name)
        {
            var url = $"/admin/indexes/disable?name={name}";

            return createReplicationAwareRequest(serverUrl, url, HttpMethods.Post);
        }

        public HttpJsonRequest AdminStats()
        {
            return createRequestForSystemDatabase("/admin/stats", HttpMethods.Get);
        }

        public HttpJsonRequest StartBackup(string backupLocation, DatabaseDocument databaseDocument, string databaseName, bool incremental)
        {
            if (databaseName == Constants.SystemDatabase)
            {
                return createRequestForSystemDatabase("/admin/backup", HttpMethods.Post);
            }
            return createRequestForSystemDatabase("/databases/" + databaseName + "/admin/backup?incremental=" + incremental, HttpMethods.Post);

        }

        public HttpJsonRequest CreateRestoreRequest()
        {
            return createRequestForSystemDatabase("/admin/restore", HttpMethods.Post);
        }

        public HttpJsonRequest CreatePutApiKeyRequest(string name)
        {
            return createRequestForSystemDatabase("/admin/api-keys?name=" + name, HttpMethods.Put);
        }

        public HttpJsonRequest CreateGetApiKeyRequest(string name)
        {
            return createRequestForSystemDatabase("/admin/api-keys?name=" + name, HttpMethods.Get);
        }

        public HttpJsonRequest CreateDeleteApiKeyRequest(string name)
        {
            return createRequestForSystemDatabase("/admin/api-keys?name=" + name, HttpMethods.Delete);
        }

        public HttpJsonRequest CreateGetAllApiKeyRequest(int start, int pageSize)
        {
            return createRequestForSystemDatabase($"/admin/api-keys?start={start}&pageSize={pageSize}", HttpMethods.Get);
        }

        public HttpJsonRequest IndexesStatus(string serverUrl)
        {
            return createReplicationAwareRequest(serverUrl, "/indexes/status", HttpMethods.Get);
        }

        public HttpJsonRequest CompactDatabase(string databaseName)
        {
            return createRequestForSystemDatabase("/admin/compact?database=" + databaseName, HttpMethods.Post);
        }

        public HttpJsonRequest GetDatabaseConfiguration(string serverUrl)
        {
            return createReplicationAwareRequest(serverUrl, "/debug/config", HttpMethods.Get);
        }

        /// <summary>
        /// Gets the list of databases from the server asynchronously
        /// </summary>
        public async Task<string[]> GetDatabaseNamesAsync(int pageSize, int start = 0, CancellationToken token = default(CancellationToken))
        {
            using (var requestForSystemDatabase = createRequestForSystemDatabase(string.Format(CultureInfo.InvariantCulture, "/resources?namesOnly=true&pageSize={0}&start={1}", pageSize, start), HttpMethods.Get))
            {
                var json = (RavenJObject)await requestForSystemDatabase.ReadResponseJsonAsync().WithCancellation(token).ConfigureAwait(false);
                var array = json.Value<RavenJArray>("Databases");

                return array
                    .Select(x => x.Value<string>())
                    .ToArray();
            }
        }

        public async Task<BuildNumber> GetBuildNumberAsync(CancellationToken token = default(CancellationToken))
        {
            using (var requestForSystemDatabase = createRequestForSystemDatabase("/build/version", HttpMethods.Get))
            {
                var json = await requestForSystemDatabase.ReadResponseJsonAsync().WithCancellation(token).ConfigureAwait(false);
                return json.JsonDeserialization<BuildNumber>();
            }
        }
    }
}

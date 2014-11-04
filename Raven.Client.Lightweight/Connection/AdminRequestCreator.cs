// -----------------------------------------------------------------------
//  <copyright file="AdminDatabaseCommandsBase.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Globalization;
using System.Linq;
using System.Threading.Tasks;
using Raven.Abstractions.Data;
using Raven.Client.Extensions;
using Raven.Json.Linq;

namespace Raven.Client.Connection
{
	public class AdminRequestCreator
	{
		// url, method
		private readonly Func<string, string, HttpJsonRequest> createRequestForSystemDatabase;
		private readonly Func<string, string, HttpJsonRequest> createRequest;

		// currentServerUrl, operationUrl, method
		private readonly Func<string, string, string, HttpJsonRequest> createReplicationAwareRequest;

		public AdminRequestCreator(Func<string, string, HttpJsonRequest> createRequestForSystemDatabase, Func<string, string, HttpJsonRequest> createRequest, Func<string, string, string, HttpJsonRequest> createReplicationAwareRequest)
		{
			this.createRequestForSystemDatabase = createRequestForSystemDatabase;
			this.createRequest = createRequest;
			this.createReplicationAwareRequest = createReplicationAwareRequest;
		}

		public HttpJsonRequest CreateDatabase(DatabaseDocument databaseDocument, out RavenJObject doc)
		{
			if (databaseDocument.Settings.ContainsKey("Raven/DataDir") == false)
				throw new InvalidOperationException("The Raven/DataDir setting is mandatory");
            if (databaseDocument.Settings.ContainsKey(Constants.IndexingDisabled) == false)
		        databaseDocument.Settings[Constants.IndexingDisabled] = "false";
			var dbname = databaseDocument.Id.Replace("Raven/Databases/", "");
            MultiDatabase.AssertValidName(dbname);
			doc = RavenJObject.FromObject(databaseDocument);
			doc.Remove("Id");

			return createRequestForSystemDatabase("/admin/databases/" + Uri.EscapeDataString(dbname), "PUT");
		}

		public HttpJsonRequest DeleteDatabase(string databaseName, bool hardDelete)
		{
			var deleteUrl = "/admin/databases/" + Uri.EscapeDataString(databaseName);

			if(hardDelete)
				deleteUrl += "?hard-delete=true";

			return createRequestForSystemDatabase(deleteUrl, "DELETE");
		}

		public HttpJsonRequest StopIndexing(string serverUrl)
		{
			return createReplicationAwareRequest(serverUrl, "/admin/StopIndexing", "POST");
		}

        public HttpJsonRequest StartIndexing(string serverUrl, int? maxNumberOfParallelIndexTasks)
        {
            var url = "/admin/StartIndexing";
            if (maxNumberOfParallelIndexTasks.HasValue)
            {
                url += "?concurrency=" + maxNumberOfParallelIndexTasks.Value;
            }

			return createReplicationAwareRequest(serverUrl, url, "POST");
		}

		public HttpJsonRequest AdminStats()
		{
			return createRequestForSystemDatabase("/admin/stats", "GET");
		}

		public HttpJsonRequest StartBackup(string backupLocation, DatabaseDocument databaseDocument, string databaseName, bool incremental)
		{
            if (databaseName == Constants.SystemDatabase)
            {
                return createRequestForSystemDatabase("/admin/backup", "POST");
            }
            return createRequestForSystemDatabase("/databases/" + databaseName + "/admin/backup?incremental=" + incremental, "POST");
            
		}

		public HttpJsonRequest CreateRestoreRequest()
		{
			return createRequestForSystemDatabase("/admin/restore", "POST");
		}

		public HttpJsonRequest IndexingStatus(string serverUrl)
		{
			return createReplicationAwareRequest(serverUrl, "/admin/IndexingStatus", "GET");
		}

		public HttpJsonRequest CompactDatabase(string databaseName)
		{
			return createRequestForSystemDatabase("/admin/compact?database=" + databaseName, "POST");
		}

		public HttpJsonRequest GetDatabaseConfiguration(string serverUrl)
		{
			return createReplicationAwareRequest(serverUrl, "/debug/config", "GET");
		}

        /// <summary>
        /// Gets the list of databases from the server asynchronously
        /// </summary>
        public async Task<string[]> GetDatabaseNamesAsync(int pageSize, int start = 0)
        {
            var requestForSystemDatabase = createRequestForSystemDatabase(string.Format(CultureInfo.InvariantCulture,"/databases?pageSize={0}&start={1}", pageSize, start), "GET");
            var result = await requestForSystemDatabase.ReadResponseJsonAsync();
            var json = (RavenJArray)result;
            return json.Select(x => x.ToString())
                .ToArray();
        }
	}
}

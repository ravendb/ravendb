// -----------------------------------------------------------------------
//  <copyright file="AdminDatabaseCommandsBase.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
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

		public HttpJsonRequest StartIndexing(string serverUrl)
		{
			return createReplicationAwareRequest(serverUrl, "/admin/StartIndexing", "POST");
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
	}
}

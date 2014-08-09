
// -----------------------------------------------------------------------
//  <copyright file="AdminDatabaseCommands.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using Raven.Abstractions.Data;
using Raven.Client.Connection.Async;
using Raven.Client.Extensions;
using Raven.Json.Linq;

namespace Raven.Client.Connection
{
	public class AdminServerClient : IAdminDatabaseCommands, IGlobalAdminDatabaseCommands
	{
		private readonly AsyncServerClient asyncServerClient;
		private readonly AsyncAdminServerClient asyncAdminServerClient;

		public AdminServerClient(AsyncServerClient asyncServerClient, AsyncAdminServerClient asyncAdminServerClient)
		{
			this.asyncServerClient = asyncServerClient;
			this.asyncAdminServerClient = asyncAdminServerClient;
		}

		public void CreateDatabase(DatabaseDocument databaseDocument)
		{
			asyncAdminServerClient.CreateDatabaseAsync(databaseDocument).WaitUnwrap();
		}

		public void DeleteDatabase(string databaseName, bool hardDelete = false)
		{
			asyncAdminServerClient.DeleteDatabaseAsync(databaseName, hardDelete).WaitUnwrap();
		}

		public IDatabaseCommands Commands { get { return new ServerClient(asyncServerClient); } }

		public void CompactDatabase(string databaseName)
		{
			asyncAdminServerClient.CompactDatabaseAsync(databaseName).WaitUnwrap();
		}

		public void StopIndexing()
		{
			asyncAdminServerClient.StopIndexingAsync().WaitUnwrap();
		}

        public void StartIndexing(int? maxNumberOfParallelIndexTasks)
		{
            asyncAdminServerClient.StartIndexingAsync(maxNumberOfParallelIndexTasks).WaitUnwrap();
		}

		public void StartBackup(string backupLocation, DatabaseDocument databaseDocument, bool incremental, string databaseName)
		{
            asyncAdminServerClient.StartBackupAsync(backupLocation, databaseDocument, incremental, databaseName).WaitUnwrap();
		}

		public void StartRestore(RestoreRequest restoreRequest)
		{
			asyncAdminServerClient.StartRestoreAsync(restoreRequest).WaitUnwrap();
		}

		public string GetIndexingStatus()
		{
			return asyncAdminServerClient.GetIndexingStatusAsync().ResultUnwrap();
		}

		public RavenJObject GetDatabaseConfiguration()
		{
			return asyncAdminServerClient.GetDatabaseConfigurationAsync().ResultUnwrap();
		}

		public BuildNumber GetBuildNumber()
		{
			return asyncAdminServerClient.GetBuildNumberAsync().ResultUnwrap();
		}

		public string[] GetDatabaseNames(int pageSize, int start = 0)
		{
			return asyncAdminServerClient.GetDatabaseNamesAsync(pageSize, start).ResultUnwrap();
		}

		public AdminStatistics GetStatistics()
		{
			return asyncAdminServerClient.GetStatisticsAsync().ResultUnwrap();
		}
	}
}

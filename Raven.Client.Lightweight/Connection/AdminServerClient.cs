
#if !SILVERLIGHT && !NETFX_CORE
// -----------------------------------------------------------------------
//  <copyright file="AdminDatabaseCommands.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using Raven.Abstractions.Data;
using Raven.Client.Connection.Async;
using Raven.Client.Extensions;

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
			asyncAdminServerClient.DeleteDatabaseAsync(databaseName, hardDelete);
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

		public void StartIndexing()
		{
			asyncAdminServerClient.StartIndexingAsync().WaitUnwrap();
		}

		public void StartBackup(string backupLocation, DatabaseDocument databaseDocument, string databaseName)
		{
			asyncAdminServerClient.StartBackupAsync(backupLocation, databaseDocument, databaseName).WaitUnwrap();
		}

		public void StartRestore(string restoreLocation, string databaseLocation, string databaseName = null, bool defrag = false)
		{
			asyncAdminServerClient.StartRestoreAsync(restoreLocation, databaseLocation, databaseName, defrag);
		}

		public string GetIndexingStatus()
		{
			return asyncAdminServerClient.GetIndexingStatusAsync().Result;
		}

		public AdminStatistics GetStatistics()
		{
			return asyncAdminServerClient.GetStatisticsAsync().Result;
		}
	}
}
#endif

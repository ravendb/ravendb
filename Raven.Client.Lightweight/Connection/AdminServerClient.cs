#if !SILVERLIGHT && !NETFX_CORE
// -----------------------------------------------------------------------
//  <copyright file="AdminDatabaseCommands.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using Raven.Abstractions.Data;

namespace Raven.Client.Connection
{
    using Raven.Client.Connection.Async;

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
            asyncAdminServerClient.CreateDatabaseAsync(databaseDocument).Wait();
        }

        public void DeleteDatabase(string databaseName, bool hardDelete = false)
        {
            asyncAdminServerClient.DeleteDatabaseAsync(databaseName, hardDelete);
        }

        public IDatabaseCommands Commands { get { return new ServerClient(asyncServerClient); } }

        public void CompactDatabase(string databaseName)
        {
            asyncAdminServerClient.CompactDatabaseAsync(databaseName).Wait();
        }

        public void StopIndexing()
        {
            asyncAdminServerClient.StopIndexingAsync().Wait();
        }

        public void StartIndexing()
        {
            asyncAdminServerClient.StartIndexingAsync().Wait();
        }

        public void StartBackup(string backupLocation, DatabaseDocument databaseDocument)
        {
            asyncAdminServerClient.StartBackupAsync(backupLocation, databaseDocument).Wait();
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

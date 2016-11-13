
// -----------------------------------------------------------------------
//  <copyright file="AdminDatabaseCommands.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using Raven.Abstractions.Data;
using Raven.Abstractions.FileSystem;
using Raven.Abstractions.Util;
using Raven.Client.Connection.Async;
using Raven.Client.Data;
using Raven.Client.Data.Indexes;
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
            AsyncHelpers.RunSync(() => asyncAdminServerClient.CreateDatabaseAsync(databaseDocument));
        }

        public void DeleteDatabase(string databaseName, bool hardDelete = false)
        {
            AsyncHelpers.RunSync(() => asyncAdminServerClient.DeleteDatabaseAsync(databaseName, hardDelete));
        }

        public IDatabaseCommands Commands { get { return new ServerClient(asyncServerClient); } }

        public Operation CompactDatabase(string databaseName)
        {
            return AsyncHelpers.RunSync(() => asyncAdminServerClient.CompactDatabaseAsync(databaseName));
        }

        public void StopIndexing()
        {
            AsyncHelpers.RunSync(() => asyncAdminServerClient.StopIndexingAsync());
        }

        public void StartIndex(string name)
        {
            AsyncHelpers.RunSync(() => asyncAdminServerClient.StartIndexAsync(name));
        }

        public void StopIndex(string name)
        {
            AsyncHelpers.RunSync(() => asyncAdminServerClient.StopIndexAsync(name));
        }

        public void StartIndexing(int? maxNumberOfParallelIndexTasks)
        {
            AsyncHelpers.RunSync(() => asyncAdminServerClient.StartIndexingAsync(maxNumberOfParallelIndexTasks));
        }

        public Operation CompactIndex(string name)
        {
            return AsyncHelpers.RunSync(() => asyncAdminServerClient.CompactIndexAsync(name));
        }

        public void StartBackup(string backupLocation, DatabaseDocument databaseDocument, bool incremental, string databaseName)
        {
            AsyncHelpers.RunSync(() => asyncAdminServerClient.StartBackupAsync(backupLocation, databaseDocument, incremental, databaseName));
        }

        public Operation StartRestore(DatabaseRestoreRequest restoreRequest)
        {
            return AsyncHelpers.RunSync(() => asyncAdminServerClient.StartRestoreAsync(restoreRequest));
        }

        public IndexingStatus GetIndexingStatus()
        {
            return AsyncHelpers.RunSync(() => asyncAdminServerClient.GetIndexingStatusAsync());
        }

        public RavenJObject GetDatabaseConfiguration()
        {
            return AsyncHelpers.RunSync(() => asyncAdminServerClient.GetDatabaseConfigurationAsync());
        }

        public BuildNumber GetBuildNumber()
        {
            return AsyncHelpers.RunSync(() => asyncAdminServerClient.GetBuildNumberAsync());
        }

        public string[] GetDatabaseNames(int pageSize, int start = 0)
        {
            return AsyncHelpers.RunSync(() => asyncAdminServerClient.GetDatabaseNamesAsync(pageSize, start));
        }

        public AdminStatistics GetStatistics()
        {
            return AsyncHelpers.RunSync(() => asyncAdminServerClient.GetStatisticsAsync());
        }

        public ApiKeyDefinition GetApiKey(string name)
        {
            return AsyncHelpers.RunSync(() => asyncAdminServerClient.GetApiKeyAsync(name));
        }

        public void PutApiKey(string name, ApiKeyDefinition apiKeyData)
        {
            AsyncHelpers.RunSync(() => asyncAdminServerClient.PutApiKeyAsync(name, apiKeyData));
        }

        public void DeleteApiKey(string name)
        {
            AsyncHelpers.RunSync(() => asyncAdminServerClient.DeleteApiKeyAsync(name));
        }

        public IEnumerable<NamedApiKeyDefinition> GetAllApiKeys()
        {
            return  AsyncHelpers.RunSync(() => asyncAdminServerClient.GetAllApiKeys());
        }
    }
}

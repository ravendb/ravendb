// -----------------------------------------------------------------------
//  <copyright file="Smuggler.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Globalization;
using System.IO;
using System.Text;
using System.Threading.Tasks;
using NDesk.Options;
using Raven.Abstractions;
using Raven.Abstractions.Connection;
using Raven.Abstractions.Data;
using Raven.Abstractions.Smuggler;
using Raven.Client.Document;
using Raven.Client.Extensions;

namespace Raven.Smuggler
{
    public static class SmugglerOp
    {
        public static async Task Between(SmugglerBetweenOptions options)
        {
            SetDatabaseNameIfEmpty(options.From);
            SetDatabaseNameIfEmpty(options.To);

            using (var exportStore = CreateStore(options.From))
            using (var importStore = CreateStore(options.To))
            {
                await EnsureDatabaseExists(importStore, options.To.DefaultDatabase);
                
                await ExportIndexes(exportStore, importStore, options.BatchSize);
            }
        }

        private static async Task EnsureDatabaseExists(DocumentStore store, string databaseName)
        {
            if (string.IsNullOrWhiteSpace(databaseName) == false)
                await store.AsyncDatabaseCommands.GlobalAdmin.EnsureDatabaseExistsAsync(databaseName);
        }

        private static void SetDatabaseNameIfEmpty(RavenConnectionStringOptions connection)
        {
            if (string.IsNullOrWhiteSpace(connection.DefaultDatabase) == false)
                return;
            
            var index = connection.Url.IndexOf("/databases/", StringComparison.OrdinalIgnoreCase);
            if (index != -1)
            {
                connection.DefaultDatabase = connection.Url.Substring(index + "/databases/".Length).Trim(new[] {'/'});
            }
        }

        private static async Task ExportIndexes(DocumentStore exportStore, DocumentStore importStore, int batchSize)
        {
            var totalCount = 0;
            while (true)
            {
                var indexes = await exportStore.AsyncDatabaseCommands.GetIndexesAsync(totalCount, batchSize);
                if (indexes.Length == 0)
                {
                    ShowProgress("Done with reading indexes, total: {0}", totalCount);
                    break;
                }
                totalCount += indexes.Length;
                ShowProgress("Reading batch of {0,3} indexes, read so far: {1,10:#,#;;0}", indexes.Length, totalCount);
                foreach (var index in indexes)
                {
                    var indexName = await importStore.AsyncDatabaseCommands.PutIndexAsync(index.Name, index, true);
                    ShowProgress("Succesfully PUT index '{0}'", indexName);
                }
            }
        }

        private static DocumentStore CreateStore(RavenConnectionStringOptions connection)
        {
            var store = new DocumentStore
            {
                Url = connection.Url,
                ApiKey = connection.ApiKey,
                Credentials = connection.Credentials,
                DefaultDatabase = connection.DefaultDatabase
            };
            store.Initialize();
            return store;
        }

        private static void ShowProgress(string format, params object[] args)
        {
            Console.WriteLine(format, args);
        }
    }
}
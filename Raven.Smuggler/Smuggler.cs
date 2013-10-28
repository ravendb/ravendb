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
            using (var importStore = CreateStore(options.To))
            {
                var databaseName = options.To.DefaultDatabase;
                if (string.IsNullOrWhiteSpace(databaseName))
                {
                    var index = options.To.Url.IndexOf("/databases/", StringComparison.OrdinalIgnoreCase);
                    if (index != -1)
                    {
                        databaseName = options.To.Url.Substring(index + "/databases/".Length).Trim(new[] {'/'});
                    }
                }
                if (string.IsNullOrWhiteSpace(databaseName) == false)
                    await importStore.AsyncDatabaseCommands.GlobalAdmin.EnsureDatabaseExistsAsync(databaseName);
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
    }
}
//-----------------------------------------------------------------------
// <copyright file="MultiTenancyExtensions.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Threading.Tasks;
using Raven.Abstractions.Data;
using Raven.Client.Connection;
using Raven.Client.Connection.Async;
using Raven.Client.Document;
using Raven.Client.Indexes;

namespace Raven.Client.Extensions
{
    ///<summary>
    /// Extension methods to create multitenant databases
    ///</summary>
    public static class MultiTenancyExtensions
    {
#if !SILVERLIGHT && !NETFX_CORE
        ///<summary>
        /// Ensures that the database exists, creating it if needed
        ///</summary>
        /// <remarks>
        /// This operation happens _outside_ of any transaction
        /// </remarks>
        public static void EnsureDatabaseExists(this IGlobalAdminDatabaseCommands self, string name, bool ignoreFailures = false)
        {
            var serverClient = self.Commands.ForSystemDatabase() as ServerClient;
            if (serverClient == null)
                throw new InvalidOperationException("Multiple databases are not supported in the embedded API currently");

            serverClient.ForceReadFromMaster();

            var doc = MultiDatabase.CreateDatabaseDocument(name);

            try
            {
                if (serverClient.Get(doc.Id) != null)
                    return;

                serverClient.GlobalAdmin.CreateDatabase(doc);
            }
            catch (Exception)
            {
                if (ignoreFailures == false)
                    throw;
            }

            try
            {
                new RavenDocumentsByEntityName().Execute(serverClient.ForDatabase(name), new DocumentConvention());
            }
            catch (Exception)
            {
                // we really don't care if this fails, and it might, if the user doesn't have permissions on the new db
            }
        }

        [Obsolete("The method was moved to be under the Admin property. Use the store.DatabaseCommands.GlobalAdmin.EnsureDatabaseExists instead.")]
        public static void EnsureDatabaseExists(this IDatabaseCommands self, string name, bool ignoreFailures = false)
        {
            self.GlobalAdmin.EnsureDatabaseExists(name, ignoreFailures);
        }

        [Obsolete("The method was moved to be under the Admin property. Use the store.DatabaseCommands.Admin.CreateDatabase instead.")]
        public static void CreateDatabase(this IDatabaseCommands self, DatabaseDocument databaseDocument)
        {
            self.GlobalAdmin.CreateDatabase(databaseDocument);
        }

#endif

        ///<summary>
        /// Ensures that the database exists, creating it if needed
        ///</summary>
        public static async Task EnsureDatabaseExistsAsync(this IAsyncGlobalAdminDatabaseCommands self, string name, bool ignoreFailures = false)
        {
            var serverClient = ((IAsyncDatabaseCommands)self).ForSystemDatabase() as AsyncServerClient;
            if (serverClient == null)
                throw new InvalidOperationException("Ensuring database existence requires a Server Client but got: " + self);

            var doc = MultiDatabase.CreateDatabaseDocument(name);

            serverClient.ForceReadFromMaster();

            var get = await serverClient.GetAsync(doc.Id).ConfigureAwait(false);
            if (get != null)
                return;

            try
            {
                await serverClient.GlobalAdmin.CreateDatabaseAsync(doc).ConfigureAwait(false);
            }
            catch (Exception)
            {
                if (ignoreFailures == false)
                    throw;
            }
            await new RavenDocumentsByEntityName().ExecuteAsync(serverClient.ForDatabase(name), new DocumentConvention()).ConfigureAwait(false);
        }

        [Obsolete("The method was moved to be under the Admin property. Use the store.DatabaseCommands.GlobalAdmin.EnsureDatabaseExists instead.")]
        public static Task EnsureDatabaseExists(this IAsyncDatabaseCommands self, string name, bool ignoreFailures = false)
        {
            return self.GlobalAdmin.EnsureDatabaseExistsAsync(name, ignoreFailures);
        }

        [Obsolete("The method was moved to be under the Admin property. Use the store.AsyncDatabaseCommands.Admin.CreateDatabaseAsync instead.")]
        public static Task CreateDatabaseAsync(this IAsyncDatabaseCommands self, DatabaseDocument databaseDocument)
        {
            return self.GlobalAdmin.CreateDatabaseAsync(databaseDocument);
        }
    }
}
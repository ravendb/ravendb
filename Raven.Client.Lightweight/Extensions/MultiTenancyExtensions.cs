//-----------------------------------------------------------------------
// <copyright file="MultiTenancyExtensions.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using Raven.Abstractions.Data;
using Raven.Client.Connection;
using Raven.Client.Document;
using Raven.Imports.Newtonsoft.Json;
using Raven.Json.Linq;
using Raven.Abstractions.Extensions;
using Raven.Client.Changes;

namespace Raven.Client.Extensions
{
	using Raven.Client.Connection.Async;
	using System.Threading.Tasks;
	using Raven.Client.Indexes;

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
		public static void EnsureDatabaseExists(this IDatabaseCommands self, string name, bool ignoreFailures = false)
		{
			var serverClient = self.ForSystemDatabase() as ServerClient;
			if (serverClient == null)
				throw new InvalidOperationException("Multiple databases are not supported in the embedded API currently");

			serverClient.ForceReadFromMaster();

			var doc = MultiDatabase.CreateDatabaseDocument(name);
			var docId = "Raven/Databases/" + name;
			try
			{
				if (serverClient.Get(docId) != null)
					return;

				var req = serverClient.CreateRequest("PUT", "/admin/databases/" + Uri.EscapeDataString(name));
				req.Write(doc.ToString(Formatting.Indented));
				req.ExecuteRequest();
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

		public static void CreateDatabase(this IDatabaseCommands self, DatabaseDocument databaseDocument)
		{
			var serverClient = self.ForSystemDatabase() as ServerClient;
			if (serverClient == null)
				throw new InvalidOperationException("Ensuring database existence requires a Server Client but got: " + self);

			if (databaseDocument.Settings.ContainsKey("Raven/DataDir") == false)
				throw new InvalidOperationException("The Raven/DataDir setting is mandatory");

			var doc = RavenJObject.FromObject(databaseDocument);
			doc.Remove("Id");

			var req = serverClient.CreateRequest("PUT", "/admin/databases/" + Uri.EscapeDataString(databaseDocument.Id));
			req.Write(doc.ToString(Formatting.Indented));
			req.ExecuteRequest();
		}
#endif

#if SILVERLIGHT || NETFX_CORE
		///<summary>
		/// Ensures that the database exists, creating it if needed
		///</summary>
		public static async Task EnsureDatabaseExistsAsync(this IAsyncDatabaseCommands self, string name, bool ignoreFailures = false)
		{
		    var serverClient = self.ForSystemDatabase() as AsyncServerClient;
		    if (serverClient == null)
		        throw new InvalidOperationException("Ensuring database existence requires a Server Client but got: " + self);

		    serverClient.ForceReadFromMaster();

		    var doc = MultiDatabase.CreateDatabaseDocument(name);
		    var docId = "Raven/Databases/" + name;

            try
            {
                if (await serverClient.GetAsync(docId) != null)
                    return;

                var req = serverClient.CreateRequest("/admin/databases/" + Uri.EscapeDataString(name), "PUT");
                await req.ExecuteWriteAsync(doc.ToString(Formatting.Indented));
            }
            catch (Exception)
            {
                if (ignoreFailures == false)
                    throw;
            }

            try
            {
                await new RavenDocumentsByEntityName().ExecuteAsync(serverClient.ForDatabase(name), new DocumentConvention());
            }
            catch (Exception)
            {
                // we really don't care if this fails, and it might, if the user doesn't have permissions on the new db
            }
		}

	    public static Task CreateDatabaseAsync(this IAsyncDatabaseCommands self, DatabaseDocument databaseDocument, bool ignoreFailures = false)
		{
			var serverClient = self.ForSystemDatabase() as AsyncServerClient;
			if (serverClient == null)
				throw new InvalidOperationException("Ensuring database existence requires a Server Client but got: " + self);

			if (databaseDocument.Settings.ContainsKey("Raven/DataDir") == false)
				throw new InvalidOperationException("The Raven/DataDir setting is mandatory");

			var doc = RavenJObject.FromObject(databaseDocument);
			doc.Remove("Id");

			var req = serverClient.CreateRequest("/admin/databases/" + Uri.EscapeDataString(databaseDocument.Id), "PUT");
			return req.ExecuteWriteAsync(doc.ToString(Formatting.Indented));
		}
#else
		///<summary>
		/// Ensures that the database exists, creating it if needed
		///</summary>
		public static async Task EnsureDatabaseExistsAsync(this IAsyncDatabaseCommands self, string name, bool ignoreFailures = false)
		{
			var serverClient = self.ForSystemDatabase() as AsyncServerClient;
			if (serverClient == null)
				throw new InvalidOperationException("Ensuring database existence requires a Server Client but got: " + self);

			var doc = MultiDatabase.CreateDatabaseDocument(name);
			var docId = "Raven/Databases/" + name;

			serverClient.ForceReadFromMaster();

            var get = await serverClient.GetAsync(docId).ConfigureAwait(false);
			if (get != null)
				return;

			var req = serverClient.CreateRequest("/admin/databases/" + Uri.EscapeDataString(name), "PUT");
			req.Write(doc.ToString(Formatting.Indented));
			try
			{
                await req.ExecuteRequestAsync().ConfigureAwait(false);
			}
			catch (Exception)
			{
				if (ignoreFailures)
					return;
			}
            await new RavenDocumentsByEntityName().ExecuteAsync(serverClient.ForDatabase(name), new DocumentConvention()).ConfigureAwait(false);
		}

		public static Task CreateDatabaseAsync(this IAsyncDatabaseCommands self, DatabaseDocument databaseDocument, bool ignoreFailures = false)
		{
			var serverClient = self.ForSystemDatabase() as AsyncServerClient;
			if (serverClient == null)
				throw new InvalidOperationException("Ensuring database existence requires a Server Client but got: " + self);

			if (databaseDocument.Settings.ContainsKey("Raven/DataDir") == false)
				throw new InvalidOperationException("The Raven/DataDir setting is mandatory");

			var doc = RavenJObject.FromObject(databaseDocument);
			doc.Remove("Id");

			var req = serverClient.CreateRequest("/admin/databases/" + Uri.EscapeDataString(databaseDocument.Id), "PUT");
			return req.ExecuteWriteAsync(doc.ToString(Formatting.Indented));
		}

#endif
	}
}
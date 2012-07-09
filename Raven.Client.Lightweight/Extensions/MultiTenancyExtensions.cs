//-----------------------------------------------------------------------
// <copyright file="MultiTenancyExtensions.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
#if !SILVERLIGHT
using System.Transactions;
#endif
using Raven.Client.Connection;
using Raven.Imports.Newtonsoft.Json;
using Raven.Json.Linq;

namespace Raven.Client.Extensions
{
#if !NET35
	using Raven.Client.Connection.Async;
	using System.Threading.Tasks;

#endif

	///<summary>
	/// Extension methods to create mutli tenants databases
	///</summary>
	public static class MultiTenancyExtensions
	{
#if !SILVERLIGHT
		///<summary>
		/// Ensures that the database exists, creating it if needed
		///</summary>
		/// <remarks>
		/// This operation happens _outside_ of any transaction
		/// </remarks>
		public static void EnsureDatabaseExists(this IDatabaseCommands self, string name, bool ignoreFailures = false)
		{
			var serverClient = self.ForDefaultDatabase() as ServerClient;
			if (serverClient == null)
				throw new InvalidOperationException("Ensuring database existence requires a Server Client but got: " + self);

			var doc = MultiDatabase.CreateDatabaseDocument(name);
			var docId = "Raven/Databases/" + name;
			try
			{
				if (self.Get(docId) != null)
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
		}
#endif

#if !NET35
		///<summary>
		/// Ensures that the database exists, creating it if needed
		///</summary>
		public static Task EnsureDatabaseExistsAsync(this IAsyncDatabaseCommands self, string name, bool ignoreFailures = false)
		{
			var serverClient = self.ForDefaultDatabase() as AsyncServerClient;
			if (serverClient == null)
				throw new InvalidOperationException("Ensuring database existence requires a Server Client but got: " + self);

			var doc = MultiDatabase.CreateDatabaseDocument(name);
			var docId = "Raven/Databases/" + name;

			return self.GetAsync(docId)
				.ContinueWith(get =>
				{
					if (get.Result != null)
						return get;

					var req = serverClient.CreateRequest("PUT", "/admin/databases/" + Uri.EscapeDataString(name));
					req.Write(doc.ToString(Formatting.Indented));
					return req.ExecuteRequestAsync();
				})
				.Unwrap()
				.ContinueWith(x=>
				{
					if (ignoreFailures == false)
						x.Wait(); // will throw on error

					var observedException = x.Exception;
					GC.KeepAlive(observedException);
				});
		}

#endif
	}
}
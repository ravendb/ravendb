//-----------------------------------------------------------------------
// <copyright file="MultiTenancyExtensions.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
#if !SILVERLIGHT
using System.Transactions;
#endif
using Newtonsoft.Json.Linq;
using Raven.Abstractions.Data;
using Raven.Client.Connection;
using Raven.Json.Linq;

namespace Raven.Client.Extensions
{
#if !NET_3_5
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
			self = self.ForDefaultDatabase();
			AssertValidName(name);
			var doc = RavenJObject.FromObject(new DatabaseDocument
			{
				Settings =
					{
						{"Raven/DataDir", Path.Combine("~", Path.Combine("Tenants", name))}
					}
			});
			doc.Remove("Id");
			var docId = "Raven/Databases/" + name;
			
			using (new TransactionScope(TransactionScopeOption.Suppress))
			{
				try
				{
					if (self.Get(docId) != null)
						return;

					self.Put(docId, null, doc, new RavenJObject());
				}
				catch (Exception)
				{
					if (ignoreFailures == false)
						throw;

				}
			}
		}
#endif

#if !NET_3_5
		///<summary>
		/// Ensures that the database exists, creating it if needed
		///</summary>
		public static Task EnsureDatabaseExistsAsync(this IAsyncDatabaseCommands self, string name, bool ignoreFailures = false)
		{
			self = self.ForDefaultDatabase();
			AssertValidName(name);
			var doc = RavenJObject.FromObject(new DatabaseDocument
			{
				Settings =
					{
						{"Raven/DataDir", Path.Combine("~", Path.Combine("Tenants", name))}
					}
			});
			doc.Remove("Id");
			var docId = "Raven/Databases/" + name;

			return self.GetAsync(docId)
				.ContinueWith(get =>
				{
					if (get.Result != null)
						return get;

					return (Task)self.PutAsync(docId, null, doc, new RavenJObject());
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

		static readonly string[] invalidDbNameChars = new[] { "/", "\\", "\"", "'", "<", ">"};

		private static void AssertValidName(string name)
		{
			if (name == null) throw new ArgumentNullException("name");
			if (invalidDbNameChars.Any(name.Contains))
			{
				throw new ArgumentException("Database name cannot contain any of [" +
											string.Join(", ", invalidDbNameChars) + "] but was: " + name);
			}
		}
	}
}
using System.Collections.Generic;
using System.IO;
using System.Transactions;
using Newtonsoft.Json.Linq;
using Raven.Client.Client;
using Raven.Database.Data;

namespace Raven.Client.Extensions
{
	///<summary>
	/// Extension methods to create mutli tenants databases
	///</summary>
	public static class MultiTenancyExtensions
	{
		///<summary>
		/// Ensures that the database exists, creating it if needed
		///</summary>
		/// <remarks>
		/// This operation happens _outside_ of any transaction
		/// </remarks>
		public static void EnsureDatabaseExists(this IDatabaseCommands self,string name)
		{
			var doc = JObject.FromObject(new DatabaseDocument
			{
				Settings =
					{
						{"Raven/DataDir", Path.Combine("~", Path.Combine("Tenants", name))}
					}
			});
			var docId = "Raven/Databases/" + name;
			if (self.Get(docId) != null)
				return;

			using (new TransactionScope(TransactionScopeOption.Suppress))
				self.Put(docId, null, doc, new JObject());
		}
	}
}
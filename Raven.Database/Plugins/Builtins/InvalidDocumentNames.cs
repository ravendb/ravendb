using System;

namespace Raven.Database.Plugins.Builtins
{
	public class InvalidDocumentNames : AbstractPutTrigger
	{
		public override VetoResult AllowPut(string key, Raven.Json.Linq.RavenJObject document, Raven.Json.Linq.RavenJObject metadata, Abstractions.Data.TransactionInformation transactionInformation)
		{
			if(key.Contains(@"\"))
				return VetoResult.Deny(@"Document names cannot contains '\' but attempted to save with: " + key);
			if(string.Equals(key, "Raven/Databases/Default", StringComparison.InvariantCultureIgnoreCase))
				return
					VetoResult.Deny(
						@"Cannot create a tenant database with the name 'default', that name is reserved for the actual default database");

			return VetoResult.Allowed;
		}
	}
}
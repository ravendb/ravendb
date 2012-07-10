using System;

namespace Raven.Database.Plugins.Builtins.Tenants
{
	public class ModifiedTenantDatabase : AbstractPutTrigger
	{
		private const string RavenDatabasesPrefix = "Raven/Databases/";

		public override void AfterCommit(string key, Raven.Json.Linq.RavenJObject document, Raven.Json.Linq.RavenJObject metadata, Guid etag)
		{
			if (key.StartsWith(RavenDatabasesPrefix, StringComparison.InvariantCultureIgnoreCase) == false)
				return;

			TenantDatabaseModified.Invoke(this, new TenantDatabaseModified.Event
			{
				Database = Database,
				Name = key.Substring(RavenDatabasesPrefix.Length)
			});
		}
	}

	public class DeletedTenantDatabase : AbstractDeleteTrigger
	{
		private const string RavenDatabasesPrefix = "Raven/Databases/";

		public override void AfterCommit(string key)
		{
			if (key.StartsWith(RavenDatabasesPrefix, StringComparison.InvariantCultureIgnoreCase) == false)
				return;

			TenantDatabaseModified.Invoke(this, new TenantDatabaseModified.Event
			{
				Database = Database,
				Name = key.Substring(RavenDatabasesPrefix.Length)
			});
		}
	}


}
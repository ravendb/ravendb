using System;

namespace Raven.Database.Plugins.Builtins.Tenants
{
	public class RemoveTenantDatabase : AbstractDeleteTrigger
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
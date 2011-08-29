using System;
using Raven.Database.Util;

namespace Raven.Database.Plugins.Builtins
{
	public class RemoveTenantDatabase : AbstractDeleteTrigger
	{
		private const string RavenDatabasesPrefix = "Raven/Databases/";
		public static WeakEvent<Event> Occured = new WeakEvent<Event>();

		public override void AfterCommit(string key)
		{
			if (key.StartsWith(RavenDatabasesPrefix, StringComparison.InvariantCultureIgnoreCase) == false)
				return;

			Occured.Invoke(this, new Event
			{
				Database = Database,
				Name = key.Substring(RavenDatabasesPrefix.Length)
			});
		}

		public class Event : EventArgs
		{
			public DocumentDatabase Database { get; set; }
			public string Name { get; set; }
		}
	}
}
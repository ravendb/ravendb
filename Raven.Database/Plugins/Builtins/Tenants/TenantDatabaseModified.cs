using System;

namespace Raven.Database.Plugins.Builtins.Tenants
{
	public class TenantDatabaseModified
	{
		public static event EventHandler<Event> Occured = delegate { };

		public class Event : EventArgs
		{
			public DocumentDatabase Database { get; set; }
			public string Name { get; set; }
		}

		public static void Invoke(object sender, Event args)
		{
			Occured(sender, args);
		}
	}
}
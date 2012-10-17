using System.Collections.Generic;
using Raven.Abstractions.Data;

namespace Raven.Database.Server.Security.Windows
{
	public class WindowsAuthDocument
	{
		public List<WindowsAuthData> RequiredGroups { get; set; }
		public List<WindowsAuthData> RequiredUsers { get; set; }

		public WindowsAuthDocument()
		{
			RequiredGroups = new List<WindowsAuthData>();
			RequiredUsers = new List<WindowsAuthData>();
		}
	}

	public class WindowsAuthData
	{
		public string Name { get; set; }
		public bool Enabled { get; set; }
		public List<DatabaseAccess> Databases { get; set; }

		public WindowsAuthData()
		{
			Databases = new List<DatabaseAccess>();
		}
	}
}
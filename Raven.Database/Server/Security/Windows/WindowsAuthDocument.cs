using System.Collections.Generic;

namespace Raven.Database.Server.Security.Windows
{
	class WindowsAuthDocument
	{
		public List<string> RequiredGroups { get; set; }
		public List<string> RequiredUsers { get; set; }

		public WindowsAuthDocument()
		{
			RequiredGroups = new List<string>();
			RequiredUsers = new List<string>();
		}
	}
}
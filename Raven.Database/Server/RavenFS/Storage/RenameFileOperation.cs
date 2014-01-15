using System.Collections.Specialized;

namespace Raven.Database.Server.RavenFS.Storage
{
	public class RenameFileOperation
	{
		public string Name { get; set; }

		public string Rename { get; set; }

		public NameValueCollection MetadataAfterOperation { get; set; }
	}
}
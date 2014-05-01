using Raven.Json.Linq;
using System.Collections.Specialized;

namespace Raven.Database.Server.RavenFS.Storage
{
	public class RenameFileOperation
	{
		public string Name { get; set; }

		public string Rename { get; set; }

		public RavenJObject MetadataAfterOperation { get; set; }
	}
}
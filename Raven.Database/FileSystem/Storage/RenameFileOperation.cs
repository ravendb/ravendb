using Raven.Json.Linq;
using System.Collections.Specialized;

namespace Raven.Database.FileSystem.Storage
{
	public class RenameFileOperation
	{
        public string FileSystem { get; set; }
		public string Name { get; set; }

		public string Rename { get; set; }

		public RavenJObject MetadataAfterOperation { get; set; }
	}
}
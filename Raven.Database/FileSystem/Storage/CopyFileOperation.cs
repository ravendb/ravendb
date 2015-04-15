using Raven.Json.Linq;
using System.Collections.Specialized;

namespace Raven.Database.FileSystem.Storage
{
	public class CopyFileOperation
	{
        public string FileSystem { get; set; }
		public string SourceFilename { get; set; }

		public string TargetFilename { get; set; }

		public RavenJObject MetadataAfterOperation { get; set; }
	}
}
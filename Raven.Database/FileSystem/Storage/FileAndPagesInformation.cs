using Raven.Json.Linq;
using System.Collections.Generic;
using System.Collections.Specialized;

namespace Raven.Database.FileSystem.Storage
{
	public class FileAndPagesInformation
	{
		public string Name { get; set; }
        public RavenJObject Metadata { get; set; }
		public int Start { get; set; }

		public long? TotalSize { get; set; }
		public long UploadedSize { get; set; }

		public List<PageInformation> Pages { get; set; }

		public FileAndPagesInformation()
		{
			Pages = new List<PageInformation>();
            Metadata = new RavenJObject();
		}
	}
}

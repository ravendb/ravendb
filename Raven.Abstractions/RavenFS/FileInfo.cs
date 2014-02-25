using System.Collections.Generic;
using System.Collections.Specialized;

namespace Raven.Client.RavenFS
{
	public class FileInfo
	{
		public string Name { get; set; }
		public long? TotalSize { get; set; }
		public string HumaneTotalSize { get; set; }
		public NameValueCollection Metadata { get; set; }
	}

	public class SearchResults
	{
		public FileInfo[] Files { get; set; }
		public int FileCount { get; set; }
		public int Start { get; set; }
		public int PageSize { get; set; }
	}

	public class ConfigSearchResults
	{
		public IList<string> ConfigNames { get; set; }
		public int TotalCount { get; set; }
		public int Start { get; set; }
		public int PageSize { get; set; }
	}
}
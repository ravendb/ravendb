using Raven.Abstractions.Data;
using Raven.Json.Linq;
using System;
using System.Collections.Generic;
using System.Collections.Specialized;

namespace Raven.Client.RavenFS
{
    public class FileHeader
    {
        public RavenJObject Metadata { get; private set; }

        public string Name { get; set; }
        public long? TotalSize { get; set; }
        public string HumaneTotalSize { get; set; }

        public string Path { get; private set; }
        public string Extension { get; private set; }

        public DateTimeOffset CreationDate { get; private set; }

        public DateTimeOffset LastModified { get; private set; }

        public Etag Etag { get; private set; }
    }

    public class DirectoryHeader
    {
        public RavenJObject Metadata { get; private set; }

        public string Name { get; set; }
        public string Path { get; private set; }
    }

	public class SearchResults
	{
		public FileHeader[] Files { get; set; }
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
using System.Collections.Generic;

namespace Raven.Abstractions.RavenFS
{
	public class FileSystemStats
	{
	    public string Name { get; set; }

		public long FileCount { get; set; }

        public FileSystemMetrics Metrics { get; set; }

        public IList<SynchronizationDetails> ActiveSyncs { get; set; }

        public IList<SynchronizationDetails> PendingSyncs { get; set; } 
	}
}
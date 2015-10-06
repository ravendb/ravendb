using System.Collections.Specialized;
using System.Threading.Tasks;
using Raven.Abstractions.Logging;
using Raven.Database.FileSystem.Storage;
using Raven.Json.Linq;
using Raven.Client.FileSystem;
using Raven.Abstractions.FileSystem;

namespace Raven.Database.FileSystem.Synchronization
{
	public class MetadataUpdateWorkItem : SynchronizationWorkItem
	{
        private readonly RavenJObject destinationMetadata;
		private readonly ILog log = LogManager.GetCurrentClassLogger();

        public MetadataUpdateWorkItem(string fileName, string sourceServerUrl, ITransactionalStorage storage)
            : this(fileName, sourceServerUrl, new RavenJObject(), storage)
        {            
        }
        public MetadataUpdateWorkItem(string fileName, string sourceServerUrl, RavenJObject destinationMetadata, ITransactionalStorage storage)
            : base(fileName, sourceServerUrl, storage)
        {
            this.destinationMetadata = destinationMetadata;
        }

		public override SynchronizationType SynchronizationType
		{
			get { return SynchronizationType.MetadataUpdate; }
		}

		public override async Task<SynchronizationReport> PerformAsync(ISynchronizationServerClient synchronizationServerClient)
		{
			AssertLocalFileExistsAndIsNotConflicted(FileMetadata);

			var conflict = CheckConflictWithDestination(FileMetadata, destinationMetadata, FileSystemInfo.Url);

	        if (conflict != null)
	        {
				var report = await HandleConflict(synchronizationServerClient, conflict, log).ConfigureAwait(false);

				if (report != null)
					return report;
	        }

			return await synchronizationServerClient.UpdateMetadataAsync(FileName, FileMetadata, FileSystemInfo).ConfigureAwait(false);
		}

		public override bool Equals(object obj)
		{
			if (ReferenceEquals(null, obj)) return false;
			if (ReferenceEquals(this, obj)) return true;
			if (obj.GetType() != typeof(MetadataUpdateWorkItem)) return false;
			return Equals((MetadataUpdateWorkItem)obj);
		}

		public bool Equals(MetadataUpdateWorkItem other)
		{
			if (ReferenceEquals(null, other)) return false;
			if (ReferenceEquals(this, other)) return true;
			return Equals(other.FileName, FileName) && Equals(other.FileETag, FileETag);
		}

		public override int GetHashCode()
		{
			return (FileName != null ? GetType().Name.GetHashCode() ^ FileName.GetHashCode() ^ FileETag.GetHashCode() : 0);
		}

		public override string ToString()
		{
			return string.Format("Metadata synchronization of a file '{0}'", FileName);
		}
	}
}
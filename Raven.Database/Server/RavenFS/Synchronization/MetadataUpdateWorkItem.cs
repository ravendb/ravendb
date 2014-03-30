using System.Collections.Specialized;
using System.Threading.Tasks;
using Raven.Abstractions.Logging;
using Raven.Client.RavenFS;
using Raven.Database.Server.RavenFS.Storage;

namespace Raven.Database.Server.RavenFS.Synchronization
{
	public class MetadataUpdateWorkItem : SynchronizationWorkItem
	{
		private readonly NameValueCollection destinationMetadata;
		private readonly ILog log = LogManager.GetCurrentClassLogger();

		public MetadataUpdateWorkItem(string fileName, string sourceServerUrl, NameValueCollection destinationMetadata,
									  ITransactionalStorage storage)
			: base(fileName, sourceServerUrl, storage)
		{
			this.destinationMetadata = destinationMetadata;
		}

		public override SynchronizationType SynchronizationType
		{
			get { return SynchronizationType.MetadataUpdate; }
		}

        public override Task<SynchronizationReport> PerformAsync(RavenFileSystemClient.SynchronizationClient destination)
		{
			AssertLocalFileExistsAndIsNotConflicted(FileMetadata);

			var conflict = CheckConflictWithDestination(FileMetadata, destinationMetadata, ServerInfo.FileSystemUrl);

			if (conflict != null)
				return ApplyConflictOnDestinationAsync(conflict, destination, ServerInfo.FileSystemUrl, log);

            return destination.UpdateMetadataAsync(FileName, FileMetadata, ServerInfo);
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
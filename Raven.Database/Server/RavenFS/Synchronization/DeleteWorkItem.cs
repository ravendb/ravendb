using System.Threading.Tasks;
using Raven.Client.RavenFS;
using Raven.Database.Server.RavenFS.Storage;

namespace Raven.Database.Server.RavenFS.Synchronization
{
	public class DeleteWorkItem : SynchronizationWorkItem
	{
		public DeleteWorkItem(string fileName, string sourceServerUrl, ITransactionalStorage storage)
			: base(fileName, sourceServerUrl, storage)
		{
		}

		public override SynchronizationType SynchronizationType
		{
			get { return SynchronizationType.Delete; }
		}

        public override Task<SynchronizationReport> PerformAsync(RavenFileSystemClient.SynchronizationClient destination)
		{
			FileAndPages fileAndPages = null;
			Storage.Batch(accessor => fileAndPages = accessor.GetFile(FileName, 0, 0));

            return destination.DeleteAsync(FileName, fileAndPages.Metadata, ServerInfo);
		}

		public override bool Equals(object obj)
		{
			if (ReferenceEquals(null, obj)) return false;
			if (ReferenceEquals(this, obj)) return true;
			if (obj.GetType() != typeof(DeleteWorkItem)) return false;
			return Equals((DeleteWorkItem)obj);
		}

		public bool Equals(DeleteWorkItem other)
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
			return string.Format("Synchronization of a deleted file '{0}'", FileName);
		}
	}
}
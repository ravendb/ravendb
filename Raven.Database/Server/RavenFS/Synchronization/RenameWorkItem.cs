using System.Threading.Tasks;
using Raven.Client.RavenFS;
using Raven.Database.Server.RavenFS.Storage;

namespace Raven.Database.Server.RavenFS.Synchronization
{
	public class RenameWorkItem : SynchronizationWorkItem
	{
		private readonly string rename;

		public RenameWorkItem(string name, string rename, string sourceServerUrl, ITransactionalStorage storage)
			: base(name, sourceServerUrl, storage)
		{
			this.rename = rename;
		}

		public override SynchronizationType SynchronizationType
		{
			get { return SynchronizationType.Rename; }
		}

        public override Task<SynchronizationReport> PerformAsync(RavenFileSystemClient.SynchronizationClient destination)
		{
			FileAndPages fileAndPages = null;
			Storage.Batch(accessor => fileAndPages = accessor.GetFile(FileName, 0, 0));

            return destination.RenameAsync(FileName, rename, fileAndPages.Metadata, ServerInfo);
		}

		public override bool Equals(object obj)
		{
			if (ReferenceEquals(null, obj)) return false;
			if (ReferenceEquals(this, obj)) return true;
			if (obj.GetType() != typeof(RenameWorkItem)) return false;
			return Equals((RenameWorkItem)obj);
		}

		public bool Equals(RenameWorkItem other)
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
			return string.Format("Synchronization of a renaming of a file '{0}' to '{1}'", FileName, rename);
		}
	}
}
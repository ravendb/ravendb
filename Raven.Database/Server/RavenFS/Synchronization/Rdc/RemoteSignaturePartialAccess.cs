using System.IO;
using System.Threading.Tasks;
using Raven.Client.RavenFS;

namespace Raven.Database.Server.RavenFS.Synchronization.Rdc
{
	public class RemoteSignaturePartialAccess : IPartialDataAccess
	{
		private readonly string _fileName;
        private readonly RavenFileSystemClient.SynchronizationClient synchronizationClient;

        public RemoteSignaturePartialAccess(RavenFileSystemClient.SynchronizationClient synchronizationClient, string fileName)
		{
			this.synchronizationClient = synchronizationClient;
			_fileName = fileName;
		}

		public Task CopyToAsync(Stream target, long from, long length)
		{
			return synchronizationClient.DownloadSignatureAsync(_fileName, target, from, from + length);
		}
	}
}
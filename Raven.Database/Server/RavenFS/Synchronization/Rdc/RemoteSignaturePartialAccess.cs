using System.IO;
using System.Threading.Tasks;
using Raven.Client.RavenFS;

namespace Raven.Database.Server.RavenFS.Synchronization.Rdc
{
	public class RemoteSignaturePartialAccess : IPartialDataAccess
	{
		private readonly string _fileName;
		private readonly RavenFileSystemClient _ravenFileSystemClient;

		public RemoteSignaturePartialAccess(RavenFileSystemClient ravenFileSystemClient, string fileName)
		{
			_ravenFileSystemClient = ravenFileSystemClient;
			_fileName = fileName;
		}

		public Task CopyToAsync(Stream target, long from, long length)
		{
			return _ravenFileSystemClient.Synchronization.DownloadSignatureAsync(_fileName, target, from, from + length);
		}
	}
}
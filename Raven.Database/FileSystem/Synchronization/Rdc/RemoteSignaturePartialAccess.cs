using System.IO;
using System.Threading.Tasks;
using Raven.Client.FileSystem;

namespace Raven.Database.FileSystem.Synchronization.Rdc
{
    public class RemoteSignaturePartialAccess : IPartialDataAccess
    {
        private readonly string _fileName;
        private readonly ISynchronizationServerClient synchronizationServerClient;

        public RemoteSignaturePartialAccess(ISynchronizationServerClient synchronizationServerClient, string fileName)
        {
            this.synchronizationServerClient = synchronizationServerClient;
            _fileName = fileName;
        }

        public Task CopyToAsync(Stream target, long from, long length)
        {
            return synchronizationServerClient.DownloadSignatureAsync(_fileName, target, from, from + length);
        }
    }
}

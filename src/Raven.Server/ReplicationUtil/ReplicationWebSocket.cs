using System;
using System.Threading.Tasks;
using Raven.Client.Platform;
using Raven.Server.Documents;

namespace Raven.Server.ReplicationUtil
{
    public class ReplicationWebSocket : IDisposable
    {
        private readonly RavenClientWebSocket _socket;

        public ReplicationWebSocket(string url)
        {
            _socket = new RavenClientWebSocket();
        }

        public async Task<long> GetLastEtagAsync()  
        {
            throw new NotImplementedException();
        }

        private async Task EnsureConnectionAsync()
        {
            //TODO : connection code
            throw new NotImplementedException();
        }

        public async Task SendDocumentAsync(Document doc)
        {
            await EnsureConnectionAsync();
            throw new NotImplementedException();
        }

        public async Task SendHeartbeatAsync()
        {
            await EnsureConnectionAsync();
            throw new NotImplementedException();
        }

        public void Dispose()
        {		    
            _socket.Dispose();
        }
    }
}

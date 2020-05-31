using System;
using System.Threading.Tasks;
using Raven.Server.ServerWide;
using Sparrow.Logging;

namespace Raven.Server.Documents.Replication
{
    public class ReplicationCertificatesStorage : IDisposable
    {
        private DocumentDatabase _db;
        private ServerStore _serverStore;
        private Logger _logger;

        public ReplicationCertificatesStorage(DocumentDatabase db, ServerStore serverStore)
        {
            _db = db;
            _serverStore = serverStore;
            _logger = LoggingSource.Instance.GetLogger<ReplicationCertificatesStorage>(db.Name);
        }

        public async Task AddReplicationCertificate(string name)
        {
            
        }


        public void Dispose()
        {
            
        }
    }
}

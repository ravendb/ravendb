using System;
using System.Threading;
using Raven.Client.Platform;
using Raven.Server.ReplicationUtil;

namespace Raven.Server.Documents.Replication
{
    //TODO : add DocumentReplicationStatistics that will track operational data
    public class DocumentReplicationExecuter: BaseReplicationExecuter
    {
        private readonly ReplicationWebSocket _socket;
        private readonly DocumentReplicationDestination _config;

        public DocumentReplicationExecuter(DocumentDatabase database, DocumentReplicationDestination config) : base(database)
        {
            _config = config;
            _socket = new ReplicationWebSocket(config.Url);
        }

        public override string Name => _config.Url;

        protected override void ExecuteReplicationOnce()
        {

            throw new NotImplementedException();
        }

        public override void Dispose()
        {
            _socket.Dispose();
            base.Dispose();
        }
    }
}

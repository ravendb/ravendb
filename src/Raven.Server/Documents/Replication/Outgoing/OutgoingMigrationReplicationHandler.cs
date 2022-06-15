using System;
using System.IO;
using Raven.Client.ServerWide.Commands;
using Raven.Server.Documents.Replication.Senders;
using Sparrow.Logging;

namespace Raven.Server.Documents.Replication.Outgoing
{
    public class OutgoingMigrationReplicationHandler : OutgoingReplicationHandlerBase
    {
        public readonly BucketMigrationReplication BucketMigrationNode;
        public OutgoingMigrationReplicationHandler(ReplicationLoader parent, DocumentDatabase database, BucketMigrationReplication node, TcpConnectionInfo connectionInfo) : base(parent, database, node, connectionInfo)
        {
            BucketMigrationNode = node;
        }

        public override ReplicationDocumentSenderBase CreateDocumentSender(Stream stream, Logger logger) => 
            new MigrationReplicationDocumentSender(stream, this, logger);

        protected override void ProcessHandshakeResponse((ReplicationMessageReply.ReplyType ReplyType, ReplicationMessageReply Reply) response)
        {
            var request = new MigrationRequest
            {
                Database = Destination.Database,
                Bucket = BucketMigrationNode.Bucket,
                MigrationIndex = BucketMigrationNode.MigrationIndex
            };

            base.ProcessHandshakeResponse(response);
        }

        public override int GetHashCode() => BucketMigrationNode.GetHashCode();

        public class MigrationRequest
        {
            public int Version;
            public string Database;
            public int Bucket;
            public long MigrationIndex;
        }

        public class MigrationRequestResponse
        {
            public string LastBucketChangeVector;
        }
    }
}

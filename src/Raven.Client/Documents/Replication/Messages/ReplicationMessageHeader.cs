namespace Raven.Client.Documents.Replication.Messages
{
    internal sealed class ReplicationMessageHeader
    {
        public string Type { get; set; }

        public long LastDocumentEtag { get; set; }

        public string DatabaseChangeVector { get; set; }

        // TODO RavenDB-26295 / #22885: consume this source-frontier value on the incoming side to advance pull-replication failover cursors.
        public string LastSentChangeVector { get; set; }

        public int ItemsCount { get; set; }

        public int AttachmentStreamsCount { get; set; }
    }
}

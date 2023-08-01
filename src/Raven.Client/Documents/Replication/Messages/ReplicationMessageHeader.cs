namespace Raven.Client.Documents.Replication.Messages
{
    internal sealed class ReplicationMessageHeader
    {
        public string Type { get; set; }

        public long LastDocumentEtag { get; set; }

        public string DatabaseChangeVector { get; set; }

        public int ItemsCount { get; set; }

        public int AttachmentStreamsCount { get; set; }
    }
}

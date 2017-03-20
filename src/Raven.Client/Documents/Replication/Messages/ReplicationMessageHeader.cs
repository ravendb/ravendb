namespace Raven.Client.Documents.Replication.Messages
{
    internal class ReplicationMessageHeader
    {
        public string Type { get; set; }

        public long LastDocumentEtag { get; set; }

        public long LastIndexOrTransformerEtag { get; set; }

        public int ItemsCount { get; set; }

        public int AttachmentStreamsCount { get; set; }

        public string ResolverId { get; set; }

        public int? ResolverVersion { get; set; }
    }
}

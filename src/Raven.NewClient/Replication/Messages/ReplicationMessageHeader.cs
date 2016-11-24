namespace Raven.NewClient.Replication.Messages
{
    public class ReplicationMessageHeader
    {
        public string Type { get; set; }

        public long LastDocumentEtag { get; set; }

        public long LastIndexOrTransformerEtag { get; set; }

        public int ItemCount { get; set; }
    }
}

namespace Raven.Client.Replication.Messages
{
    public class ReplicationLatestEtagReply
    {
        public long LastSentEtag { get; set; }

        public ChangeVectorEntry[] CurrentChangeVector { get; set; }
    }
}
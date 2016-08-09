namespace Raven.Abstractions.Replication
{
    public class ReplicationLatestEtagReply
    {
        public long LastSentEtag { get; set; }

        public ChangeVectorEntry[] CurrentChangeVector { get; set; }
    }
}
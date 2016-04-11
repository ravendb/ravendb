namespace Raven.Server.Documents.SqlReplication
{
    public class SqlReplicationStatus
    {
        public string Name { get; set; } 
        public long LastReplicatedEtag { get; set; } 
        public long LastTombstonesEtag { get; set; } 
    }
}
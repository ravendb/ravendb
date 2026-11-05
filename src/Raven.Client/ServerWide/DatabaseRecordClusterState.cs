namespace Raven.Client.ServerWide
{
    /// <summary>
    /// Raft indexes of the last change to each section of the database record.
    /// Managed by the server, values sent by the client are ignored.
    /// </summary>
    public sealed class DatabaseRecordClusterState
    {
        public long? LastTopologyIndex;

        public long? LastIndexesIndex;

        public long? LastSortersIndex;

        public long? LastAnalyzersIndex;

        public long? LastReplicationIndex;

        public long? LastConnectionStringsIndex;

        public long? LastEtlsIndex;

        public long? LastEmbeddingsGenerationsIndex;

        public long? LastPeriodicBackupsIndex;

        public long? LastQueueSinksIndex;

        public long? LastCdcSinksIndex;
    }
}

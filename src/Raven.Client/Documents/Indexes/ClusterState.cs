namespace Raven.Client.Documents.Indexes
{
    internal class ClusterState
    {
        public ClusterState()
        {
            LastStateIndex = 0;
        }

        public ClusterState(ClusterState clusterState)
        {
            LastStateIndex = clusterState.LastStateIndex;
        }

        public long LastStateIndex;
    }
}

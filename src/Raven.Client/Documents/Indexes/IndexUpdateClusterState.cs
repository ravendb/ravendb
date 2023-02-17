namespace Raven.Client.Documents.Indexes
{
    public class IndexUpdateClusterState
    {
        public IndexUpdateClusterState()
        {
            LastIndex = 0;
            LastStateIndex = 0;
            LastRollingDeploymentIndex = 0;
        }

        public IndexUpdateClusterState(IndexUpdateClusterState clusterState)
        {
            LastIndex = clusterState.LastIndex;
            LastStateIndex = clusterState.LastStateIndex;
            LastRollingDeploymentIndex = clusterState.LastRollingDeploymentIndex;
        }

        public long LastIndex;
        public long LastStateIndex;
        public long LastRollingDeploymentIndex;
    }
}

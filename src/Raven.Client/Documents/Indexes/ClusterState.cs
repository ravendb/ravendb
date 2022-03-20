namespace Raven.Client.Documents.Indexes
{
    internal class ClusterState
    {
        public ClusterState()
        {
            LastStateIndex = 0;
            LastRollingDeploymentIndex = 0;
        }

        public ClusterState(ClusterState clusterState)
        {
            LastStateIndex = clusterState.LastStateIndex;
            LastRollingDeploymentIndex = clusterState.LastRollingDeploymentIndex;
        }

        public long LastStateIndex;
        public long LastRollingDeploymentIndex;
    }
}

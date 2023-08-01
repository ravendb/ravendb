namespace Raven.Client.Documents.Indexes
{
    internal sealed class IndexDefinitionClusterState
    {
        public IndexDefinitionClusterState()
        {
            LastIndex = 0;
            LastStateIndex = 0;
            LastRollingDeploymentIndex = 0;
        }

        public IndexDefinitionClusterState(IndexDefinitionClusterState clusterState)
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

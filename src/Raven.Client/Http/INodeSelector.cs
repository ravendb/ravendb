using System;

namespace Raven.Client.Http
{
    public interface INodeSelector
    {
        Topology Topology { get; }
        int GetCurrentNodeIndex();
        void OnSucceededRequest();
        void OnFailedRequest(int nodeIndex);
        bool OnUpdateTopology(Topology topology, bool forceUpdate = false);
        ServerNode GetCurrentNode();
        void RestoreNodeIndex(int nodeIndex);

        event Action<int> NodeSwitch;
    }
}

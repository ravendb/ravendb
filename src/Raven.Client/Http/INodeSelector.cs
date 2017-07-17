using System;

namespace Raven.Client.Http
{
    public interface INodeSelector
    {
        Topology Topology { get; }
        int GetCurrentNodeIndex();
        INodeSelector HandleRequestWithoutSessionId();
        INodeSelector AdvanceToNextNodeAndFetchInstance();
        void OnFailedRequest(int nodeIndex);
        bool OnUpdateTopology(Topology topology, bool forceUpdate = false);
        ServerNode GetCurrentNode();
        void RestoreNodeIndex(int nodeIndex);

        INodeSelector CloneForNewSession();

        event Action<int> NodeSwitch;
    }
}

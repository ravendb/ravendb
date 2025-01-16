using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using Raven.Client.Exceptions;
using Raven.Client.Exceptions.Database;

namespace Raven.Client.Http
{
    public class NodeSelector : IDisposable
    {
        internal class NodeSelectorState
        {
            public readonly Topology Topology;
            public readonly List<ServerNode> Nodes;
            public readonly int[] Failures;
            public readonly int[] FastestRecords;
            public int Fastest;
            public int SpeedTestMode;
            public int UnlikelyEveryoneFaultedChoiceIndex;

            public NodeSelectorState(Topology topology)
            {
                Topology = topology;
                Nodes = topology.Nodes;
                Failures = new int[topology.Nodes.Count];
                FastestRecords = new int[topology.Nodes.Count];
                UnlikelyEveryoneFaultedChoiceIndex = 0;
            }

            public NodeSelectorState(Topology topology, NodeSelectorState prevState) : this(topology)
            {
                if (prevState.Fastest < 0 || prevState.Fastest >= prevState.Nodes.Count)
                {
                    Debug.Assert(false, "Fastest is out of range of Nodes in NodeSelectorState");
                    return;
                }

                var fastestNode = prevState.Nodes.ElementAt(prevState.Fastest);
                int index = 0;
                foreach (var node in topology.Nodes)
                {
                    if (node.ClusterTag == fastestNode.ClusterTag)
                    {
                        Fastest = index;
                        break;
                    }

                    index++;
                }
                
                // fastest node was not found in the new topology. enable speed tests
                if (index >= topology.Nodes.Count)
                {
                    SpeedTestMode = 2;
                }
                else
                {
                    // we might be in the process of finding fastest node when we reorder the nodes, we don't want the tests to stop until we reach 10
                    // otherwise, we want to stop the tests and they may be scheduled later on relevant topology change
                    if (Fastest < prevState.FastestRecords.Length && prevState.FastestRecords[Fastest] < 10)
                    {
                        SpeedTestMode = prevState.SpeedTestMode;
                    }
                }
            }

            public ValueTuple<int, ServerNode> GetNodeWhenEveryoneMarkedAsFaulted()
            {
                int index = UnlikelyEveryoneFaultedChoiceIndex;
                UnlikelyEveryoneFaultedChoiceIndex = (UnlikelyEveryoneFaultedChoiceIndex + 1) % Nodes.Count;
                return (index, Nodes[index]);
            }
        }        

        public Topology Topology => _state.Topology;

        internal Timer _updateFastestNodeTimer;

        internal NodeSelectorState _state;

        public NodeSelector(Topology topology)
        {
            _state = new NodeSelectorState(topology);
        }

        public void OnFailedRequest(int nodeIndex)
        {
            var state = _state;
            if (nodeIndex < 0 || nodeIndex >= state.Failures.Length)
                return; // probably already changed

            Interlocked.Increment(ref state.Failures[nodeIndex]);
        }

        public bool OnUpdateTopology(Topology topology, bool forceUpdate = false)
        {
            if (topology == null)
                return false;

            if (_state.Topology.Etag >= topology.Etag && forceUpdate == false)
                return false;

            var state = new NodeSelectorState(topology, _state);
            
            Interlocked.Exchange(ref _state, state);

            return true;
        }

        internal (int Index, ServerNode Node) GetRequestedNode(string nodeTag)
        {
            var state = _state;
            var serverNodes = state.Nodes;

            for (var i = 0; i < serverNodes.Count; i++)
            {
                if (serverNodes[i].ClusterTag == nodeTag)
                {
                    Debug.Assert(string.IsNullOrEmpty(serverNodes[i].Url) == false, $"Expected serverNodes Url not null or empty but got: \'{serverNodes[i].Url}\'");

                    return (i, serverNodes[i]);
                }
            }

            if (state.Nodes.Count == 0)
                throw new DatabaseDoesNotExistException("There are no nodes in the topology.");

            throw new RequestedNodeUnavailableException($"Could not find requested node {nodeTag}.");
        }

        internal bool NodeIsAvailable(int index)
        {
            return _state.Failures[index] == 0;
        }

        public (int Index, ServerNode Node) GetPreferredNode()
        {
            var state = _state;
            return GetPreferredNodeInternal(state);
        }

        private static (int Index, ServerNode Node) GetPreferredNodeInternal(NodeSelectorState state)
        {
            var stateFailures = state.Failures;
            var serverNodes = state.Nodes;
            var len = Math.Min(serverNodes.Count, stateFailures.Length);
            for (int i = 0; i < len; i++)
            {
                Debug.Assert(string.IsNullOrEmpty(serverNodes[i].Url) == false, $"Expected serverNodes Url not null or empty but got: \'{serverNodes[i].Url}\'");
                if (stateFailures[i] == 0 && serverNodes[i].ServerRole == ServerNode.Role.Member)
                {
                    return (i, serverNodes[i]);
                }
            }

            return UnlikelyEveryoneFaultedChoice(state);
        }

        internal int[] NodeSelectorFailures => _state.Failures;

        private static ValueTuple<int, ServerNode> UnlikelyEveryoneFaultedChoice(NodeSelectorState state)
        {
            // if there are all marked as failed, we'll chose the next (the one in CurrentNodeIndex)
            // one so the user will get an error (or recover :-) );
            if (state.Nodes.Count == 0)
                throw new DatabaseDoesNotExistException("There are no nodes in the topology at all");

            var stateFailures = state.Failures;
            var serverNodes = state.Nodes;
            var len = Math.Min(serverNodes.Count, stateFailures.Length);
            for (int i = 0; i < len; i++)
            {
                if (stateFailures[i] == 0)
                {
                    return (i, serverNodes[i]);
                }
            }
            
            return state.GetNodeWhenEveryoneMarkedAsFaulted();
        }

        public (int Index, ServerNode Node) GetNodeBySessionId(int sessionId)
        {
            var state = _state;

            if (state.Topology.Nodes.Count == 0)
                throw new AllTopologyNodesDownException("There are no nodes in the topology at all");

            var index = Math.Abs(sessionId % state.Topology.Nodes.Count);

            for (int i = index; i < state.Failures.Length; i++)
            {
                if (state.Failures[i] == 0 && state.Nodes[i].ServerRole == ServerNode.Role.Member)
                    return (i, state.Nodes[i]);
            }

            for (int i = 0; i < index; i++)
            {
                if (state.Failures[i] == 0 && state.Nodes[i].ServerRole == ServerNode.Role.Member)
                    return (i, state.Nodes[i]);
            }
            
            return GetPreferredNode();
        }

        public (int Index, ServerNode Node) GetFastestNode()
        {            
            var state = _state;
            if (state.Failures[state.Fastest] == 0 && state.Nodes[state.Fastest].ServerRole == ServerNode.Role.Member)
                return (state.Fastest, state.Nodes[state.Fastest]);
            
            // until new fastest node is selected, we'll just use the server preferred node or failover as usual
            ScheduleSpeedTest();
            return GetPreferredNode();
        }

        public void RestoreNodeIndex(ServerNode node)
        {
            var state = _state;
            var nodeIndex = state.Nodes.IndexOf(node);
            if (nodeIndex == -1)
                return;
            
            while (true)
            {
                var stateFailure = state.Failures[nodeIndex];
                stateFailure = Interlocked.Add(ref state.Failures[nodeIndex], -stateFailure);// zero it
                if (stateFailure >= 0)
                    break;// someone could add failures in the meanwhile, so we are good with values higher than 0. 
            }
        }

        protected static void ThrowEmptyTopology()
        {
            throw new InvalidOperationException("Empty database topology, this shouldn't happen.");
        }

        private void SwitchToSpeedTestPhase(object _)
        {
            var state = _state;
            if (Interlocked.CompareExchange(ref state.SpeedTestMode, 1, 0) != 0)
                return;

            Array.Clear(state.FastestRecords,0, state.Failures.Length);

            Interlocked.Increment(ref state.SpeedTestMode);
        }

        internal void DisableFastestNodeReadBalance()
        {
            if (_updateFastestNodeTimer != null)
            {
                _updateFastestNodeTimer.Dispose();
                _updateFastestNodeTimer = null;
            }

            var state = _state;
            Interlocked.Exchange(ref state.SpeedTestMode, 0);
            Array.Clear(state.FastestRecords, index: 0, state.FastestRecords.Length);
        }

        public bool InSpeedTestPhase => _state.SpeedTestMode > 1;

        public void RecordFastest(int index, ServerNode node)
        {
            var state = _state;
            var stateFastest = state.FastestRecords;

            // the following two checks are to verify that things didn't move
            // while we were computing the fastest node, we verify that the index
            // of the fastest node and the identity of the node didn't change during
            // our check
            if (index < 0 || index >= stateFastest.Length)
                return;

            if (ReferenceEquals(node, state.Nodes[index]) == false)
                return;

            if (Interlocked.Increment(ref stateFastest[index]) >= 10)
            {
                SelectFastest(state, index);
                return;
            }

            if (Interlocked.Increment(ref state.SpeedTestMode) <= state.Nodes.Count * 10)
                return;

            //too many concurrent speed tests are happening
            var maxIndex = FindMaxIndex(state);
            SelectFastest(state, maxIndex);
        }

        private static int FindMaxIndex(NodeSelectorState state)
        {
            var stateFastest = state.FastestRecords;
            var maxIndex = 0;
            var maxValue = 0;

            for (var i = 0; i < stateFastest.Length; i++)
            {
                if (maxValue >= stateFastest[i])
                    continue;
                maxIndex = i;
                maxValue = stateFastest[i];
            }

            return maxIndex;
        }

        private void SelectFastest(NodeSelectorState state, int index)
        {
            state.Fastest = index;
            Interlocked.Exchange(ref state.SpeedTestMode, 0);
            ScheduleSpeedTest();
        }

        private readonly object _timerCreationLocker = new object();

        public void ScheduleSpeedTest()
        {
            if (_updateFastestNodeTimer != null)
                return;

            lock (_timerCreationLocker)
            {
                if (_updateFastestNodeTimer != null)
                    return;

                SwitchToSpeedTestPhase(null);
                _updateFastestNodeTimer = new Timer(SwitchToSpeedTestPhase, null, TimeSpan.FromMinutes(1), TimeSpan.FromMinutes(1));
            }
        }

        public void Dispose()
        {
            _updateFastestNodeTimer?.Dispose();
        }
    }
}

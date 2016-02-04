using System;
using System.Collections.Generic;
using Raven.Abstractions.Data;
using Raven.Database.Bundles.Replication.Data;

namespace Raven.Database.Counters.Replication
{
    public class CountersReplicationTopologyRootNode : CountersReplicationTopologyNodeBase
    {
        public Guid ServerId { get; set; }

        public CountersReplicationTopologyRootNode(string serverUrl, Guid serverId)
        {
            ServerUrl = serverUrl;
            ServerId = serverId;
        }

        private static void HandleLink(CountersReplicationTopology topology, CountersReplicationTopologyNodeBase source, CountersReplicationTopologyNodeBase target)
        {
            topology.Servers.Add(source.ServerUrl);
            topology.Servers.Add(target.ServerUrl);

            CountersReplicationTopologyConnection connection = null;

            if (target is CountersReplicationTopologyDestinationNode)
            {
                var destinationNode = (CountersReplicationTopologyDestinationNode)target;
                // going to destination
                connection = topology.GetConnection(source.ServerUrl, target.ServerUrl) ?? new CountersReplicationTopologyConnection();
                connection.Source = source.ServerUrl;
                connection.Destination = target.ServerUrl;
                connection.SourceToDestinationState = destinationNode.State;
                connection.SendServerId = destinationNode.SendServerId;
                connection.Errors = destinationNode.Errors;
            }
            else if (target is CountersReplicationTopologySourceNode)
            {
                // going to source
                var sourceNode = (CountersReplicationTopologySourceNode)target;
                connection = topology.GetConnection(target.ServerUrl, source.ServerUrl) ?? new CountersReplicationTopologyConnection();
                connection.Source = target.ServerUrl;
                connection.Destination = source.ServerUrl;
                connection.DestinationToSourceState = sourceNode.State;
                connection.StoredServerId = sourceNode.StoredServerId;
                connection.LastEtag = sourceNode.LastEtag;
            }

            topology.Connections.Add(connection);
        }

        public CountersReplicationTopology Flatten()
        {
            var topology = new CountersReplicationTopology();
            topology.Servers.Add(ServerUrl);

            var queue = new Queue<CountersReplicationTopologyNodeBase>();
            queue.Enqueue(this);

            while (queue.Count > 0)
            {
                var node = queue.Dequeue();

                foreach (var dst in node.Destinations)
                {
                    HandleLink(topology, node, dst);
                    queue.Enqueue(dst);
                }

                foreach (var source in node.Sources)
                {
                    HandleLink(topology, node, source);
                    queue.Enqueue(source);
                }
            }

            return topology;
        }
    }

    public class CountersReplicationTopologyDestinationNode : CountersReplicationTopologyNode
    {
        public Guid SendServerId { get; set; }

        public static CountersReplicationTopologyDestinationNode Online(string serverUrl, Guid serverId)
        {
            return new CountersReplicationTopologyDestinationNode
            {
                ServerUrl = serverUrl,
                State = ReplicatonNodeState.Online,
                SendServerId = serverId
            };
        }

        public static CountersReplicationTopologyDestinationNode Offline(string serverUrl, Guid serverId)
        {
            return new CountersReplicationTopologyDestinationNode
            {
                ServerUrl = serverUrl,
                State = ReplicatonNodeState.Offline,
                SendServerId = serverId
            };
        }

        public static CountersReplicationTopologyDestinationNode Disabled(string serverUrl, Guid serverId)
        {
            return new CountersReplicationTopologyDestinationNode
            {
                ServerUrl = serverUrl,
                State = ReplicatonNodeState.Disabled,
                SendServerId = serverId
            };
        }
    }

    public class CountersReplicationTopologySourceNode : CountersReplicationTopologyNode
    {
        public Guid StoredServerId { get; set; }

        public long LastEtag { get; set; }

        public static CountersReplicationTopologySourceNode Online(string serverUrl, Guid serverId, long lastEtag)
        {
            return new CountersReplicationTopologySourceNode
            {
                       ServerUrl = serverUrl,
                       State = ReplicatonNodeState.Online,
                       LastEtag = lastEtag,
                       StoredServerId = serverId
                   };
        }

        public static CountersReplicationTopologySourceNode Offline(string serverUrl, Guid serverId, long lastEtag)
        {
            return new CountersReplicationTopologySourceNode
            {
                ServerUrl = serverUrl,
                State = ReplicatonNodeState.Offline,
                LastEtag = lastEtag,
                StoredServerId = serverId
            };
        }
    }

    public abstract class CountersReplicationTopologyNode : CountersReplicationTopologyNodeBase
    {
        public ReplicatonNodeState State { get; protected set; }
    }

    public abstract class CountersReplicationTopologyNodeBase
    {
        protected CountersReplicationTopologyNodeBase()
        {
            Sources = new List<CountersReplicationTopologySourceNode>();
            Destinations = new List<CountersReplicationTopologyDestinationNode>();
            Errors = new List<string>();
        }

        public string ServerUrl { get; protected set; }

        public List<CountersReplicationTopologySourceNode> Sources { get; set; }

        public List<CountersReplicationTopologyDestinationNode> Destinations { get; set; }

        public List<string> Errors { get; set; }
    }
}

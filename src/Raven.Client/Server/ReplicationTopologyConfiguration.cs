//using System;
//using System.Collections.Generic;
//using System.Linq;
//using Raven.Client.Documents.Replication;
//using Sparrow.Json.Parsing;
//
//namespace Raven.Client.Documents
//{
//    public class ReplicationTopologyConfiguration
//    {
//        public ReplicationNode Senator;
//        public Dictionary<string, List<ReplicationNode>> OutgoingConnections;
//        public Dictionary<string, ScriptResolver> ResolveByCollection;
//        public bool ResolveToLatest;
//
//        public void SetupRingReplication(DatabaseTopology topology, string database, ReplicationNode senator = null)
//        {
//            OutgoingConnections = new Dictionary<string, List<ReplicationNode>>();
//            // setup ring master-master between the members
//            var memberNodes = SetupMembers(topology, database);
//            // setup master-slave between the senator and the Watchers / Promotables
//            SetupSlaves(topology, topology.Watchers, database, senator ?? memberNodes[0]);
//            SetupSlaves(topology, topology.Promotables, database, senator ?? memberNodes[0]);
//        }
//
//        private void SetupSlaves(DatabaseTopology topology,List<string> nodes, string database, ReplicationNode senator)
//        {
//            if (senator == null)
//            {
//                throw new ArgumentNullException($"Must set a senator in order to have watchers in the replication topology.");
//            }
//
//            var nodesCount = nodes.Count;
//            var replicationNodes = nodes.Select(w => new ReplicationNode
//                {
//                    Url = topology.NameToUrlMap[w],
//                    NodeTag = w,
//                    Database = database,
//                })
//                .ToArray();
//
//            for (var i = 0; i < nodesCount; i++)
//            {
//                var currentNode = replicationNodes[i];
//                OutgoingConnections[Senator.NodeTag].Add(currentNode);
//            }
//        }
//
//        public IEnumerable<ReplicationNode> GetDestinations(string me)
//        {
//            if (me == null)
//                return null;
//
//            return OutgoingConnections[me];
//        }
//
//        private ReplicationNode[] SetupMembers(DatabaseTopology topology, string database)
//        {
//            var members = topology.Members;
//            var memberCount = members.Count;
//            var memberNodes = members.Select(n => new ReplicationNode
//                {
//                    Url = topology.NameToUrlMap[n],
//                    NodeTag = n,
//                    Database = database,
//                })
//                .ToArray();
//
//            for (var i = 0; i < memberCount; i++)
//            {
//                var currentNode = memberNodes[i];
//                OutgoingConnections[currentNode.NodeTag] = new List<ReplicationNode>
//                {
//                    memberNodes[(i + 1) % memberCount],
//                    memberNodes[(memberCount - i - 1) % memberCount]
//                };
//            }
//            return memberNodes;
//        }
//
//        public bool MyConnectionChanged(ReplicationNode me, ReplicationTopologyConfiguration other)
//        {
//            var myCurrentConnections = OutgoingConnections[me.NodeTag];
//            var myNewConnections = OutgoingConnections[me.NodeTag];
//
//            if (myCurrentConnections == null && myNewConnections == null)
//                return false;
//            return myCurrentConnections?.SequenceEqual(myNewConnections) ?? true;
//        }
//
//        public bool ConflictResolutionChanged(ReplicationTopologyConfiguration other)
//        {
//            if ((ResolveByCollection == null ^ other.ResolveByCollection == null) == false)
//                return true;
//
//            return (ResolveToLatest == other.ResolveToLatest &&
//                    Senator.Equals(other.Senator) &&
//                    (ResolveByCollection?.SequenceEqual(other.ResolveByCollection) ?? true)
//            );
//        }
//
//        public DynamicJsonValue ToJson()
//        {
//            return new DynamicJsonValue
//            {
//                [nameof(Senator)] = Senator?.ToJson(),
//                [nameof(OutgoingConnections)] = new DynamicJsonArray
//                {
//                    OutgoingConnections?.Select(s => new DynamicJsonValue
//                    {
//                        [nameof(s.Key)] = new DynamicJsonArray(s.Value.Select(v => v.ToJson()))
//                    })
//                },
//                [nameof(ResolveByCollection)] = new DynamicJsonArray
//                {
//                    ResolveByCollection?.Select(r => new DynamicJsonValue
//                    {
//                        [nameof(r.Key)] = r.Value.ToJson()
//                    })
//                },
//                [nameof(ResolveToLatest)] = ResolveToLatest
//            };
//        }
//    }
//}

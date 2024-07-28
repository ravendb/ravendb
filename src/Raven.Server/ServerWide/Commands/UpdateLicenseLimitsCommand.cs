using System;
using System.Linq;
using Raven.Server.Commercial;
using Raven.Server.Json;
using Raven.Server.Rachis;
using Sparrow.Json;

namespace Raven.Server.ServerWide.Commands
{
    public class UpdateLicenseLimitsCommand : UpdateValueCommand<NodeLicenseLimits>
    {
        public static int NodeInfoUpdate = -1;

        public UpdateLicenseLimitsCommand()
        {
            // for deserialization
        }

        public UpdateLicenseLimitsCommand(string name, NodeLicenseLimits nodeLicenseLimits, string uniqueRaftId) : base(uniqueRaftId)
        {
            Name = name;
            Value = nodeLicenseLimits;
        }

        public override object ValueToJson()
        {
            return Value.ToJson();
        }

        public override BlittableJsonReaderObject GetUpdatedValue(JsonOperationContext context, BlittableJsonReaderObject previousValue, long index)
        {
            if (Value == null)
                throw new RachisApplyException($"{nameof(Value)} cannot be null");

            if (Value.DetailsPerNode == null || Value.NodeTag == null || Value.AllNodes == null)
                throw new RachisApplyException($"{nameof(Value.DetailsPerNode)}, {nameof(Value.NodeTag)}, {nameof(Value.AllNodes)} cannot be null");

            if (Value.DetailsPerNode.MaxUtilizedCores != null && Value.DetailsPerNode.MaxUtilizedCores <= 0)
                throw new RachisApplyException($"{nameof(DetailsPerNode.MaxUtilizedCores)} must be greater than 0");

            var licenseLimits = previousValue == null ? new LicenseLimits() : JsonDeserializationServer.LicenseLimits(previousValue);

            RemoveNotInClusterNodes(licenseLimits);
            UpdateNodeDetails(licenseLimits);
            ReBalanceCores(licenseLimits);

            return context.ReadObject(licenseLimits.ToJson(), "update-license-limits");
        }

        private void RemoveNotInClusterNodes(LicenseLimits licenseLimits)
        {
            var nodesToRemove = licenseLimits.NodeLicenseDetails.Keys.Except(Value.AllNodes).ToList();
            foreach (var nodeToRemove in nodesToRemove)
            {
                licenseLimits.NodeLicenseDetails.Remove(nodeToRemove);
            }
        }

        private void UpdateNodeDetails(LicenseLimits licenseLimits)
        {
            if (licenseLimits.NodeLicenseDetails.TryGetValue(Value.NodeTag, out var currentDetailsPerNode))
            {
                if (Value.DetailsPerNode.UtilizedCores == NodeInfoUpdate)
                {
                    // this is a node info update, need to keep the previous max utilized cores
                    Value.DetailsPerNode.MaxUtilizedCores = currentDetailsPerNode.MaxUtilizedCores;
                }

                Value.DetailsPerNode.UtilizedCores = Value.DetailsPerNode.GetMaxCoresToUtilize(currentDetailsPerNode.UtilizedCores);
            }
            else
            {
                var availableCoresToDistribute = Value.LicensedCores - licenseLimits.TotalUtilizedCores;

                // need to "reserve" cores for nodes that aren't in license limits yet
                // we are going to distribute the available cores equally
                var unassignedNodesCount = Value.AllNodes.Except(licenseLimits.NodeLicenseDetails.Keys).Count();
                if (unassignedNodesCount == 0)
                    throw new RachisApplyException($"Node {Value.NodeTag} isn't part of the cluster, all nodes are: {string.Join(", ", Value.AllNodes)}");

                var coresPerNodeToDistribute = Math.Max(1, availableCoresToDistribute / unassignedNodesCount);
                Value.DetailsPerNode.UtilizedCores = Value.DetailsPerNode.GetMaxCoresToUtilize(coresPerNodeToDistribute);
            }

            licenseLimits.NodeLicenseDetails[Value.NodeTag] = Value.DetailsPerNode;
        }

        private void ReBalanceCores(LicenseLimits licenseLimits)
        {
            if (Value.LicensedCores < Value.AllNodes.Count)
            {
                // the number of licensed cores is less then the number of nodes
                foreach (var detailsPerNode in licenseLimits.NodeLicenseDetails.Values)
                {
                    detailsPerNode.UtilizedCores = detailsPerNode.GetMaxCoresToUtilize(1);
                }

                return;
            }

            var nodesToDistribute = licenseLimits.NodeLicenseDetails
                .OrderBy(x => x.Value.MaxCoresToUtilize)
                .ThenBy(x => x.Key)
                .Select(x => x.Value)
                .ToList();

            var unassignedNodesCount = Value.AllNodes.Except(licenseLimits.NodeLicenseDetails.Keys).Count();
            var coresToDistribute = Value.LicensedCores - unassignedNodesCount;
            if (coresToDistribute <= 0)
                throw new RachisApplyException($"Number of cores to distribute is {coresToDistribute}. " +
                                               $"The number of licensed cores is {Value.LicensedCores}. " +
                                               $"The number of unassigned nodes is {unassignedNodesCount}");

            for (var i = 0; i < nodesToDistribute.Count; i++)
            {
                var nodeDetails = nodesToDistribute[i];

                var coresToDistributePerNode = (int)Math.Ceiling((double)coresToDistribute / (nodesToDistribute.Count - i));
                var utilizedCores = nodeDetails.GetMaxCoresToUtilize(coresToDistributePerNode);
                nodeDetails.UtilizedCores = utilizedCores;
                coresToDistribute -= utilizedCores;
            }
        }
    }
}

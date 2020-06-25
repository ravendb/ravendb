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

            VerifyCoresPerNode(licenseLimits);
            RedistributeAvailableCores(licenseLimits);

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
                if (Value.DetailsPerNode.UtilizedCores == 0)
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

        private void VerifyCoresPerNode(LicenseLimits licenseLimits)
        {
            var utilizedCores = licenseLimits.TotalUtilizedCores;
            if (Value.LicensedCores >= utilizedCores)
                return;

            // we have less licensed cores than we are currently utilizing
            var coresPerNode = Math.Max(1, Value.LicensedCores / Value.AllNodes.Count);
            foreach (var detailsPerNode in licenseLimits.NodeLicenseDetails.Values)
            {
                detailsPerNode.UtilizedCores = detailsPerNode.GetMaxCoresToUtilize(coresPerNode);
            }
        }

        private void RedistributeAvailableCores(LicenseLimits licenseLimits)
        {
            var utilizedCores = licenseLimits.TotalUtilizedCores;
            if (Value.LicensedCores == utilizedCores)
                return;

            var unassignedNodesCount = Value.AllNodes.Except(licenseLimits.NodeLicenseDetails.Keys).Count();
            if (unassignedNodesCount > 0)
            {
                // there are still nodes that didn't send their info
                // once they do we'll be able to split the free cores that weren't distributed
                return;
            }

            var availableCoresToDistribute = Value.LicensedCores - utilizedCores;
            if (availableCoresToDistribute <= 0)
                return;

            // we have spare cores to distribute
            var nodesToDistribute = licenseLimits.NodeLicenseDetails
                .Select(x => x.Value)
                .Where(x => x.AvailableCoresToAssignForNode > 0)
                .OrderByDescending(x => x.AvailableCoresToAssignForNode)
                .ToList();

            for (var i = 0; i < nodesToDistribute.Count; i++)
            {
                if (availableCoresToDistribute == 0)
                    break;

                var nodeDetails = nodesToDistribute[i];
                var availableCoresToAssignForNode = nodeDetails.AvailableCoresToAssignForNode;
                if (availableCoresToAssignForNode <= 0)
                    continue;

                var coresToDistributePerNode = availableCoresToDistribute / (nodesToDistribute.Count - i);
                var numberOfCoresToAdd = Math.Min(availableCoresToAssignForNode, coresToDistributePerNode);
                nodeDetails.UtilizedCores += numberOfCoresToAdd;
                availableCoresToDistribute -= numberOfCoresToAdd;
            }
        }
    }
}

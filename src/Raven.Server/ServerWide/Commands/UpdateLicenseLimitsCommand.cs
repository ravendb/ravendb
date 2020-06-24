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

            if (Value.DetailsPerNode.CustomUtilizedCores && Value.DetailsPerNode.UtilizedCores == 0)
                throw new RachisApplyException(
                    $"{nameof(Value.DetailsPerNode.UtilizedCores)} cannot be 0 while the {nameof(Value.DetailsPerNode.CustomUtilizedCores)} is set");

            if (Value.DetailsPerNode.NumberOfCores < Value.DetailsPerNode.UtilizedCores)
                throw new RachisApplyException($"The utilized cores count: {Value.DetailsPerNode.UtilizedCores} " +
                                               $"is larger than the number of cores in the node: {Value.DetailsPerNode.NumberOfCores}");

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
            try
            {
                if (Value.DetailsPerNode.CustomUtilizedCores)
                {
                    // setting a custom number of utilized cores
                    return;
                }

                if (licenseLimits.NodeLicenseDetails.TryGetValue(Value.NodeTag, out var currentDetailsPerNode))
                {
                    if (currentDetailsPerNode.CustomUtilizedCores)
                    {
                        // don't change the currently utilized cores
                        Value.DetailsPerNode.CustomUtilizedCores = true;
                        Value.DetailsPerNode.UtilizedCores = Math.Min(Value.DetailsPerNode.NumberOfCores, currentDetailsPerNode.UtilizedCores);
                        return;
                    }

                    if (currentDetailsPerNode.NumberOfCores == Value.DetailsPerNode.NumberOfCores)
                    {
                        // the number of cores on the node hasn't changed, nothing to do
                        Value.DetailsPerNode.UtilizedCores = currentDetailsPerNode.UtilizedCores;
                        return;
                    }
                }

                licenseLimits.NodeLicenseDetails.Remove(Value.NodeTag);
                var availableCoresToDistribute = Value.LicensedCores - licenseLimits.TotalUtilizedCores;

                // need to "reserve" cores for nodes that aren't in license limits yet
                // we are going to distribute the available cores equally
                var unassignedNodesCount = Value.AllNodes.Except(licenseLimits.NodeLicenseDetails.Keys).Count();
                var coresPerNodeToDistribute = availableCoresToDistribute / unassignedNodesCount;

                Value.DetailsPerNode.UtilizedCores = coresPerNodeToDistribute > 0
                    ? Math.Min(Value.DetailsPerNode.NumberOfCores, coresPerNodeToDistribute)
                    : 1;
            }
            finally
            {
                if (Value.DetailsPerNode.UtilizedCores == 0)
                    throw new RachisApplyException($"Trying to set {nameof(Value.DetailsPerNode.UtilizedCores)} to 0");

                licenseLimits.NodeLicenseDetails[Value.NodeTag] = Value.DetailsPerNode;
                ResetCustomUtilizedCoresPropertyIfNeeded(licenseLimits, Value.DetailsPerNode);
            }
        }

        private void VerifyCoresPerNode(LicenseLimits licenseLimits)
        {
            var utilizedCores = licenseLimits.TotalUtilizedCores;
            if (Value.LicensedCores == utilizedCores)
                return;

            if (Value.LicensedCores >= utilizedCores)
                return;

            // we have less licensed cores than we are currently utilizing
            var coresPerNode = Math.Max(1, Value.LicensedCores / Value.AllNodes.Count);
            foreach (var nodeDetails in licenseLimits.NodeLicenseDetails)
            {
                nodeDetails.Value.UtilizedCores = Math.Min(coresPerNode, nodeDetails.Value.NumberOfCores);
                ResetCustomUtilizedCoresPropertyIfNeeded(licenseLimits, nodeDetails.Value);
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

            var freeCoresToDistribute = Value.LicensedCores - utilizedCores;
            if (freeCoresToDistribute <= 0)
                return;

            // we have spare cores to distribute
            var nodesToDistribute = licenseLimits.NodeLicenseDetails
                .Select(x => x.Value)
                .Where(x => x.CustomUtilizedCores == false && x.NumberOfCores - x.UtilizedCores > 0)
                .OrderBy(x => x.NumberOfCores - x.UtilizedCores)
                .ToList();

            for (var i = 0; i < nodesToDistribute.Count; i++)
            {
                if (freeCoresToDistribute == 0)
                    break;

                var nodeDetails = nodesToDistribute[i];
                var availableCoresToAssignForNode = nodeDetails.NumberOfCores - nodeDetails.UtilizedCores;
                if (availableCoresToAssignForNode <= 0)
                    continue;

                var coresToDistributePerNode = freeCoresToDistribute / (nodesToDistribute.Count - i);
                var numberOfCoresToAdd = Math.Min(availableCoresToAssignForNode, coresToDistributePerNode);
                nodeDetails.UtilizedCores += numberOfCoresToAdd;
                freeCoresToDistribute -= numberOfCoresToAdd;
                ResetCustomUtilizedCoresPropertyIfNeeded(licenseLimits, nodeDetails);
            }
        }

        private void ResetCustomUtilizedCoresPropertyIfNeeded(LicenseLimits licenseLimits, DetailsPerNode detailsPerNode)
        {
            if (detailsPerNode.CustomUtilizedCores == false)
                return;

            if (detailsPerNode.UtilizedCores == detailsPerNode.NumberOfCores || 
                licenseLimits.TotalUtilizedCores == Value.LicensedCores)
            {
                detailsPerNode.CustomUtilizedCores = false;
            }
        }
    }
}

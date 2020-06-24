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
                throw new RachisApplyException($"{nameof(Value.DetailsPerNode.UtilizedCores)} cannot be 0 while the {nameof(Value.DetailsPerNode.CustomUtilizedCores)} is set");

            if (Value.DetailsPerNode.NumberOfCores < Value.DetailsPerNode.UtilizedCores)
                throw new RachisApplyException($"The utilized cores count: {Value.DetailsPerNode.UtilizedCores} " +
                                               $"is larger than the number of cores in the node: {Value.DetailsPerNode.NumberOfCores}");

            var licenseLimits = previousValue == null ? new LicenseLimits() : JsonDeserializationServer.LicenseLimits(previousValue);

            RemoveNotInClusterNodes(licenseLimits);
            TryUpdatingNodeDetailsCores(licenseLimits);
            if (Value.DetailsPerNode.UtilizedCores == 0)
                throw new RachisApplyException($"Trying to set {nameof(Value.DetailsPerNode.UtilizedCores)} to 0");

            licenseLimits.NodeLicenseDetails[Value.NodeTag] = Value.DetailsPerNode;
            VerifyCoresPerNode(licenseLimits);

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

        private void TryUpdatingNodeDetailsCores(LicenseLimits licenseLimits)
        {
            if (Value.DetailsPerNode.CustomUtilizedCores)
            {
                // setting a custom number of utilized cores
                SetCustomUtilizedCoresProperty();
                return;
            }

            if (licenseLimits.NodeLicenseDetails.TryGetValue(Value.NodeTag, out var currentDetailsPerNode))
            {
                if (currentDetailsPerNode.CustomUtilizedCores)
                {
                    // don't change the currently utilized cores
                    Value.DetailsPerNode.UtilizedCores = Math.Min(Value.DetailsPerNode.NumberOfCores, currentDetailsPerNode.UtilizedCores);
                    SetCustomUtilizedCoresProperty();
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
            var totalUtilizedCores = licenseLimits.NodeLicenseDetails.Sum(x => x.Value.UtilizedCores);
            var availableCoresToDistribute = Value.LicensedCores - totalUtilizedCores;

            // need to "reserve" cores for nodes that aren't in license limits yet
            // we are going to distribute the available cores equally
            var unassignedNodesCount = Value.AllNodes.Except(licenseLimits.NodeLicenseDetails.Keys).Count();
            var coresPerNodeToDistribute = availableCoresToDistribute / unassignedNodesCount;

            Value.DetailsPerNode.UtilizedCores = coresPerNodeToDistribute > 0
                ? Math.Min(Value.DetailsPerNode.NumberOfCores, coresPerNodeToDistribute)
                : 1;

            void SetCustomUtilizedCoresProperty()
            {
                // remove it if we are utilizing all the available cores
                Value.DetailsPerNode.CustomUtilizedCores = Value.DetailsPerNode.UtilizedCores < Value.DetailsPerNode.NumberOfCores;
            }
        }

        private void VerifyCoresPerNode(LicenseLimits licenseLimits)
        {
            var utilizedCores = licenseLimits.NodeLicenseDetails.Sum(x => x.Value.UtilizedCores);
            if (Value.LicensedCores == utilizedCores)
                return;

            if (Value.LicensedCores < utilizedCores)
            {
                // we have less licensed cores than we are currently utilizing
                var coresPerNode = Math.Max(1, Value.LicensedCores / Value.AllNodes.Count);
                foreach (var nodeDetails in licenseLimits.NodeLicenseDetails)
                {
                    nodeDetails.Value.UtilizedCores = Math.Min(coresPerNode, nodeDetails.Value.NumberOfCores);
                }
            }

            var unassignedNodesCount = Value.AllNodes.Except(licenseLimits.NodeLicenseDetails.Keys).Count();
            if (unassignedNodesCount > 0)
            {
                // there are still nodes that didn't send their info
                // once they do we'll be able to split the free cores that weren't distributed
                return;
            }

            // we have spare cores to distribute
            var freeCoresToDistribute = Value.LicensedCores - utilizedCores;

            foreach (var node in licenseLimits.NodeLicenseDetails)
            {
                if (freeCoresToDistribute == 0)
                    break;

                var nodeDetails = node.Value;
                if (nodeDetails.CustomUtilizedCores)
                    continue;

                var availableCoresToAssignForNode = nodeDetails.NumberOfCores - nodeDetails.UtilizedCores;
                if (availableCoresToAssignForNode <= 0)
                    continue;

                var numberOfCoresToAdd = Math.Min(availableCoresToAssignForNode, freeCoresToDistribute);
                nodeDetails.UtilizedCores += numberOfCoresToAdd;
                freeCoresToDistribute -= numberOfCoresToAdd;
            }
        }
    }
}

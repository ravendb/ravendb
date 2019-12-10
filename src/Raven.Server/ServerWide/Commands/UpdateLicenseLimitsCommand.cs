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

            if (previousValue == null)
                throw new RachisApplyException("Cannot apply node details to a non existing license limits");

            var licenseLimits = JsonDeserializationServer.LicenseLimits(previousValue);
            if (licenseLimits.NodeLicenseDetails.TryGetValue(Value.NodeTag, out var currentDetailsPerNode) == false)
                return null;

            if (Value.DetailsPerNode.NumberOfCores == currentDetailsPerNode.NumberOfCores)
            {
                // the number of cores on this node hasn't changed, keep the old value
                Value.DetailsPerNode.UtilizedCores = currentDetailsPerNode.UtilizedCores;
            }
            else
            {
                var utilizedCores = licenseLimits.NodeLicenseDetails.Sum(x => x.Value.UtilizedCores);
                var availableCoresToDistribute = Value.LicensedCores - utilizedCores + currentDetailsPerNode.UtilizedCores;

                // nodes that aren't in yet in license limits, need to "reserve" cores for them
                var nodesThatArentRegistered = licenseLimits.NodeLicenseDetails.Keys.Except(Value.AllNodes).Count();
                availableCoresToDistribute -= nodesThatArentRegistered;

                Value.DetailsPerNode.UtilizedCores = availableCoresToDistribute > 0
                    ? Math.Min(Value.DetailsPerNode.NumberOfCores, availableCoresToDistribute)
                    : 1;
            }

            licenseLimits.NodeLicenseDetails[Value.NodeTag] = Value.DetailsPerNode;

            return context.ReadObject(licenseLimits.ToJson(), "update-license-limits");
        }
    }
}

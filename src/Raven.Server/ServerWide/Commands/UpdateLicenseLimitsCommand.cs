using System;
using System.Linq;
using Raven.Server.Commercial;
using Raven.Server.Json;
using Raven.Server.Rachis;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.ServerWide.Commands
{
    public class UpdateLicenseLimitsCommand : UpdateValueCommand<NodeLicenseLimits>
    {
        public UpdateLicenseLimitsCommand()
        {
            // for deserialization
        }

        public UpdateLicenseLimitsCommand(string name, string nodeTag, DetailsPerNode detailsPerNode, int licensedCores, string uniqueRaftId) : base(uniqueRaftId)
        {
            Name = name;
            Value = new NodeLicenseLimits
            {
                NodeTag = nodeTag,
                DetailsPerNode = detailsPerNode,
                LicensedCores = licensedCores
            };
        }

        public override object ValueToJson()
        {
            return Value.ToJson();
        }

        public override BlittableJsonReaderObject GetUpdatedValue(JsonOperationContext context, BlittableJsonReaderObject previousValue, long index)
        {
            if (Value == null)
                throw new RachisApplyException($"{nameof(Value)} cannot be null");

            if (Value.DetailsPerNode == null || Value.NodeTag == null)
                throw new RachisApplyException($"{nameof(Value.DetailsPerNode)}, {nameof(Value.NodeTag)} cannot be null");

            if (previousValue == null)
                throw new RachisApplyException("Cannot apply node details to a non existing license limits");

            var licenseLimits = JsonDeserializationServer.LicenseLimits(previousValue);
            if (licenseLimits.NodeLicenseDetails.TryGetValue(Value.NodeTag, out var currentDetailsPerNode) == false)
                return null;

            var utilizedCores = licenseLimits.NodeLicenseDetails.Sum(x => x.Value.UtilizedCores);
            var availableCoresToDistribute = Value.LicensedCores - utilizedCores + currentDetailsPerNode.UtilizedCores;

            Value.DetailsPerNode.UtilizedCores = availableCoresToDistribute > 0 
                ? Math.Min(Value.DetailsPerNode.NumberOfCores, availableCoresToDistribute) 
                : 1;

            licenseLimits.NodeLicenseDetails[Value.NodeTag] = Value.DetailsPerNode;

            return context.ReadObject(licenseLimits.ToJson(), "update-license-limits");
        }
    }

    public class NodeLicenseLimits : IDynamicJson
    {
        public string NodeTag { get; set; }

        public DetailsPerNode DetailsPerNode { get; set; }

        public int LicensedCores { get; set; }

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(NodeTag)] = NodeTag,
                [nameof(DetailsPerNode)] = DetailsPerNode.ToJson(),
                [nameof(LicensedCores)] = LicensedCores
            };
        }
    }
}

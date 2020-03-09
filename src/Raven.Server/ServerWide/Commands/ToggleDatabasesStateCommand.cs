using Raven.Server.Utils;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.ServerWide.Commands
{
    public class ToggleDatabasesStateCommand : UpdateValueCommand<ToggleParameters>
    {
        protected ToggleDatabasesStateCommand()
        {
            // for deserialization
        }

        public ToggleDatabasesStateCommand(ToggleParameters toggleParameters, string uniqueRequestId) : base(uniqueRequestId)
        {
            Value = toggleParameters;
        }

        public override object ValueToJson()
        {
            return Value.ToJson();
        }

        public override BlittableJsonReaderObject GetUpdatedValue(JsonOperationContext context, BlittableJsonReaderObject previousValue, long index)
        {
            return null;
        }
    }

    public class ToggleParameters : IDynamicJson
    {
        public ToggleType ToggleType { get; set; }

        public string[] DatabaseNames { get; set; }

        public bool State { get; set; }

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(ToggleType)] = ToggleType,
                [nameof(DatabaseNames)] = TypeConverter.ToBlittableSupportedType(DatabaseNames),
                [nameof(State)] = State
            };
        }
    }

    public enum ToggleType
    {
        Databases,
        Indexes,
        DynamicDatabaseDistribution
    }
}

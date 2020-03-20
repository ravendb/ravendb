using Raven.Server.Utils;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.ServerWide.Commands
{
    public class ToggleDatabasesStateCommand : UpdateValueCommand<ToggleDatabasesStateCommand.Parameters>
    {
        protected ToggleDatabasesStateCommand()
        {
            // for deserialization
        }

        public ToggleDatabasesStateCommand(Parameters parameters, string uniqueRequestId) : base(uniqueRequestId)
        {
            Value = parameters;
        }

        public override object ValueToJson()
        {
            return Value.ToJson();
        }

        public override BlittableJsonReaderObject GetUpdatedValue(JsonOperationContext context, BlittableJsonReaderObject previousValue, long index)
        {
            return null;
        }

        public class Parameters : IDynamicJson
        {
            public ToggleType Type { get; set; }

            public string[] DatabaseNames { get; set; }

            public bool Disable { get; set; }

            public DynamicJsonValue ToJson()
            {
                return new DynamicJsonValue
                {
                    [nameof(Type)] = Type,
                    [nameof(DatabaseNames)] = TypeConverter.ToBlittableSupportedType(DatabaseNames),
                    [nameof(Disable)] = Disable
                };
            }

            public enum ToggleType
            {
                Databases,
                Indexes,
                DynamicDatabaseDistribution
            }
        }
    }
}

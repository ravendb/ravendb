using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.ServerWide.Commands
{
    public abstract class UpdateValueCommand<T> : CommandBase
    {
        public string Name;

        public T Value;

        public override DynamicJsonValue ToJson(JsonOperationContext context)
        {
            var djv = base.ToJson(context);
            djv[nameof(Name)] = Name;
            djv[nameof(Value)] = ValueToJson();

            return djv;
        }

        public abstract object ValueToJson();

        public abstract BlittableJsonReaderObject GetUpdatedValue(JsonOperationContext context, BlittableJsonReaderObject previousValue);
    }
}

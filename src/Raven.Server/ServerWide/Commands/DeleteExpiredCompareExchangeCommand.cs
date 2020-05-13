using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.ServerWide.Commands
{
    public class DeleteExpiredCompareExchangeCommand : CommandBase
    {
        public long Ticks;
        public long Take;
        public DeleteExpiredCompareExchangeCommand()
        { }

        public DeleteExpiredCompareExchangeCommand(long ticks, long take, string uniqueRequestId) : base(uniqueRequestId)
        {
            Ticks = ticks;
            Take = take;
        }

        public override DynamicJsonValue ToJson(JsonOperationContext context)
        {
            var json = base.ToJson(context);
            json[nameof(Ticks)] = Ticks;
            json[nameof(Take)] = Take;
            return json;
        }
    }
}

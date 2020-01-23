using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.ServerWide.Commands
{
    public class CleanCompareExchangeTombstonesCommand : CommandBase
    {
        public string DatabaseName;
        public long MaxRaftIndex;
        public long Take;

        public CleanCompareExchangeTombstonesCommand()
        {}

        public CleanCompareExchangeTombstonesCommand(string databaseName, long maxRaftIndex, long take, string uniqueRequestId) : base(uniqueRequestId)
        {
            DatabaseName = databaseName;
            MaxRaftIndex = maxRaftIndex;
            Take = take;
        }

        public override DynamicJsonValue ToJson(JsonOperationContext context)
        {
            var json = base.ToJson(context);
            json[nameof(DatabaseName)] = DatabaseName;
            json[nameof(MaxRaftIndex)] = MaxRaftIndex;
            json[nameof(Take)] = Take;
            return json;
        }
    }
}

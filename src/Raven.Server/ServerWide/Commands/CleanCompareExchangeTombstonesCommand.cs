using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.ServerWide.Commands
{
    public class CleanCompareExchangeTombstonesCommand : CommandBase
    {
        public readonly string DatabaseName;
        public readonly long MaxRaftIndex;

        public CleanCompareExchangeTombstonesCommand()
        {}

        public CleanCompareExchangeTombstonesCommand(string databaseName, long maxRaftIndex)
        {
            DatabaseName = databaseName;
            MaxRaftIndex = maxRaftIndex;
        }

        public override DynamicJsonValue ToJson(JsonOperationContext context)
        {
            var json = base.ToJson(context);
            json[nameof(DatabaseName)] = DatabaseName;
            json[nameof(MaxRaftIndex)] = MaxRaftIndex;
            return json;
        }
    }
}

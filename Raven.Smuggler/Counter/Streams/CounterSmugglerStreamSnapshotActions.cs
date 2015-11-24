using System.Threading;
using System.Threading.Tasks;

using Raven.Abstractions.Counters;
using Raven.Abstractions.Util;
using Raven.Imports.Newtonsoft.Json;
using Raven.Smuggler.Common;

namespace Raven.Smuggler.Counter.Streams
{
    public class CounterSmugglerStreamSnapshotActions : SmugglerStreamActionsBase, ICounterSmugglerSnapshotActions
    {
        public CounterSmugglerStreamSnapshotActions(JsonTextWriter writer)
            : base(writer, "CounterSnapshots")
        {
        }

        public Task WriteSnapshotAsync(CounterSummary snapshot, CancellationToken cancellationToken)
        {
            Writer.WriteStartObject();
            Writer.WritePropertyName("Group");
            Writer.WriteValue(snapshot.GroupName);

            Writer.WritePropertyName("Name");
            Writer.WriteValue(snapshot.CounterName);
            Writer.WritePropertyName("Positive");
            Writer.WriteValue(snapshot.Increments);

            Writer.WritePropertyName("Negative");
            Writer.WriteValue(snapshot.Decrements);

            Writer.WriteEndObject();

            return new CompletedTask();
        }
    }
}
using System.Threading;
using System.Threading.Tasks;

using Raven.Abstractions.Util;
using Raven.Database.Counters;
using Raven.Imports.Newtonsoft.Json;
using Raven.Smuggler.Common;

namespace Raven.Smuggler.Counter.Streams
{
    public class CounterSmugglerStreamDeltaActions : SmugglerStreamActionsBase, ICounterSmugglerDeltaActions
    {
        public CounterSmugglerStreamDeltaActions(JsonTextWriter writer)
            : base(writer, "CountersDeltas")
        {
        }

        public Task WriteDeltaAsync(CounterState delta, CancellationToken cancellationToken)
        {
            Writer.WriteStartObject();
            Writer.WritePropertyName("CounterName");
            Writer.WriteValue(delta.CounterName);

            Writer.WritePropertyName("GroupName");
            Writer.WriteValue(delta.GroupName);

            Writer.WritePropertyName("Sign");
            Writer.WriteValue(delta.Sign);

            Writer.WritePropertyName("Value");
            Writer.WriteValue(delta.Value);
            Writer.WriteEndObject();

            return new CompletedTask();
        }
    }
}
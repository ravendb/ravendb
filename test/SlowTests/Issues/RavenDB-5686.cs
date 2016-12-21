using FastTests;
using Raven.Client.Connection;
using Raven.Client.Document;
using Raven.Json.Linq;
using Raven.Server.Utils.Metrics;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_5686 : NoDisposalNeeded
    {
        [Fact]
        public void CanSerializeAndDeserializeMeterValue()
        {
            var meter = new MeterValue("name", 1, 2.0, 3.0, 4.0, 15.0);
            var jObject = RavenJObject.FromObject(meter);
            var parsed = jObject.Deserialize<MeterValue>(new DocumentConvention());

            Assert.Equal(meter.Name, parsed.Name);

            Assert.Equal(meter.Count, parsed.Count);
            Assert.Equal(meter.MeanRate, parsed.MeanRate);

            Assert.Equal(meter.OneMinuteRate, parsed.OneMinuteRate);
            Assert.Equal(meter.FiveMinuteRate, parsed.FiveMinuteRate);
            Assert.Equal(meter.FifteenMinuteRate, parsed.FifteenMinuteRate);
        }
    }
}
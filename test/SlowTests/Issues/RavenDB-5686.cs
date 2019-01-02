using FastTests;
using Raven.Client.Documents.Session;
using Raven.Server.Utils.Metrics;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_5686 : RavenTestBase
    {
        [Fact]
        public void CanSerializeAndDeserializeMeterValue()
        {
            var meter = new MeterValue("name", 1, 2.0, 3.0, 4.0, 15.0);

            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    var json = EntityToBlittable.ConvertCommandToBlittable(meter, session.Advanced.Context);
                    var parsed = (MeterValue)session.Advanced.EntityToBlittable.ConvertToEntity(typeof(MeterValue), "meter", ref json);

                    Assert.Equal(meter.Name, parsed.Name);

                    Assert.Equal(meter.Count, parsed.Count);
                    Assert.Equal(meter.MeanRate, parsed.MeanRate);

                    Assert.Equal(meter.OneMinuteRate, parsed.OneMinuteRate);
                    Assert.Equal(meter.FiveMinuteRate, parsed.FiveMinuteRate);
                    Assert.Equal(meter.FifteenMinuteRate, parsed.FifteenMinuteRate);
                }
            }
        }
    }
}

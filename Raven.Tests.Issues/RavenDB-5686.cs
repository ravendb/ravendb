using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Linq.Expressions;
using Raven.Abstractions.Util.MiniMetrics;
using Raven.Client.Connection;
using Raven.Client.Document;
using Raven.Client.Linq;
using Xunit;
using Raven.Client.Indexes;
using Raven.Json.Linq;
using Raven.Tests.Helpers;

namespace Raven.Tests.Issues
{
    public class RavenDB_5686
    {
        [Fact]
        public void CanSerializeAndDeserializeMeterValue()
        {
            var meter = new MeterValue(1, 2.0, 3.0, 4.0, 15.0);
            var jObject = RavenJObject.FromObject(meter);
            var parsed = jObject.Deserialize<MeterValue>(new DocumentConvention());

            Assert.Equal(meter.Count, parsed.Count);
            Assert.Equal(meter.MeanRate, parsed.MeanRate);

            Assert.Equal(meter.OneMinuteRate, parsed.OneMinuteRate);
            Assert.Equal(meter.FiveMinuteRate, parsed.FiveMinuteRate);
            Assert.Equal(meter.FifteenMinuteRate, parsed.FifteenMinuteRate);
        }
    }
}
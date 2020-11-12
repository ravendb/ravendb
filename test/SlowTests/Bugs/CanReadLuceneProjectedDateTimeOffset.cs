using System;
using System.IO;
using FastTests;
using Newtonsoft.Json;
using Raven.Client.Documents.Conventions;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Bugs
{
    public class CanReadLuceneProjectedDateTimeOffset : RavenTestBase
    {
        public CanReadLuceneProjectedDateTimeOffset(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void Can_read_date_time_offset_from_lucene_query()
        {
            var jsonSerializer = (JsonSerializer)DocumentConventions.Default.Serialization.CreateDeserializer();

            using (var reader = new JsonTextReader(new StringReader(@"{""Item"": ""20090402193554412""}")))
            {
                var deserialize = jsonSerializer.Deserialize<Test>(reader);
                Assert.Equal(2009, deserialize.Item.Year);
            }
        }

        private class Test
        {
            public DateTimeOffset Item { get; set; }
        }
    }
}

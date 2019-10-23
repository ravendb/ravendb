using System;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Issues
{
    public class RavenDB_7654 : NoDisposalNeeded
    {
        public RavenDB_7654(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void TimeSpanCanBeParsedFromStringIntAndDouble()
        {
            var deserialize = JsonDeserializationBase.GenerateJsonDeserializationRoutine<Test>();

            using (var context = JsonOperationContext.ShortTermSingleUse())
            {
                var ts = TimeSpan.FromMinutes(77);

                var djv = new DynamicJsonValue
                {
                    [nameof(Test.NotNullable)] = ts,
                    [nameof(Test.Nullable)] = ts
                };

                var json = context.ReadObject(djv, "json");

                var result = deserialize.Invoke(json);
                Assert.Equal(ts, result.NotNullable);
                Assert.Equal(ts, result.Nullable);

                djv = new DynamicJsonValue
                {
                    [nameof(Test.NotNullable)] = ts.TotalMilliseconds,
                    [nameof(Test.Nullable)] = (long)ts.TotalMilliseconds
                };

                json = context.ReadObject(djv, "json");

                result = deserialize.Invoke(json);
                Assert.Equal(ts, result.NotNullable);
                Assert.Equal(ts, result.Nullable);

                djv = new DynamicJsonValue
                {
                    [nameof(Test.NotNullable)] = ts.TotalMilliseconds,
                    [nameof(Test.Nullable)] = null
                };

                json = context.ReadObject(djv, "json");

                result = deserialize.Invoke(json);
                Assert.Equal(ts, result.NotNullable);
                Assert.Null(result.Nullable);
            }
        }

        private class Test
        {
            public TimeSpan? Nullable { get; set; }

            public TimeSpan NotNullable { get; set; }
        }
    }
}

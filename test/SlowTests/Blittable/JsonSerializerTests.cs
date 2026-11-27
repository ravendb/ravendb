using System.IO;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Conventions;
using Raven.Server.Documents;
using Sparrow;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Json.Sync;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Blittable
{
    public class JsonSerializerTests : RavenTestBase
    {
        public JsonSerializerTests(ITestOutputHelper output) : base(output)
        {
        }

        public class Command
        {
            public BlittableJsonReaderArray BlittableArray { get; set; }
            public BlittableJsonReaderObject BlittableObject { get; set; }
            public LazyStringValue LazyString { get; set; }
        }

        [RavenFact(RavenTestCategory.Core)]
        public void JsonDeserialize_WhenBlittableArrayBlittableObjectAndLazyStringAreNull_ShouldResultInCopy()
        {
            using (Server.ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext readContext))
            using (Server.ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext writeContext))
            {
                var toDeseialize = ReplayTxCommandHelper.Serialize(writeContext, new Command());
                var actual = ReplayTxCommandHelper.Deserialize<Command>(readContext, toDeseialize);

                Assert.Null(actual.BlittableArray);
                Assert.Null(actual.BlittableObject);
                Assert.Null(actual.LazyString);
            }
        }

        [RavenFact(RavenTestCategory.Core)]
        public void JsonDeserialize_WhenHasBlittableJsonReaderArrayProperty_ShouldResultInCopy()
        {
            using (Server.ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext readContext))
            using (Server.ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext writeContext))
            {
                var data = new { Property = new[] { "Value1", "Value2" } };
                var readerObject = DocumentConventions.DefaultForServer.Serialization.DefaultConverter.ToBlittable(data, readContext);
                readerObject.TryGet(nameof(data.Property), out BlittableJsonReaderArray expected);

                var toDeseialize = ReplayTxCommandHelper.Serialize(writeContext, new Command { BlittableArray = expected });
                var actual = ReplayTxCommandHelper.Deserialize<Command>(readContext, toDeseialize);

                Assert.Equal(expected, actual.BlittableArray);
            }
        }

        [RavenFact(RavenTestCategory.Core)]
        public async Task JsonDeserialize_WhenHasBlittableObjectPropertyAndWriteAndReadFromStream_ShouldResultInCommandWithTheProperty()
        {
            using (Server.ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext context))
            {
                var data = new { SomeProperty = "SomeValue" };
                var expected = DocumentConventions.DefaultForServer.Serialization.DefaultConverter.ToBlittable(data, context);
                var command = new Command { BlittableObject = expected };

                //Serialize
                BlittableJsonReaderObject toStream = ReplayTxCommandHelper.Serialize(context, command);

                //Simulates copying to file and loading
                BlittableJsonReaderObject fromStream;
                using (Stream stream = new MemoryStream())
                {
                    //Pass to stream
                    await using (var textWriter = new AsyncBlittableJsonTextWriter(context, stream))
                    {
                        context.Write(textWriter, toStream);
                    }

                    //Get from stream
                    stream.Position = 0;

                    var state = new JsonParserState();
                    var parser = new UnmanagedJsonParser(context, state, "some tag");
                    var peepingTomStream = new PeepingTomStream(stream, context);

                    using (context.GetMemoryBuffer(out var buffer))
                    using (var builder =
                        new BlittableJsonDocumentBuilder(context, BlittableJsonDocumentBuilder.UsageMode.None, "some tag", parser, state))
                    {
                        await UnmanagedJsonParserHelper.ReadAsync(peepingTomStream, parser, state, buffer);
                        await UnmanagedJsonParserHelper.ReadObjectAsync(builder, peepingTomStream, parser, buffer);

                        fromStream = builder.CreateReader();
                    }
                }

                //Deserialize
                BlittableJsonReaderObject actual = ReplayTxCommandHelper.Deserialize<Command>(context, fromStream).BlittableObject;

                Assert.Equal(expected, actual);
            }
        }

        [RavenFact(RavenTestCategory.Core)]
        public void JsonDeserialize_WhenHasLazyStringProperty_ShouldResultInCopy()
        {
            using (Server.ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext readContext))
            using (Server.ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext writeContext))
            {
                LazyStringValue expected = readContext.GetLazyString("Some Lazy String");

                var toDeseialize = ReplayTxCommandHelper.Serialize(writeContext, new Command { LazyString = expected });
                var actual = ReplayTxCommandHelper.Deserialize<Command>(readContext, toDeseialize);

                Assert.Equal(expected, actual.LazyString);
            }
        }

        [RavenFact(RavenTestCategory.Core)]
        public void JsonDeserialize_WhenHasBlittableObjectProperty_ShouldResultInCopy()
        {
            using (Server.ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext readContext))
            using (Server.ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext writeContext))
            {
                var data = new { Property = "Value" };
                BlittableJsonReaderObject expected = DocumentConventions.DefaultForServer.Serialization.DefaultConverter.ToBlittable(data, readContext);

                var toDeseialize = ReplayTxCommandHelper.Serialize(writeContext, new Command { BlittableObject = expected });
                var actual = ReplayTxCommandHelper.Deserialize<Command>(readContext, toDeseialize);

                Assert.Equal(expected, actual.BlittableObject);
            }
        }

        [RavenFact(RavenTestCategory.Core)]
        public void JsonDeserialize_WhenBlittableObjectPropertyIsDeeplyNested_ShouldResultInCopy()
        {
            using (Server.ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext readContext))
            using (Server.ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext writeContext))
            {
                var data = new DynamicJsonValue { ["Property"] = "Value" };
                for (int i = 0; i < 200; i++)
                    data = new DynamicJsonValue { ["Nested"] = data };

                BlittableJsonReaderObject expected = readContext.ReadObject(data, "deep");

                var toDeseialize = ReplayTxCommandHelper.Serialize(writeContext, new Command { BlittableObject = expected });
                var actual = ReplayTxCommandHelper.Deserialize<Command>(readContext, toDeseialize);

                Assert.Equal(expected, actual.BlittableObject);
            }
        }

        [RavenFact(RavenTestCategory.Core)]
        public void JsonDeserialize_WhenBlittableObjectPropertyHasHighPrecisionNumbers_ShouldKeepTheirText()
        {
            const string precise = "0.12345678901234567890123456789012345";
            const string exponent = "1.5E+300";
            const string whole = "1.0";
            const string item = "-98765432109876543210987654321.000000000000000000001";

            using (Server.ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext readContext))
            using (Server.ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext writeContext))
            using (BlittableJsonReaderObject expected = readContext.Sync.ReadForMemory(
                       $"{{\"Precise\":{precise},\"Exponent\":{exponent},\"Whole\":{whole},\"Integer\":42,\"Items\":[{item}]}}", "numbers"))
            {
                var toDeseialize = ReplayTxCommandHelper.Serialize(writeContext, new Command { BlittableObject = expected });
                var actual = ReplayTxCommandHelper.Deserialize<Command>(readContext, toDeseialize);

                Assert.True(actual.BlittableObject.TryGet("Precise", out LazyNumberValue actualPrecise));
                Assert.Equal(precise, actualPrecise.Inner.ToString());
                Assert.True(actual.BlittableObject.TryGet("Exponent", out LazyNumberValue actualExponent));
                Assert.Equal(exponent, actualExponent.Inner.ToString());
                Assert.True(actual.BlittableObject.TryGet("Whole", out LazyNumberValue actualWhole));
                Assert.Equal(whole, actualWhole.Inner.ToString());
                Assert.True(actual.BlittableObject.TryGet("Integer", out long actualInteger));
                Assert.Equal(42, actualInteger);
                Assert.True(actual.BlittableObject.TryGet("Items", out BlittableJsonReaderArray actualItems));
                Assert.Equal(item, ((LazyNumberValue)actualItems[0]).Inner.ToString());
            }
        }

        [RavenFact(RavenTestCategory.Core)]
        public void JsonSerialize_WhenLazyStringValueIsProperty_ShouldSerialize()
        {
            using (Server.ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext context))
            {
                var expected = context.GetLazyString("igal");

                var reader = ReplayTxCommandHelper.Serialize(context, new Command { LazyString = expected });

                reader.TryGet(nameof(Command.LazyString), out LazyStringValue actual);
                Assert.Equal(expected, actual);
            }
        }

        [RavenFact(RavenTestCategory.Core)]
        public void JsonSerialize_WhenBlittableObjectIsProperty_ShouldResultInCopy()
        {
            using (Server.ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext context))
            {
                var data = new { Property = "Value" };
                var expected = DocumentConventions.DefaultForServer.Serialization.DefaultConverter.ToBlittable(data, context);

                var reader = ReplayTxCommandHelper.Serialize(context, new Command { BlittableObject = expected });

                reader.TryGet(nameof(Command.BlittableObject), out BlittableJsonReaderObject actual);
                Assert.Equal(expected, actual);
            }
        }

        [RavenFact(RavenTestCategory.Core)]
        public void JsonSerialize_WhenNestedBlittableObjectIsProperty_ShouldSerialize()
        {
            using (Server.ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext context))
            using (Server.ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext context2))
            {
                var data = new { ParentProperty = new { NestedProperty = "Some Value" } };
                var parentBlittable = DocumentConventions.DefaultForServer.Serialization.DefaultConverter.ToBlittable(data, context);
                parentBlittable.TryGet(nameof(data.ParentProperty), out BlittableJsonReaderObject expected);

                var reader = ReplayTxCommandHelper.Serialize(context2, new Command { BlittableObject = expected });

                reader.TryGet(nameof(Command.BlittableObject), out BlittableJsonReaderObject actual);
                Assert.Equal(expected, actual);
            }
        }
    }
}

using System;
using System.Collections.Generic;
using System.Text;
using FastTests;
using Newtonsoft.Json;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Session;
using Raven.Client.Json;
using Raven.Client.Json.Converters;
using Raven.Server.Documents.Handlers;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Xunit;

namespace SlowTests.Blittable
{
    public class JsonConverterTests : RavenTestBase
    {
        public class Command
        {
            //Todo To consider if should deal with BlittableJsonReaderArray
            //        public BlittableJsonReaderArray BlittableArray { get; set; }
            public BlittableJsonReaderObject BlittableObject { get; set; }
            public LazyStringValue LazyString { get; set; }
        }

        [Fact]
        public void JsonConvert_WhenLazyStringValueIsProperty_ShouldSerialize()
        {
            using (Server.ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext context))
            using (var writer = new BlittableJsonWriter(context))
            {
                var expected = context.GetLazyString("igal");

                var jsonSerializer = new JsonSerializer
                {
                    ContractResolver = new DefaultRavenContractResolver(),
                    ConstructorHandling = ConstructorHandling.AllowNonPublicDefaultConstructor,
                };

                jsonSerializer.Converters.Add(new LazyStringValueJsonConverter());
                var command = new Command { LazyString = expected };
                jsonSerializer.Serialize(writer, command);
                writer.FinalizeDocument();

                //Assert
                var reader = writer.CreateReader();
                reader.TryGet(nameof(Command.LazyString), out LazyStringValue actual);
                Assert.Equal(expected, actual);
            }
        }

        [Fact]
        public void JsonConvert_WhenBlittableObjectIsProperty_ShouldResultInCopy()
        {
            using (Server.ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext context))
            using (var writer = new BlittableJsonWriter(context))
            {
                var data = new { Property = "Value" };
                var expected = EntityToBlittable.ConvertCommandToBlittable(data, context);
                var jsonSerializer = new JsonSerializer
                {
                    ContractResolver = new DefaultRavenContractResolver(),
                    ConstructorHandling = ConstructorHandling.AllowNonPublicDefaultConstructor,
                };

                jsonSerializer.Converters.Add(new BlittableJsonConverter());
                var command = new Command { BlittableObject = expected };
                jsonSerializer.Serialize(writer, command);
                writer.FinalizeDocument();

                //Assert
                var reader = writer.CreateReader();
                reader.TryGet(nameof(Command.BlittableObject), out BlittableJsonReaderObject actual);
                Assert.Equal(expected, actual);
            }
        }

        [Fact]
        //Todo To consider if should Support direct serialize of BlittableObject
        public void JsonConvert_WhenBlittableIsTheRoot_ShouldResultInCopy()
        {
            using (Server.ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext context))
            using (var writer = new BlittableJsonWriter(context))
            {
                var data = new DynamicJsonValue
                {
                    ["Property"] = "Value"
                };
                var blittableData = EntityToBlittable.ConvertCommandToBlittable(data, context);

                var jsonSerializer = new JsonSerializer
                {
                    ContractResolver = new DefaultRavenContractResolver(),
                    ConstructorHandling = ConstructorHandling.AllowNonPublicDefaultConstructor,
                };

                jsonSerializer.Converters.Add(new BlittableJsonConverter());
                jsonSerializer.Serialize(writer, blittableData);
                writer.FinalizeDocument();

                //Assert
                var result = writer.CreateReader();
                Assert.True(result.TryGet(nameof(data), out object _));
            }
        }
    }
}

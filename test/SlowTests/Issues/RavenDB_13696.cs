using System;
using System.Collections.Generic;
using FastTests;
using Raven.Client;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations;
using Raven.Server.Documents.Indexes.Static;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_13696 : RavenTestBase
    {
        public RavenDB_13696(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void PatchShouldPreserverOrderOfProperties()
        {
            using (var store = GetDocumentStore())
            {
                var (properties, djv) = GenerateRandomProperties();

                var (nestedProps, nestedDjv) = GenerateRandomProperties();

                djv["Nested"] = nestedDjv;

                using (var commands = store.Commands())
                {
                    var json = commands.Context.ReadObject(djv, "doc");

                    commands.Put("items/1", null, json);
                }

                ValidateOrderOfProperties(store, properties, nestedProps);

                store.Operations.Send(new PatchOperation("items/1", null, new PatchRequest { Script = $"this['{properties[0]}'] = 10" }));

                ValidateOrderOfProperties(store, properties, nestedProps);
            }
        }

        private static (List<string> properties, DynamicJsonValue djv) GenerateRandomProperties()
        {
            var random = new Random();
            var properties = new List<string>();
            for (var i = 0; i < 100; i++)
            {
                var number = random.Next().ToString();
                if (properties.Contains(number))
                    continue;

                properties.Add(number);
            }

            var djv = new DynamicJsonValue();
            foreach (var property in properties)
                djv[property] = null;
            return (properties, djv);
        }

        private static void ValidateOrderOfProperties(DocumentStore store, List<string> properties, List<string> nestedProps)
        {
            using (var commands = store.Commands())
            {
                var json = commands.Get("items/1");

                Assert.Equal(properties.Count + 2, json.BlittableJson.Count);

                ValidateProperties(properties, json.BlittableJson);

                ValidateProperties(nestedProps, (BlittableJsonReaderObject)json.BlittableJson["Nested"]);
            }
        }

        private unsafe static void ValidateProperties(List<string> properties, BlittableJsonReaderObject json)
        {
            var propDetails = new BlittableJsonReaderObject.PropertyDetails();
            var propertiesByInsertionOrder = json.GetPropertiesByInsertionOrder();
            for (var i = 0; i < properties.Count; i++)
            {
                json.GetPropertyByIndex(propertiesByInsertionOrder.Properties[i], ref propDetails);

                if (propDetails.Name == Constants.Documents.Metadata.Key)
                    continue;

                var expected = properties[i];
                Assert.Equal(expected, propDetails.Name);
            }
        }
    }
}

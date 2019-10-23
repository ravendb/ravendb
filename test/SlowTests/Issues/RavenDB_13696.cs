using System;
using System.Collections.Generic;
using FastTests;
using Raven.Client;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations;
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

                using (var commands = store.Commands())
                {
                    var json = commands.Context.ReadObject(djv, "doc");

                    commands.Put("items/1", null, json);
                }

                ValidateOrderOfProperties(store, properties);

                store.Operations.Send(new PatchOperation("items/1", null, new PatchRequest { Script = $"this['{properties[0]}'] = 10" }));

                ValidateOrderOfProperties(store, properties);
            }
        }

        private static void ValidateOrderOfProperties(DocumentStore store, List<string> properties)
        {
            using (var commands = store.Commands())
            {
                var json = commands.Get("items/1");

                Assert.Equal(properties.Count + 1, json.BlittableJson.Count);

                var propDetails = new BlittableJsonReaderObject.PropertyDetails();
                var propertiesByInsertionOrder = json.BlittableJson.GetPropertiesByInsertionOrder();
                for (var i = 0; i < propertiesByInsertionOrder.Properties.Count; i++)
                {
                    var propIndex = propertiesByInsertionOrder.Properties[i];
                    json.BlittableJson.GetPropertyByIndex(propIndex, ref propDetails);

                    if (propDetails.Name == Constants.Documents.Metadata.Key)
                        continue;

                    var expected = properties[i];
                    Assert.Equal(expected, propDetails.Name);
                }
            }
        }
    }
}

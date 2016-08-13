using System;
using System.Collections.Generic;
using Raven.Abstractions;
using Raven.Abstractions.Data;
using Raven.Server.Documents;
using Raven.Server.Documents.Indexes.Static;
using Raven.Abstractions.Extensions;
using Raven.Client.Linq;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Xunit;

namespace FastTests.Server.Documents.Indexing.Static
{
    public class DynamicBlittableJsonTests
    {
        private readonly JsonOperationContext _ctx;
        private readonly List<BlittableJsonReaderObject> _docs = new List<BlittableJsonReaderObject>();

        public DynamicBlittableJsonTests()
        {
            _ctx = JsonOperationContext.ShortTermSingleUse();
        }

        [Fact]
        public void Can_get_simple_values()
        {
            var now = SystemTime.UtcNow;

            using (var lazyStringValue = _ctx.GetLazyString("22.0"))
            {

                var doc = create_doc(new DynamicJsonValue
                {
                    ["Name"] = "Arek",
                    ["Address"] = new DynamicJsonValue
                    {
                        ["City"] = "NYC"
                    },
                    ["NullField"] = null,
                    ["Age"] = new LazyDoubleValue(lazyStringValue),
                    ["LazyName"] = _ctx.GetLazyString("Arek"),
                    ["Friends"] = new DynamicJsonArray
                {
                    new DynamicJsonValue
                    {
                        ["Name"] = "Joe"
                    },
                    new DynamicJsonValue
                    {
                        ["Name"] = "John"
                    }
                },
                    [Constants.Metadata] = new DynamicJsonValue
                    {
                        [Constants.Headers.RavenEntityName] = "Users",
                        [Constants.Headers.RavenLastModified] = now.GetDefaultRavenFormat(true)
                    }
                }, "users/1");

                dynamic user = new DynamicBlittableJson(doc);

                Assert.Equal("Arek", user.Name);
                Assert.Equal("NYC", user.Address.City);
                Assert.Equal("users/1", user.Id);
                Assert.Equal(DynamicNullObject.Null, user.NullField);
                Assert.Equal(22.0, user.Age);
                Assert.Equal("Arek", user.LazyName);
                Assert.Equal(2, user.Friends.Length);
                Assert.Equal("Users", user[Constants.Metadata][Constants.Headers.RavenEntityName]);
                Assert.Equal(now, user[Constants.Metadata].Value<DateTime>(Constants.Headers.RavenLastModified));
            }
        }

        public Document create_doc(DynamicJsonValue document, string id)
        {
            var data = _ctx.ReadObject(document, id);

            _docs.Add(data);

            return new Document
            {
                Data = data,
                Key = _ctx.GetLazyString(id)
            };
        }

        public void Dispose()
        {
            foreach (var docReader in _docs)
            {
                docReader.Dispose();
            }

            _ctx.Dispose();
        }
    }
}
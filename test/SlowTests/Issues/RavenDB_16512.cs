using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using FastTests;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Newtonsoft.Json.Serialization;
using Raven.Client.Json.Serialization.NewtonsoftJson;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_16512 : RavenTestBase
    {
        public RavenDB_16512(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void Can_Use_JsonArrayContract_With_ItemConverter()
        {
            using var store = GetDocumentStore(new Options
            {
                ModifyDocumentStore = s => s.Conventions.Serialization = new NewtonsoftJsonSerializationConventions
                {
                    JsonContractResolver = new StrictTypeContractResolver()
                }
            });
            using (var session = store.OpenSession())
            {
                var example = new ExampleModel
                {
                    Description = "Testing 1,2,3",
                    Id = Guid.NewGuid().ToString(),
                    Name = "Test Example"
                };

                var parent = new ParentModel
                {
                    Example = example,
                    ExampleList = new HashSet<ExampleModelRef> { example }
                };

                var json = JsonConvert.SerializeObject(parent, new JsonSerializerSettings
                {
                    ContractResolver = new StrictTypeContractResolver(),
                    Formatting = Formatting.Indented
                });

                var result = JsonConvert.DeserializeObject<ParentModel>(json);
                Assert.True(result.Example.GetType() == typeof(ExampleModelRef));
                var item = result.ExampleList.First();
                Assert.True(item.GetType() == typeof(ExampleModelRef));

                session.Store(parent);
                session.SaveChanges();
            }

            WaitForIndexing(store);

            using (var session = store.OpenSession())
            {
                var parent = session.Query<ParentModel>().First();
                Assert.True(parent.Example.GetType() == typeof(ExampleModelRef));
                var item = parent.ExampleList.First();
                Assert.Equal(typeof(ExampleModelRef), item.GetType());
            }
        }

        private class ExampleModelRef
        {
            public string Id { get; set; }
            public string Name { get; set; }
        }

        private class ExampleModel : ExampleModelRef
        {
            public string Description { get; set; }
        }

        private class ParentModel
        {
            public string Id { get; init; } = Guid.NewGuid().ToString();
            public ExampleModelRef Example { get; set; }
            public ICollection<ExampleModelRef> ExampleList { get; set; }
        }

        private class StrictTypeContractResolver : DefaultContractResolver
        {
            private readonly FieldInfo _isSealedField = typeof(JsonContract).GetField("IsSealed", BindingFlags.Instance | BindingFlags.NonPublic)!;

            public override JsonContract ResolveContract(Type type)
            {
                var resolveContract = base.ResolveContract(type);
                _isSealedField.SetValue(resolveContract, true);
                return resolveContract;
            }

            protected override JsonArrayContract CreateArrayContract(Type objectType)
            {
                var contract = base.CreateArrayContract(objectType);
                //contract.item = true;

                if (contract.CollectionItemType != null)
                    contract.ItemConverter = new ItemJsonConverter(contract.CollectionItemType);

                return contract;
            }

        }

        private class ItemJsonConverter : JsonConverter
        {
            private readonly PropertyInfo[] _properties;
            private readonly Type _destinationType;
            public ItemJsonConverter(Type destinationType)
            {
                this._destinationType = destinationType;
                _properties = destinationType.GetProperties();
            }

            public override bool CanConvert(Type objectType) => objectType.IsAssignableTo(_destinationType);
            public override object ReadJson(JsonReader reader, Type objectType, object existingValue, JsonSerializer serializer)
            {
                throw new NotSupportedException();
            }

            public override bool CanWrite => true;
            public override bool CanRead => false;

            public override void WriteJson(JsonWriter writer, object value, JsonSerializer serializer)
            {
                //serializer.Serialize(writer, value, destinationType);

                if (value == null)
                    return;

                JToken t = JToken.FromObject(value);

                if (t.Type != JTokenType.Object)
                {
                    t.WriteTo(writer);
                }
                else
                {
                    JObject o = (JObject)t;
                    var propNames = o.Properties().Where(p => _properties.All(pp => pp.Name != p.Name)).Select(p => p.Name).ToArray();
                    foreach (var propName in propNames)
                        o.Property(propName).Remove();

                    serializer.Serialize(writer, o, _destinationType);
                    //o.WriteTo(writer);
                }
            }
        }
    }
}

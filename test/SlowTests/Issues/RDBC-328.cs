using System;
using FastTests;
using Newtonsoft.Json;
using Raven.Client.Json.Serialization.NewtonsoftJson;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RDBC_328 : RavenTestBase
    {
        public RDBC_328(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void CanCustomizeDeserialization()
        {
            using (var documentStore = GetDocumentStore(new Options
            {
                ModifyDocumentStore = x =>
                {
                    x.Conventions.Serialization = new JsonNetSerializationConventions
                    {
                        CustomizeJsonDeserializer = s =>
                        {
                            s.Converters.Add(new NeverNullStringConverter());
                        }
                    };
                }
            }))
            {
                using (var session = documentStore.OpenSession())
                {
                    session.Store(new Entity { Id = "isnotnull", Property = new NeverNullString { Value = "hello" } });
                    session.Store(new Entity { Id = "isnull", Property = new NeverNullString() });
                    session.SaveChanges();
                }

                using (var session = documentStore.OpenSession())
                {
                    var isnotnull = session.Load<Entity>("isnotnull");
                    var isnull = session.Load<Entity>("isnull");
                    WaitForUserToContinueTheTest(documentStore);
                    Assert.NotNull(isnotnull.Property);
                    Assert.NotNull(isnull.Property);
                }
            }
        }

        [Fact]
        public void CanCustomizeSeserializationWithoutAffectingDeserialization()
        {
            using (var documentStore = base.GetDocumentStore(new Options
            {
                ModifyDocumentStore = x =>
                {
                    NeverNullStringConverter converter = new NeverNullStringConverter();

                    x.Conventions.Serialization = new JsonNetSerializationConventions
                    {
                        CustomizeJsonSerializer = s =>
                        {
                            s.Converters.Add(converter);
                        },
                        CustomizeJsonDeserializer = s =>
                        {
                            s.Converters.Remove(converter);
                        }
                    };
                }
            }))
            {
                using (var session = documentStore.OpenSession())
                {
                    session.Store(new Entity { Id = "isnotnull", Property = new NeverNullString { Value = "hello" } });
                    session.Store(new Entity { Id = "isnull", Property = new NeverNullString() });
                    session.SaveChanges();
                }

                using (var session = documentStore.OpenSession())
                {
                    Assert.Equal(typeof(JsonSerializationException),
                    Assert.Throws<InvalidOperationException>(() => session.Load<Entity>("isnotnull")).InnerException.GetType());
                }
            }
        }

        public class Entity
        {
            public string Id { get; set; }
            public NeverNullString Property { get; set; }
        }
    }

    public class NeverNullString
    {
        private string _value;
        public string Value { get => _value ?? "<null>"; set => _value = value; }
    }

    public class NeverNullStringConverter : JsonConverter
    {
        public override bool CanConvert(Type objectType)
        {
            return objectType == typeof(NeverNullString);
        }

        public override object ReadJson(JsonReader reader, Type objectType, object existingValue, JsonSerializer serializer)
        {
            return new NeverNullString { Value = reader.Value?.ToString() };
        }

        public override void WriteJson(JsonWriter writer, object value, JsonSerializer serializer)
        {
            var obj = (NeverNullString)value;
            var str = obj.Value == "<null>" ? null : obj.Value;
            serializer.Serialize(writer, str);
        }
    }
}
